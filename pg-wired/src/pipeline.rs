use std::collections::HashMap;

use bytes::BytesMut;

use crate::connection::WireConn;
use crate::error::PgWireError;
use crate::protocol::frontend;
use crate::protocol::types::{FormatCode, FrontendMsg};

/// High-level pipelined PostgreSQL client.
/// Coalesces Parse+Bind+Execute+Sync into a single TCP write.
/// Caches prepared statements to skip Parse on subsequent calls.
pub struct PgPipeline {
    conn: WireConn,
    stmt_cache: HashMap<String, String>, // sql → statement name
    stmt_counter: u64,
    max_cache_size: usize,
    send_buf: BytesMut,
}

impl PgPipeline {
    pub fn new(conn: WireConn) -> Self {
        Self {
            conn,
            stmt_cache: HashMap::new(),
            stmt_counter: 0,
            max_cache_size: 256,
            send_buf: BytesMut::with_capacity(4096),
        }
    }

    /// Execute a parameterized query, returning rows as Vec<Vec<Option<Vec<u8>>>>.
    /// Uses binary format for parameters and results.
    /// On cache miss: Parse+Bind+Execute+Sync in ONE write.
    /// On cache hit: Bind+Execute+Sync in ONE write.
    pub async fn query(
        &mut self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let (stmt_name, needs_parse) = self.lookup_or_alloc(sql);
        let stmt_name_bytes = stmt_name.as_bytes().to_vec();

        // Build all messages into one buffer.
        self.send_buf.clear();

        // Send params as Text format — our values are text strings that PostgreSQL
        // casts via ($1::text)::target_type in the SQL.
        let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
        let result_fmts = [FormatCode::Text]; // Text for JSON result

        let mut msgs: Vec<FrontendMsg<'_>> = Vec::with_capacity(4);

        if needs_parse {
            msgs.push(FrontendMsg::Parse {
                name: &stmt_name_bytes,
                sql: sql.as_bytes(),
                param_oids,
            });
        }

        msgs.push(FrontendMsg::Bind {
            portal: b"",
            statement: &stmt_name_bytes,
            param_formats: &text_fmts[..params.len()],
            params,
            result_formats: &result_fmts,
        });

        msgs.push(FrontendMsg::Execute {
            portal: b"",
            max_rows: 0, // unlimited
        });

        msgs.push(FrontendMsg::Sync);

        // Encode all messages into ONE buffer.
        frontend::encode_messages(&msgs, &mut self.send_buf);

        // ONE write() syscall.
        self.conn.send_raw(&self.send_buf).await?;

        // Collect all rows until ReadyForQuery.
        let (rows, _tag) = self.conn.collect_rows().await?;
        Ok(rows)
    }

    /// Execute a simple query (no parameters, text protocol).
    /// Used for SET LOCAL ROLE, set_config, BEGIN, COMMIT etc.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), PgWireError> {
        self.send_buf.clear();
        frontend::encode_message(
            &FrontendMsg::Query(sql.as_bytes()),
            &mut self.send_buf,
        );
        self.conn.send_raw(&self.send_buf).await?;

        // Drain until ReadyForQuery.
        self.conn.drain_until_ready().await?;
        Ok(())
    }

    /// Execute a simple query and return rows (text format).
    /// Used for introspection queries (e.g., migration status).
    pub async fn simple_query_rows(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<Vec<Option<Vec<u8>>>>, String), PgWireError> {
        self.send_buf.clear();
        frontend::encode_message(
            &FrontendMsg::Query(sql.as_bytes()),
            &mut self.send_buf,
        );
        self.conn.send_raw(&self.send_buf).await?;
        self.conn.collect_rows().await
    }

    /// Execute a pipelined transaction: setup (simple) + query (parameterized) in TWO messages
    /// but coalesced into ONE TCP write.
    ///
    /// This is the key optimization: BEGIN + SET LOCAL ROLE + set_config + parameterized query
    /// all go in one write() syscall, with the data query using the safe binary protocol.
    pub async fn pipeline_with_setup(
        &mut self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let (stmt_name, needs_parse) = self.lookup_or_alloc(query_sql);
        let stmt_name_bytes = stmt_name.as_bytes().to_vec();

        self.send_buf.clear();

        // 1. Simple query for setup (BEGIN + SET ROLE + set_config).
        frontend::encode_message(
            &FrontendMsg::Query(setup_sql.as_bytes()),
            &mut self.send_buf,
        );

        // 2. Extended query for data (Parse? + Bind + Execute + Sync).
        // Send params as Text format — our values are text strings that PostgreSQL
        // casts via ($1::text)::target_type in the SQL.
        let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
        let result_fmts = [FormatCode::Text];

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name_bytes,
                    sql: query_sql.as_bytes(),
                    param_oids,
                },
                &mut self.send_buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name_bytes,
                param_formats: &text_fmts[..params.len()],
                params,
                result_formats: &result_fmts,
            },
            &mut self.send_buf,
        );

        frontend::encode_message(
            &FrontendMsg::Execute {
                portal: b"",
                max_rows: 0,
            },
            &mut self.send_buf,
        );

        frontend::encode_message(&FrontendMsg::Sync, &mut self.send_buf);

        // ONE write() syscall for everything.
        self.conn.send_raw(&self.send_buf).await?;

        // Read responses: first ReadyForQuery from the simple query setup,
        // then DataRows + ReadyForQuery from the extended query.
        self.conn.drain_until_ready().await?; // Setup response
        let (rows, _tag) = self.conn.collect_rows().await?; // Data response

        Ok(rows)
    }

    /// Execute a pipelined transaction with COMMIT at the end.
    /// setup_sql should contain "BEGIN; SET LOCAL ROLE ...; SELECT set_config(...)"
    /// The commit is sent as a separate simple query, coalesced in the same write.
    pub async fn pipeline_transaction(
        &mut self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let (stmt_name, needs_parse) = self.lookup_or_alloc(query_sql);
        let stmt_name_bytes = stmt_name.as_bytes().to_vec();

        self.send_buf.clear();

        // 1. Simple query: BEGIN + SET ROLE + set_config
        frontend::encode_message(
            &FrontendMsg::Query(setup_sql.as_bytes()),
            &mut self.send_buf,
        );

        // 2. Extended query: Bind + Execute + Sync (parameterized, binary safe)
        // Send params as Text format — our values are text strings that PostgreSQL
        // casts via ($1::text)::target_type in the SQL.
        let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
        let result_fmts = [FormatCode::Text];

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name_bytes,
                    sql: query_sql.as_bytes(),
                    param_oids,
                },
                &mut self.send_buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name_bytes,
                param_formats: &text_fmts[..params.len()],
                params,
                result_formats: &result_fmts,
            },
            &mut self.send_buf,
        );

        frontend::encode_message(
            &FrontendMsg::Execute { portal: b"", max_rows: 0 },
            &mut self.send_buf,
        );

        frontend::encode_message(&FrontendMsg::Sync, &mut self.send_buf);

        // 3. Simple query: COMMIT
        frontend::encode_message(
            &FrontendMsg::Query(b"COMMIT"),
            &mut self.send_buf,
        );

        // ONE write() syscall for the entire transaction.
        self.conn.send_raw(&self.send_buf).await?;

        // Read responses in order:
        // 1. ReadyForQuery from setup
        // 2. DataRows + ReadyForQuery from data query
        // 3. ReadyForQuery from COMMIT
        self.conn.drain_until_ready().await?; // Setup
        let (rows, _tag) = self.conn.collect_rows().await?; // Data
        self.conn.drain_until_ready().await?; // COMMIT

        Ok(rows)
    }

    /// Look up or allocate a statement name.
    fn lookup_or_alloc(&mut self, sql: &str) -> (String, bool) {
        if let Some(name) = self.stmt_cache.get(sql) {
            return (name.clone(), false);
        }

        // Evict if cache is full.
        if self.stmt_cache.len() >= self.max_cache_size {
            // Simple eviction: clear all (LRU would be better).
            self.stmt_cache.clear();
        }

        let name = format!("s{}", self.stmt_counter);
        self.stmt_counter += 1;
        self.stmt_cache.insert(sql.to_string(), name.clone());
        (name, true)
    }

    /// Clear the statement cache (e.g., after DISCARD ALL).
    pub fn clear_cache(&mut self) {
        self.stmt_cache.clear();
    }

    /// Get a mutable reference to the underlying connection.
    pub fn conn(&mut self) -> &mut WireConn {
        &mut self.conn
    }

    /// Get a reference to the underlying connection.
    pub fn conn_ref(&self) -> &WireConn {
        &self.conn
    }
}

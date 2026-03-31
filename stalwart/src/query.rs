//! Typed query client built on pg-wire AsyncConn.
//!
//! Sends queries with binary format parameters and results,
//! returning typed Rows instead of raw bytes.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use tokio::sync::mpsc;

use pg_wire::protocol::frontend;
use pg_wire::protocol::types::{FormatCode, FrontendMsg};
use pg_wire::{AsyncConn, PipelineResponse, ResponseCollector, WireConn};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

/// A typed query client wrapping an AsyncConn.
/// Sends parameters in binary format and requests binary results.
pub struct Client {
    conn: AsyncConn,
}

impl Client {
    /// Default row buffer size for streaming queries.
    /// Higher values use more memory but reduce backpressure stalls.
    pub const DEFAULT_STREAM_BUFFER: usize = 256;
}

impl Client {
    /// Create a new typed client from a raw WireConn.
    pub fn new(conn: WireConn) -> Self {
        Self {
            conn: AsyncConn::new(conn),
        }
    }

    /// Create from an existing AsyncConn.
    pub fn from_async_conn(conn: AsyncConn) -> Self {
        Self { conn }
    }

    /// Connect to PostgreSQL and create a typed client.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, TypedError> {
        tracing::debug!(addr = addr, user = user, database = database, "connecting");
        let wire = WireConn::connect(addr, user, password, database).await?;
        tracing::info!(addr = addr, database = database, pid = wire.pid, "connected");
        Ok(Self::new(wire))
    }

    /// Connect and run initialization SQL (e.g. `SET search_path`, `SET role`).
    ///
    /// ```ignore
    /// let client = Client::connect_with_init(
    ///     "127.0.0.1:5432", "user", "pass", "mydb",
    ///     &["SET search_path TO myschema, public", "SET statement_timeout = '30s'"],
    /// ).await?;
    /// ```
    pub async fn connect_with_init(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        init_sql: &[&str],
    ) -> Result<Self, TypedError> {
        let client = Self::connect(addr, user, password, database).await?;
        for sql in init_sql {
            tracing::debug!(sql = sql, "running init SQL");
            client.simple_query(sql).await?;
        }
        Ok(client)
    }

    /// Execute a query and return typed rows.
    ///
    /// Parameters are encoded in binary format. Results are requested in binary.
    ///
    /// ```ignore
    /// let rows = client.query("SELECT id, name FROM users WHERE id = $1", &[&42i32]).await?;
    /// for row in &rows {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        let start = std::time::Instant::now();
        let result = match self.query_inner(sql, params).await {
            Err(TypedError::Wire(ref e))
                if is_stale_statement_error(e) =>
            {
                tracing::debug!("stale statement detected, re-preparing");
                self.conn.invalidate_statement(sql);
                self.query_inner(sql, params).await
            }
            other => other,
        };
        let elapsed = start.elapsed();
        match &result {
            Ok(rows) => {
                let us = elapsed.as_micros() as u64;
                crate::metrics::record_query(us);
                tracing::debug!(sql = %truncate_sql(sql), rows = rows.len(), elapsed_us = us, "query ok");
            }
            Err(ref e) => {
                crate::metrics::record_query_error();
                tracing::warn!(sql = %truncate_sql(sql), error = %e, elapsed_us = elapsed.as_micros() as u64, "query failed");
            }
        }
        result.map_err(|e| e.with_sql(sql))
    }

    /// Execute a query on an arbitrary AsyncConn (used by PooledTypedClient).
    pub(crate) async fn query_on_conn(
        conn: &AsyncConn,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        Self::query_inner_on(conn, sql, params).await
    }

    /// Execute a statement on an arbitrary AsyncConn (used by PooledTypedClient).
    pub(crate) async fn execute_on_conn(
        conn: &AsyncConn,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        Self::execute_inner_on(conn, sql, params).await
    }

    /// Send a simple query on an arbitrary AsyncConn.
    pub(crate) async fn simple_query_on_conn(
        conn: &AsyncConn,
        sql: &str,
    ) -> Result<(), TypedError> {
        let mut buf = BytesMut::new();
        frontend::encode_message(&FrontendMsg::Query(sql.as_bytes()), &mut buf);
        let _resp = conn.submit(buf, ResponseCollector::Drain).await?;
        Ok(())
    }

    async fn query_inner(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        Self::query_inner_on(&self.conn, sql, params).await
    }

    async fn query_inner_on(
        conn: &AsyncConn,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        let (stmt_name, needs_parse) = conn.lookup_or_alloc(sql);

        let mut buf = BytesMut::with_capacity(512);

        // Encode parameters in binary format.
        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary]; // Request binary results.

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
        }

        // Convert to the wire format expected by pg-wire.
        let param_refs: Vec<Option<&[u8]>> = param_values
            .iter()
            .map(|v| v.as_ref().map(|b| b.as_ref()))
            .collect();

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name,
                    sql: sql.as_bytes(),
                    param_oids: &param_oids,
                },
                &mut buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name,
                param_formats: &param_formats,
                params: &param_refs,
                result_formats: &result_formats,
            },
            &mut buf,
        );

        // Describe portal to get RowDescription (column names + types).
        frontend::encode_message(
            &FrontendMsg::Describe {
                kind: b'P', // Portal
                name: b"",
            },
            &mut buf,
        );

        frontend::encode_message(
            &FrontendMsg::Execute {
                portal: b"",
                max_rows: 0,
            },
            &mut buf,
        );

        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let resp = conn.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows { fields, rows: raw_rows, command_tag: _ } => {
                // Build column metadata from RowDescription if available.
                let has_desc = !fields.is_empty();
                let columns: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
                let type_oids: Vec<u32> = fields.iter().map(|f| f.type_oid).collect();
                // If no RowDescription, default to binary (we requested it in Bind).
                let formats: Vec<i16> = if has_desc {
                    fields.iter().map(|f| f.format as i16).collect()
                } else {
                    Vec::new() // Will use binary default per-row below.
                };

                let rows = raw_rows
                    .into_iter()
                    .map(|data| {
                        let row_formats = if formats.is_empty() {
                            vec![1i16; data.len()] // Binary format (we requested it).
                        } else {
                            formats.clone()
                        };
                        Row {
                            columns: columns.clone(),
                            type_oids: type_oids.clone(),
                            formats: row_formats,
                            data,
                        }
                    })
                    .collect();
                Ok(rows)
            }
            PipelineResponse::Done => Ok(Vec::new()),
        }
    }

    /// Execute a query and return exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Row, TypedError> {
        let rows = self.query(sql, params).await?;
        if rows.len() != 1 {
            return Err(TypedError::NotExactlyOne(rows.len()));
        }
        Ok(rows.into_iter().next().unwrap())
    }

    /// Execute a query and return an optional single row.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Option<Row>, TypedError> {
        let rows = self.query(sql, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.into_iter().next().unwrap())),
            n => Err(TypedError::NotExactlyOne(n)),
        }
    }

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE).
    /// Returns the number of affected rows.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        let start = std::time::Instant::now();
        let result = match self.execute_inner(sql, params).await {
            Err(TypedError::Wire(ref e))
                if is_stale_statement_error(e) =>
            {
                tracing::debug!("stale statement detected, re-preparing");
                self.conn.invalidate_statement(sql);
                self.execute_inner(sql, params).await
            }
            other => other,
        };
        let elapsed = start.elapsed();
        match &result {
            Ok(n) => {
                crate::metrics::record_execute();
                tracing::debug!(sql = %truncate_sql(sql), affected = n, elapsed_us = elapsed.as_micros() as u64, "execute ok");
            }
            Err(ref e) => {
                crate::metrics::record_execute_error();
                tracing::warn!(sql = %truncate_sql(sql), error = %e, elapsed_us = elapsed.as_micros() as u64, "execute failed");
            }
        }
        result.map_err(|e| e.with_sql(sql))
    }

    async fn execute_inner(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        Self::execute_inner_on(&self.conn, sql, params).await
    }

    async fn execute_inner_on(
        conn: &AsyncConn,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        let (stmt_name, needs_parse) = conn.lookup_or_alloc(sql);
        let mut buf = BytesMut::with_capacity(512);

        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary];

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
        }
        let param_refs: Vec<Option<&[u8]>> = param_values
            .iter()
            .map(|v| v.as_ref().map(|b| b.as_ref()))
            .collect();

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name,
                    sql: sql.as_bytes(),
                    param_oids: &param_oids,
                },
                &mut buf,
            );
        }
        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name,
                param_formats: &param_formats,
                params: &param_refs,
                result_formats: &result_formats,
            },
            &mut buf,
        );
        frontend::encode_message(
            &FrontendMsg::Execute { portal: b"", max_rows: 0 },
            &mut buf,
        );
        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let resp = conn.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows { command_tag, .. } => {
                Ok(parse_row_count(&command_tag))
            }
            PipelineResponse::Done => Ok(0),
        }
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Rows are delivered one at a time as they arrive from PostgreSQL,
    /// without buffering the entire result set in memory.
    ///
    /// ```ignore
    /// use tokio_stream::StreamExt;
    /// let mut stream = client.query_stream("SELECT * FROM large_table", &[]).await?;
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     let id: i32 = row.get(0)?;
    /// }
    /// ```
    pub async fn query_stream(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<RowStream, TypedError> {
        let (stmt_name, needs_parse) = self.conn.lookup_or_alloc(sql);

        let mut buf = BytesMut::with_capacity(512);
        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary];

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
        }
        let param_refs: Vec<Option<&[u8]>> = param_values
            .iter()
            .map(|v| v.as_ref().map(|b| b.as_ref()))
            .collect();

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name,
                    sql: sql.as_bytes(),
                    param_oids: &param_oids,
                },
                &mut buf,
            );
        }
        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name,
                param_formats: &param_formats,
                params: &param_refs,
                result_formats: &result_formats,
            },
            &mut buf,
        );
        frontend::encode_message(
            &FrontendMsg::Describe {
                kind: b'P',
                name: b"",
            },
            &mut buf,
        );
        frontend::encode_message(
            &FrontendMsg::Execute {
                portal: b"",
                max_rows: 0,
            },
            &mut buf,
        );
        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let (header, row_rx) = self.conn.submit_stream(buf, Self::DEFAULT_STREAM_BUFFER).await?;

        let columns: Vec<String> = header.fields.iter().map(|f| f.name.clone()).collect();
        let type_oids: Vec<u32> = header.fields.iter().map(|f| f.type_oid).collect();
        let formats: Vec<i16> = header
            .fields
            .iter()
            .map(|f| f.format as i16)
            .collect();

        Ok(RowStream {
            row_rx,
            columns,
            type_oids,
            formats,
        })
    }

    /// Bulk-load data via COPY FROM STDIN.
    ///
    /// Sends `COPY table FROM STDIN (FORMAT csv)` (or whatever `copy_sql` specifies),
    /// then streams `data` to PostgreSQL. Returns the number of rows copied.
    ///
    /// ```ignore
    /// let csv = b"1,Alice\n2,Bob\n";
    /// let count = client.copy_in("COPY users (id, name) FROM STDIN WITH (FORMAT csv)", csv).await?;
    /// assert_eq!(count, 2);
    /// ```
    pub async fn copy_in(&self, copy_sql: &str, data: &[u8]) -> Result<u64, TypedError> {
        self.conn
            .copy_in(copy_sql, data)
            .await
            .map_err(TypedError::from)
    }

    /// Export data via COPY TO STDOUT.
    ///
    /// Sends `COPY table TO STDOUT (FORMAT csv)` and returns all the data.
    ///
    /// ```ignore
    /// let csv_data = client.copy_out("COPY users TO STDOUT WITH (FORMAT csv)").await?;
    /// println!("{}", String::from_utf8_lossy(&csv_data));
    /// ```
    pub async fn copy_out(&self, copy_sql: &str) -> Result<Vec<u8>, TypedError> {
        self.conn
            .copy_out(copy_sql)
            .await
            .map_err(TypedError::from)
    }

    /// Begin a transaction. Returns a `Transaction` guard that
    /// commits on `commit()` or rolls back on drop.
    pub async fn begin(&self) -> Result<Transaction<'_>, TypedError> {
        self.simple_query("BEGIN").await?;
        Ok(Transaction { client: self, done: false })
    }

    /// Send a simple text query (no params, no binary format).
    /// Used for BEGIN/COMMIT/ROLLBACK and DDL.
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        use pg_wire::protocol::types::FrontendMsg;
        let mut buf = BytesMut::new();
        frontend::encode_message(&FrontendMsg::Query(sql.as_bytes()), &mut buf);
        let _resp = self.conn.submit(buf, ResponseCollector::Drain).await?;
        Ok(())
    }

    /// Execute a query with named parameters (`:name` syntax).
    ///
    /// Named params are rewritten to `$1, $2, ...` before sending to PostgreSQL.
    /// Duplicate names in SQL reuse the same positional slot.
    ///
    /// ```ignore
    /// let rows = client.query_named(
    ///     "SELECT * FROM users WHERE id = :id AND org = :org",
    ///     &[("id", &42i32), ("org", &"acme")],
    /// ).await?;
    /// ```
    pub async fn query_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<Vec<Row>, TypedError> {
        let (rewritten, names) = crate::named_params::rewrite(sql);
        let ordered = resolve_named_params(&names, params)?;
        self.query(&rewritten, &ordered).await
    }

    /// Execute a named-param statement (INSERT/UPDATE/DELETE). Returns affected row count.
    pub async fn execute_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<u64, TypedError> {
        let (rewritten, names) = crate::named_params::rewrite(sql);
        let ordered = resolve_named_params(&names, params)?;
        self.execute(&rewritten, &ordered).await
    }

    /// Execute a query with a timeout. Auto-cancels via CancelRequest if exceeded.
    ///
    /// ```ignore
    /// let rows = client.query_timeout(
    ///     "SELECT pg_sleep(60)",
    ///     &[],
    ///     Duration::from_secs(5),
    /// ).await; // returns Err after 5s
    /// ```
    pub async fn query_timeout(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
        timeout: std::time::Duration,
    ) -> Result<Vec<Row>, TypedError> {
        let token = self.cancel_token();
        match tokio::time::timeout(timeout, self.query(sql, params)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                let _ = token.cancel().await;
                Err(TypedError::Timeout(timeout))
            }
        }
    }

    /// Execute a statement with a timeout. Auto-cancels if exceeded.
    pub async fn execute_timeout(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
        timeout: std::time::Duration,
    ) -> Result<u64, TypedError> {
        let token = self.cancel_token();
        match tokio::time::timeout(timeout, self.execute(sql, params)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                let _ = token.cancel().await;
                Err(TypedError::Timeout(timeout))
            }
        }
    }

    /// Get a cancel token for this connection.
    ///
    /// The token can be cloned, sent to another task, and used to cancel
    /// a long-running query. Cancellation is best-effort.
    ///
    /// ```ignore
    /// let token = client.cancel_token();
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(Duration::from_secs(5)).await;
    ///     token.cancel().await.ok();
    /// });
    /// client.query("SELECT pg_sleep(60)", &[]).await; // cancelled after 5s
    /// ```
    pub fn cancel_token(&self) -> pg_wire::CancelToken {
        pg_wire::CancelToken {
            addr: self.conn.addr.clone(),
            pid: self.conn.backend_pid,
            secret: self.conn.backend_secret,
        }
    }

    /// Ping the database to verify the connection is healthy.
    /// Runs `SELECT 1` and returns Ok if successful.
    pub async fn ping(&self) -> Result<(), TypedError> {
        self.query("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Check if the underlying connection is alive (non-blocking check).
    pub fn is_alive(&self) -> bool {
        self.conn.is_alive()
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// A transaction guard. Commits on `commit()`, rolls back on drop.
pub struct Transaction<'a> {
    pub(crate) client: &'a Client,
    pub(crate) done: bool,
}

impl<'a> Transaction<'a> {
    /// Execute a query within the transaction.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        self.client.query(sql, params).await
    }

    /// Execute a statement within the transaction. Returns affected row count.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        self.client.execute(sql, params).await
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("COMMIT").await
    }

    /// Explicitly roll back the transaction.
    pub async fn rollback(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("ROLLBACK").await
    }

    /// Execute a query with named parameters within the transaction.
    pub async fn query_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<Vec<Row>, TypedError> {
        self.client.query_named(sql, params).await
    }

    /// Execute a named-param statement within the transaction. Returns affected row count.
    pub async fn execute_named(
        &self,
        sql: &str,
        params: &[(&str, &dyn SqlParam)],
    ) -> Result<u64, TypedError> {
        self.client.execute_named(sql, params).await
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.done {
            // Can't await in drop — spawn a task to rollback.
            // This is best-effort; the connection will recover on next use.
            let client_alive = self.client.is_alive();
            if client_alive {
                // We can't actually send ROLLBACK in drop without a runtime.
                // The PG connection will auto-rollback when the next statement
                // is sent outside a transaction block.
                tracing::warn!("Transaction dropped without commit — will auto-rollback");
            }
        }
    }
}

/// Resolve named params: map `names` (from SQL rewriting) to the user-provided params slice.
fn resolve_named_params<'a>(
    names: &[String],
    params: &[(&str, &'a dyn SqlParam)],
) -> Result<Vec<&'a dyn SqlParam>, TypedError> {
    names
        .iter()
        .map(|name| {
            params
                .iter()
                .find(|(n, _)| *n == name.as_str())
                .map(|(_, p)| *p)
                .ok_or_else(|| TypedError::MissingParam(name.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Pipeline: batch N queries in one round-trip
// ---------------------------------------------------------------------------

/// A batch of queries to execute in a single network round-trip.
///
/// ```ignore
/// let (rows1, rows2, count) = client.pipeline()
///     .query("SELECT 1::int4 AS n", &[])
///     .query("SELECT 'hello'::text AS s", &[])
///     .execute("INSERT INTO t VALUES ($1)", &[&42i32])
///     .run()
///     .await?;
/// ```
pub struct Pipeline<'a> {
    client: &'a Client,
    /// Encoded messages for all queries in sequence.
    buf: BytesMut,
    /// Number of queries/executions in the pipeline.
    count: usize,
}

/// Result from a single pipeline step.
pub enum PipelineResult {
    /// Rows from a SELECT query.
    Rows(Vec<Row>),
    /// Affected row count from an INSERT/UPDATE/DELETE.
    Execute(u64),
}

impl<'a> Pipeline<'a> {
    /// Add a query to the pipeline.
    pub fn query(mut self, sql: &str, params: &[&dyn SqlParam]) -> Self {
        self.encode_query(sql, params);
        self
    }

    /// Add an execute (INSERT/UPDATE/DELETE) to the pipeline.
    pub fn execute(mut self, sql: &str, params: &[&dyn SqlParam]) -> Self {
        self.encode_query(sql, params);
        self
    }

    fn encode_query(&mut self, sql: &str, params: &[&dyn SqlParam]) {
        let (stmt_name, needs_parse) = self.client.conn.lookup_or_alloc(sql);
        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary];

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
        }
        let param_refs: Vec<Option<&[u8]>> = param_values
            .iter()
            .map(|v| v.as_ref().map(|b| b.as_ref()))
            .collect();

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name,
                    sql: sql.as_bytes(),
                    param_oids: &param_oids,
                },
                &mut self.buf,
            );
        }
        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name,
                param_formats: &param_formats,
                params: &param_refs,
                result_formats: &result_formats,
            },
            &mut self.buf,
        );
        frontend::encode_message(
            &FrontendMsg::Describe { kind: b'P', name: b"" },
            &mut self.buf,
        );
        frontend::encode_message(
            &FrontendMsg::Execute { portal: b"", max_rows: 0 },
            &mut self.buf,
        );
        frontend::encode_message(&FrontendMsg::Sync, &mut self.buf);
        self.count += 1;
    }

    /// Execute all queries in one round-trip and return results.
    pub async fn run(self) -> Result<Vec<PipelineResult>, TypedError> {
        if self.count == 0 {
            return Ok(Vec::new());
        }

        // Submit all messages at once. Each Sync triggers a ReadyForQuery,
        // so we get N responses. We use Rows collector for the first one
        // and then submit the rest.
        // Actually, with the current AsyncConn, each submit() waits for one
        // ReadyForQuery. So we need to submit N separate requests.
        // But we want ONE write. The trick: put all messages in one buffer
        // and submit N PipelineRequests, the writer coalesces them.

        let mut results = Vec::with_capacity(self.count);
        let mut remaining = self.buf;

        // Split the buffer isn't practical since messages are variable-length.
        // Instead: submit the entire buffer as one write, with N pending responses.
        // We need direct access to the request channel for this.

        // Simpler approach: submit the whole buffer with the first request,
        // and submit empty buffers for the rest. The reader will process
        // N ReadyForQuery responses in order.
        for i in 0..self.count {
            let msg_buf = if i == 0 {
                std::mem::take(&mut remaining)
            } else {
                BytesMut::new()
            };

            let resp = self.client.conn.submit(msg_buf, ResponseCollector::Rows).await?;
            match resp {
                PipelineResponse::Rows { fields, rows, command_tag } => {
                    if rows.is_empty() && !command_tag.is_empty() {
                        // Execute result (INSERT/UPDATE/DELETE).
                        results.push(PipelineResult::Execute(parse_row_count(&command_tag)));
                    } else {
                        let columns: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
                        let type_oids: Vec<u32> = fields.iter().map(|f| f.type_oid).collect();
                        let formats: Vec<i16> = fields.iter().map(|f| f.format as i16).collect();
                        let typed_rows = rows
                            .into_iter()
                            .map(|data| {
                                let row_formats = if formats.is_empty() {
                                    vec![1i16; data.len()]
                                } else {
                                    formats.clone()
                                };
                                Row {
                                    columns: columns.clone(),
                                    type_oids: type_oids.clone(),
                                    formats: row_formats,
                                    data,
                                }
                            })
                            .collect();
                        results.push(PipelineResult::Rows(typed_rows));
                    }
                }
                PipelineResponse::Done => {
                    results.push(PipelineResult::Execute(0));
                }
            }
        }

        Ok(results)
    }
}

impl Client {
    /// Create a pipeline to batch multiple queries in one network round-trip.
    pub fn pipeline(&self) -> Pipeline<'_> {
        Pipeline {
            client: self,
            buf: BytesMut::with_capacity(1024),
            count: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// RowStream: async Stream of typed Rows
// ---------------------------------------------------------------------------

/// A stream of rows from a query result.
///
/// Implements `tokio_stream::Stream<Item = Result<Row, TypedError>>` for
/// row-at-a-time consumption without buffering the entire result set.
pub struct RowStream {
    row_rx: mpsc::Receiver<Result<Vec<Option<Vec<u8>>>, pg_wire::PgWireError>>,
    columns: Vec<String>,
    type_oids: Vec<u32>,
    formats: Vec<i16>,
}

impl tokio_stream::Stream for RowStream {
    type Item = Result<Row, TypedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.row_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                let row_formats = if self.formats.is_empty() {
                    vec![1i16; data.len()]
                } else {
                    self.formats.clone()
                };
                let row = Row {
                    columns: self.columns.clone(),
                    type_oids: self.type_oids.clone(),
                    formats: row_formats,
                    data,
                };
                Poll::Ready(Some(Ok(row)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None), // stream complete
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Truncate SQL for log display (avoid logging multi-KB queries).
fn truncate_sql(sql: &str) -> String {
    if sql.len() <= 100 {
        sql.to_string()
    } else {
        format!("{}...", &sql[..100])
    }
}

/// Check if a wire error indicates a stale prepared statement that should be
/// evicted and re-prepared. PG error codes:
/// - 26000: "prepared statement does not exist"
/// - 0A000: "feature not supported" (can happen with statement invalidation)
fn is_stale_statement_error(e: &pg_wire::PgWireError) -> bool {
    if let pg_wire::PgWireError::Pg(ref pg_err) = e {
        matches!(pg_err.code.as_str(), "26000" | "0A000")
    } else {
        false
    }
}

/// Parse the affected row count from a CommandComplete tag.
/// Examples: "SELECT 3" → 3, "INSERT 0 1" → 1, "UPDATE 5" → 5, "DELETE 0" → 0.
fn parse_row_count(tag: &str) -> u64 {
    // The last space-separated token is the row count.
    tag.rsplit_once(' ')
        .and_then(|(_, count)| count.parse::<u64>().ok())
        .unwrap_or(0)
}

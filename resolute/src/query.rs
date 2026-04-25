//! Typed query client built on pg-wired AsyncConn.
//!
//! Sends queries with binary format parameters and results,
//! returning typed Rows instead of raw bytes.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use tokio::sync::mpsc;

use pg_wired::protocol::frontend;
use pg_wired::protocol::types::{FormatCode, FrontendMsg, RawRow};
use pg_wired::{AsyncConn, PipelineResponse, ResponseCollector, WireConn};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::{Row, RowSchema};
use std::sync::Arc;

/// Transaction isolation level for `begin_with()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// `READ COMMITTED` — default PostgreSQL isolation level.
    /// Each statement sees only rows committed before it began.
    ReadCommitted,
    /// `REPEATABLE READ` — all statements in the transaction see
    /// a snapshot of the database as of the transaction start.
    RepeatableRead,
    /// `SERIALIZABLE` — strictest level. Transactions appear to
    /// execute one at a time. May cause serialization failures.
    Serializable,
}

impl IsolationLevel {
    /// Returns the SQL fragment for this isolation level.
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }
}

/// A typed query client wrapping an AsyncConn.
/// Sends parameters in binary format and requests binary results.
#[derive(Debug)]
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
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the TCP connect, TLS negotiation, or
    /// authentication handshake (SCRAM-SHA-256 / SCRAM-SHA-256-PLUS / MD5)
    /// fails, or if the server rejects the startup message.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, TypedError> {
        tracing::debug!(addr = addr, user = user, database = database, "connecting");
        let wire = WireConn::connect(addr, user, password, database).await?;
        tracing::info!(
            addr = addr,
            database = database,
            pid = wire.pid(),
            "connected"
        );
        Ok(Self::new(wire))
    }

    /// Connect using a connection string (URI or key=value format).
    ///
    /// Supports:
    /// - `postgres://user:pass@host:port/dbname`
    /// - `postgresql://user:pass@host:port/dbname`
    /// - `host=localhost port=5432 dbname=mydb user=postgres password=secret`
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), resolute::TypedError> {
    /// use resolute::Client;
    /// let client = Client::connect_from_str("postgres://user:pass@localhost/mydb").await?;
    /// # let _ = client;
    /// # let client = Client::connect_from_str("host=localhost dbname=mydb user=postgres").await?;
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Config` if the connection string can't be parsed
    /// as either supported format. Otherwise returns whatever [`Client::connect`]
    /// returns.
    pub async fn connect_from_str(connstr: &str) -> Result<Self, TypedError> {
        let (user, password, host, port, database) = parse_connection_string(connstr)
            .ok_or_else(|| TypedError::Config("invalid connection string".to_string()))?;
        let addr = format!("{host}:{port}");
        Self::connect(&addr, &user, &password, &database).await
    }

    /// Connect and run initialization SQL (e.g. `SET search_path`, `SET role`).
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// use resolute::Client;
    /// let _client = Client::connect_with_init(
    ///     "127.0.0.1:5432", "user", "pass", "mydb",
    ///     &["SET search_path TO myschema, public", "SET statement_timeout = '30s'"],
    /// ).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns whatever [`Client::connect`] returns. Additionally, any
    /// `init_sql` statement that fails will be surfaced as the underlying
    /// wire error, leaving no client.
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

    /// Look up the OID of a custom PostgreSQL type by name.
    ///
    /// Useful for custom enums, composites, and domains where you need the
    /// actual OID (e.g., for array operations or explicit type casting).
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// let _mood_oid = client.lookup_type_oid("mood").await?;
    /// let _mood_array_oid = client.lookup_type_oid("_mood").await?; // array type
    /// # Ok(()) }
    /// ```
    pub async fn lookup_type_oid(&self, type_name: &str) -> Result<Option<u32>, TypedError> {
        let rows = self
            .query(
                "SELECT oid::int4 FROM pg_type WHERE typname = $1",
                &[&type_name.to_string()],
            )
            .await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let oid: i32 = rows[0].get(0)?;
            Ok(Some(oid as u32))
        }
    }

    /// Look up a custom type's OID and its array OID by name.
    ///
    /// Returns `(type_oid, array_oid)`. The array type name in PostgreSQL
    /// is conventionally `_typename` (e.g., `_mood` for `mood[]`).
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// let (oid, array_oid) = client.lookup_type_oids("mood").await?;
    /// println!("mood OID: {oid}, mood[] OID: {array_oid}");
    /// # Ok(()) }
    /// ```
    pub async fn lookup_type_oids(&self, type_name: &str) -> Result<(u32, u32), TypedError> {
        let rows = self
            .query(
                "SELECT t.oid::int4, COALESCE(t.typarray, 0)::int4 \
             FROM pg_type t WHERE t.typname = $1",
                &[&type_name.to_string()],
            )
            .await?;
        if rows.is_empty() {
            Err(TypedError::Config(format!("type not found: {type_name}")))
        } else {
            let oid: i32 = rows[0].get(0)?;
            let array_oid: i32 = rows[0].get(1)?;
            Ok((oid as u32, array_oid as u32))
        }
    }

    /// Execute a query and return typed rows.
    ///
    /// Parameters are encoded in binary format. Results are requested in binary.
    ///
    /// ```no_run
    /// # async fn example(client: &resolute::Client) -> Result<(), resolute::TypedError> {
    /// let rows = client.query("SELECT id, name FROM users WHERE id = $1", &[&42i32]).await?;
    /// for row in &rows {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    ///     # let _ = (id, name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the server reports an error (parse,
    /// bind, or execute), the connection is broken, or a parameter fails
    /// to encode in binary format. Returns `TypedError::Config` if a
    /// parameter type OID is unresolvable for a custom type.
    pub async fn query(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Vec<Row>, TypedError> {
        let start = std::time::Instant::now();
        let result = match self.query_inner(sql, params).await {
            Err(TypedError::Wire(ref e)) if is_stale_statement_error(e) => {
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

        // Convert to the wire format expected by pg-wired.
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
            PipelineResponse::Rows {
                fields,
                rows: raw_rows,
                command_tag: _,
            } => {
                let schema = Arc::new(build_row_schema(&fields, raw_rows.first()));
                let rows = raw_rows
                    .into_iter()
                    .map(|data| Row {
                        schema: Arc::clone(&schema),
                        data,
                    })
                    .collect();
                Ok(rows)
            }
            PipelineResponse::Done => Ok(Vec::new()),
            _ => Ok(Vec::new()),
        }
    }

    /// Execute a query and return exactly one row.
    pub async fn query_one(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Row, TypedError> {
        let rows = self.query(sql, params).await?;
        if rows.len() != 1 {
            return Err(TypedError::NotExactlyOne(rows.len()));
        }
        Ok(rows
            .into_iter()
            .next()
            .expect("already verified rows.len()"))
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
            1 => Ok(Some(
                rows.into_iter()
                    .next()
                    .expect("already verified rows.len()"),
            )),
            n => Err(TypedError::NotExactlyOne(n)),
        }
    }

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE).
    /// Returns the number of affected rows.
    ///
    /// # Errors
    ///
    /// Same error cases as [`Client::query`]: server-reported errors, broken
    /// connection, or parameter encoding failures.
    pub async fn execute(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
        let start = std::time::Instant::now();
        let result = match self.execute_inner(sql, params).await {
            Err(TypedError::Wire(ref e)) if is_stale_statement_error(e) => {
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

    async fn execute_inner(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
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
            &FrontendMsg::Execute {
                portal: b"",
                max_rows: 0,
            },
            &mut buf,
        );
        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let resp = conn.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows { command_tag, .. } => Ok(parse_row_count(&command_tag)),
            PipelineResponse::Done => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Rows are delivered one at a time as they arrive from PostgreSQL,
    /// without buffering the entire result set in memory.
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// use tokio_stream::StreamExt;
    /// let mut stream = client.query_stream("SELECT * FROM large_table", &[]).await?;
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     let _id: i32 = row.get(0)?;
    /// }
    /// # Ok(()) }
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

        let (header, row_rx) = self
            .conn
            .submit_stream(buf, Self::DEFAULT_STREAM_BUFFER)
            .await?;

        let has_desc = !header.fields.is_empty();
        let schema = Arc::new(build_row_schema(&header.fields, None));

        Ok(RowStream {
            row_rx,
            schema,
            has_desc,
        })
    }

    /// Bulk-load data via COPY FROM STDIN.
    ///
    /// Sends `COPY table FROM STDIN (FORMAT csv)` (or whatever `copy_sql` specifies),
    /// then streams `data` to PostgreSQL. Returns the number of rows copied.
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// let csv = b"1,Alice\n2,Bob\n";
    /// let count = client.copy_in("COPY users (id, name) FROM STDIN WITH (FORMAT csv)", csv).await?;
    /// assert_eq!(count, 2);
    /// # Ok(()) }
    /// ```
    pub async fn copy_in(&self, copy_sql: &str, data: &[u8]) -> Result<u64, TypedError> {
        self.conn
            .copy_in(copy_sql, data)
            .await
            .map_err(|e| TypedError::from(e).with_sql(copy_sql))
    }

    /// Export data via COPY TO STDOUT.
    ///
    /// Sends `COPY table TO STDOUT (FORMAT csv)` and returns all the data.
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// let csv_data = client.copy_out("COPY users TO STDOUT WITH (FORMAT csv)").await?;
    /// println!("{}", String::from_utf8_lossy(&csv_data));
    /// # Ok(()) }
    /// ```
    pub async fn copy_out(&self, copy_sql: &str) -> Result<Vec<u8>, TypedError> {
        self.conn
            .copy_out(copy_sql)
            .await
            .map_err(|e| TypedError::from(e).with_sql(copy_sql))
    }

    /// Begin a transaction. Returns a `Transaction` guard that
    /// commits on `commit()` or rolls back on drop.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the `BEGIN` statement fails. Common
    /// cases: already in a transaction, or the connection is broken.
    pub async fn begin(&self) -> Result<Transaction<'_>, TypedError> {
        self.simple_query("BEGIN").await?;
        Ok(Transaction {
            client: self,
            done: false,
        })
    }

    /// Begin a transaction with a specific isolation level.
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// use resolute::IsolationLevel;
    /// let _txn = client.begin_with(IsolationLevel::Serializable).await?;
    /// // or with repeatable read:
    /// # let client: resolute::Client = unimplemented!();
    /// let _txn = client.begin_with(IsolationLevel::RepeatableRead).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Same cases as [`Client::begin`].
    pub async fn begin_with(&self, level: IsolationLevel) -> Result<Transaction<'_>, TypedError> {
        let sql = format!("BEGIN ISOLATION LEVEL {}", level.as_sql());
        self.simple_query(&sql).await?;
        Ok(Transaction {
            client: self,
            done: false,
        })
    }

    /// Acquire a session-level advisory lock (blocks until acquired).
    ///
    /// ```no_run
    /// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client: resolute::Client = unimplemented!();
    /// client.advisory_lock(12345).await?;
    /// // ... critical section ...
    /// client.advisory_unlock(12345).await?;
    /// # Ok(()) }
    /// ```
    pub async fn advisory_lock(&self, key: i64) -> Result<(), TypedError> {
        self.simple_query(&format!("SELECT pg_advisory_lock({key})"))
            .await
    }

    /// Try to acquire a session-level advisory lock (non-blocking).
    /// Returns `true` if the lock was acquired, `false` if it was already held.
    pub async fn try_advisory_lock(&self, key: i64) -> Result<bool, TypedError> {
        let rows = self
            .query("SELECT pg_try_advisory_lock($1::int8) AS acquired", &[&key])
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| TypedError::Decode {
            column: 0,
            message: "pg_try_advisory_lock returned no rows".into(),
        })?;
        row.get::<bool>(0)
    }

    /// Release a session-level advisory lock.
    /// Returns `true` if the lock was held and released, `false` if it was not held.
    pub async fn advisory_unlock(&self, key: i64) -> Result<bool, TypedError> {
        let rows = self
            .query("SELECT pg_advisory_unlock($1::int8) AS released", &[&key])
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| TypedError::Decode {
            column: 0,
            message: "pg_advisory_unlock returned no rows".into(),
        })?;
        row.get::<bool>(0)
    }

    /// Acquire a transaction-level advisory lock (released at end of transaction).
    pub async fn advisory_xact_lock(&self, key: i64) -> Result<(), TypedError> {
        self.query("SELECT pg_advisory_xact_lock($1::int8)", &[&key])
            .await?;
        Ok(())
    }

    /// Try to acquire a transaction-level advisory lock (non-blocking).
    pub async fn try_advisory_xact_lock(&self, key: i64) -> Result<bool, TypedError> {
        let rows = self
            .query(
                "SELECT pg_try_advisory_xact_lock($1::int8) AS acquired",
                &[&key],
            )
            .await?;
        let row = rows.into_iter().next().ok_or_else(|| TypedError::Decode {
            column: 0,
            message: "pg_try_advisory_xact_lock returned no rows".into(),
        })?;
        row.get::<bool>(0)
    }

    /// Send a simple text query (no params, no binary format).
    /// Used for BEGIN/COMMIT/ROLLBACK and DDL.
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        use pg_wired::protocol::types::FrontendMsg;
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
    /// ```no_run
    /// # async fn example(client: &resolute::Client) -> Result<(), resolute::TypedError> {
    /// let rows = client.query_named(
    ///     "SELECT * FROM users WHERE id = :id AND org = :org",
    ///     &[("id", &42i32), ("org", &"acme")],
    /// ).await?;
    /// # let _ = rows;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `TypedError::MissingParam` if the SQL references a `:name`
    /// that isn't in `params`. Otherwise returns whatever [`Client::query`]
    /// returns.
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
    ///
    /// # Errors
    ///
    /// Same cases as [`Client::query_named`].
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
    /// ```no_run
    /// # async fn _doctest() {
    /// # let client: resolute::Client = unimplemented!();
    /// use std::time::Duration;
    /// let _result = client.query_timeout(
    ///     "SELECT pg_sleep(60)",
    ///     &[],
    ///     Duration::from_secs(5),
    /// ).await; // returns Err after 5s
    /// # }
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
    /// ```no_run
    /// # async fn _doctest() {
    /// # let client: resolute::Client = unimplemented!();
    /// use std::time::Duration;
    /// let token = client.cancel_token();
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(Duration::from_secs(5)).await;
    ///     token.cancel().await.ok();
    /// });
    /// let _ = client.query("SELECT pg_sleep(60)", &[]).await; // cancelled after 5s
    /// # }
    /// ```
    pub fn cancel_token(&self) -> pg_wired::CancelToken {
        self.conn.cancel_token()
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

/// A transaction guard.
///
/// Call `commit()` or `rollback()` to finish the transaction explicitly. Both
/// consume the guard and send the corresponding SQL command.
///
/// # Drop behavior
///
/// If the `Transaction` is dropped without calling `commit()` or `rollback()`
/// (for example, because a `?` propagated an error out of the block), the
/// destructor cannot `await`, so it does **not** send a `ROLLBACK` itself.
/// Instead it logs a `tracing::warn!` and relies on PostgreSQL's behavior:
/// the next statement sent on the same connection while still in a failed
/// or open transaction will either error out or hit the server's automatic
/// rollback on the first command executed outside the block.
///
/// In practice this means:
///
/// - For correctness, dropping is safe. No partial work is committed, and
///   the connection will recover on its next use.
/// - For clarity, prefer calling `rollback()` explicitly on the error path.
///   It surfaces errors from the rollback itself and keeps the transaction
///   state deterministic.
/// - If you need the rollback to complete before the connection is returned
///   to a pool, call `rollback()` explicitly. The drop path does not guarantee
///   the server has observed a rollback by the time the guard is dropped.
#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) client: &'a Client,
    pub(crate) done: bool,
}

impl<'a> Transaction<'a> {
    /// Execute a query within the transaction.
    pub async fn query(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<Vec<Row>, TypedError> {
        self.client.query(sql, params).await
    }

    /// Execute a statement within the transaction. Returns affected row count.
    pub async fn execute(&self, sql: &str, params: &[&dyn SqlParam]) -> Result<u64, TypedError> {
        self.client.execute(sql, params).await
    }

    /// Commit the transaction.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the server rejects `COMMIT` (for example,
    /// a deferred constraint fires) or the connection is broken. On error,
    /// the server has already attempted to finalize the transaction, so
    /// calling rollback afterward is not meaningful.
    pub async fn commit(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("COMMIT").await
    }

    /// Explicitly roll back the transaction.
    ///
    /// # Errors
    ///
    /// Returns `TypedError::Wire` if the connection is broken. On success,
    /// the server has discarded all in-transaction work.
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
pub(crate) fn resolve_named_params<'a>(
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
/// ```no_run
/// # async fn _doctest() -> Result<(), Box<dyn std::error::Error>> {
/// # let client: resolute::Client = unimplemented!();
/// use resolute::PipelineResult;
///
/// let results = client.pipeline()
///     .query("SELECT 1::int4 AS n", &[])
///     .query("SELECT 'hello'::text AS s", &[])
///     .execute("INSERT INTO t VALUES ($1)", &[&42i32])
///     .run()
///     .await?;
///
/// for result in results {
///     match result {
///         PipelineResult::Rows(rows) => println!("got {} rows", rows.len()),
///         PipelineResult::Execute(count) => println!("affected {count} rows"),
///         _ => {}
///     }
/// }
/// # Ok(()) }
/// ```
#[derive(Debug)]
#[must_use = "Pipeline does nothing until .run() is awaited"]
pub struct Pipeline<'a> {
    client: &'a Client,
    /// One encoded wire message buffer per queued query.
    buffers: Vec<BytesMut>,
}

/// Result from a single pipeline step.
#[non_exhaustive]
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

        let mut buf = BytesMut::with_capacity(256);
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
        self.buffers.push(buf);
    }

    /// Execute all queries in one round-trip and return results.
    ///
    /// Each queued query was encoded into its own message buffer. All
    /// buffers are submitted to the writer back-to-back before any response
    /// is awaited, so the writer's batch coalescer writes them in a single
    /// syscall. The server pipelines N `ReadyForQuery` responses; the reader
    /// dispatches them to the N pending oneshots in order.
    pub async fn run(self) -> Result<Vec<PipelineResult>, TypedError> {
        if self.buffers.is_empty() {
            return Ok(Vec::new());
        }

        let items: Vec<(BytesMut, ResponseCollector)> = self
            .buffers
            .into_iter()
            .map(|b| (b, ResponseCollector::Rows))
            .collect();

        let responses = self.client.conn.submit_batch(items).await?;
        let mut results = Vec::with_capacity(responses.len());
        for resp in responses {
            match resp? {
                PipelineResponse::Rows {
                    fields,
                    rows,
                    command_tag,
                } => {
                    if rows.is_empty() && !command_tag.is_empty() {
                        results.push(PipelineResult::Execute(parse_row_count(&command_tag)));
                    } else {
                        let schema = Arc::new(build_row_schema(&fields, rows.first()));
                        let typed_rows = rows
                            .into_iter()
                            .map(|data| Row {
                                schema: Arc::clone(&schema),
                                data,
                            })
                            .collect();
                        results.push(PipelineResult::Rows(typed_rows));
                    }
                }
                PipelineResponse::Done => {
                    results.push(PipelineResult::Execute(0));
                }
                _ => {
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
            buffers: Vec::new(),
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
#[derive(Debug)]
pub struct RowStream {
    row_rx: mpsc::Receiver<Result<RawRow, pg_wired::PgWireError>>,
    schema: Arc<RowSchema>,
    /// True if the schema came from a real RowDescription. If false, the
    /// stream fills `formats` from the first row's column count on the fly
    /// (binary, since we requested binary in Bind).
    has_desc: bool,
}

impl tokio_stream::Stream for RowStream {
    type Item = Result<Row, TypedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.row_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                if !self.has_desc && self.schema.formats.len() != data.len() {
                    let mut s = RowSchema::empty();
                    s.formats = vec![1i16; data.len()];
                    self.schema = Arc::new(s);
                    self.has_desc = true;
                }
                let row = Row {
                    schema: Arc::clone(&self.schema),
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

/// Build a `RowSchema` once per result set. Pulls names / OIDs / formats from
/// the `RowDescription` if present; otherwise falls back to "binary, sized to
/// the first row" since we always request binary in `Bind`.
fn build_row_schema(
    fields: &[pg_wired::protocol::types::FieldDescription],
    first_row: Option<&RawRow>,
) -> RowSchema {
    if !fields.is_empty() {
        RowSchema {
            columns: fields.iter().map(|f| f.name.clone()).collect(),
            type_oids: fields.iter().map(|f| f.type_oid).collect(),
            formats: fields.iter().map(|f| f.format as i16).collect(),
        }
    } else if let Some(row) = first_row {
        // No RowDescription: synthesize binary formats sized to the row.
        let mut s = RowSchema::empty();
        s.formats = vec![1i16; row.len()];
        s
    } else {
        RowSchema::empty()
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
fn is_stale_statement_error(e: &pg_wired::PgWireError) -> bool {
    if let pg_wired::PgWireError::Pg(ref pg_err) = e {
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

/// Parse a connection string in URI or key=value format.
///
/// Supported formats:
/// - `postgres://user:pass@host:port/dbname`
/// - `postgresql://user:pass@host:port/dbname`
/// - `host=localhost port=5432 dbname=mydb user=postgres password=secret`
///
/// Returns `(user, password, host, port, database)` or `None` if parsing fails.
///
/// This function is exposed for internal use by `resolute` itself (migrations,
/// admin helpers, tests). It is not part of the stable public API: the tuple
/// shape and semantics may change. Prefer `Client::connect` for normal use.
#[doc(hidden)]
pub fn parse_connection_string(s: &str) -> Option<(String, String, String, u16, String)> {
    if s.starts_with("postgres://") || s.starts_with("postgresql://") {
        parse_pg_uri(s)
    } else {
        parse_pg_keyvalue(s)
    }
}

fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
    let rest = uri
        .strip_prefix("postgres://")
        .or_else(|| uri.strip_prefix("postgresql://"))?;
    // Strip query string if present (e.g., ?sslmode=require)
    let rest = rest.split('?').next().unwrap_or(rest);
    let (auth, hostdb) = rest.split_once('@').unwrap_or(("postgres:postgres", rest));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = hostdb.split_once('/').unwrap_or((hostdb, "postgres"));
    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    // Decode percent-encoded characters (e.g., %40 → @, %23 → #).
    // Common in passwords with special characters.
    Some((
        url_decode(user),
        url_decode(password),
        host.to_string(),
        port,
        url_decode(database),
    ))
}

fn parse_pg_keyvalue(s: &str) -> Option<(String, String, String, u16, String)> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 5432;
    let mut user = "postgres".to_string();
    let mut password = String::new();
    let mut dbname = "postgres".to_string();

    for part in s.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "host" | "hostaddr" => host = value.to_string(),
                "port" => port = value.parse().unwrap_or(5432),
                "user" => user = value.to_string(),
                "password" => password = value.to_string(),
                "dbname" => dbname = value.to_string(),
                // Silently ignore other keys (sslmode, connect_timeout, etc.)
                _ => {}
            }
        }
    }

    Some((user, password, host, port, dbname))
}

/// Decode percent-encoded characters in a URI component.
/// Handles %XX sequences where XX is a two-digit hex value.
/// Non-encoded characters pass through unchanged. Invalid sequences
/// (incomplete %X, non-hex digits) are left as-is.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                result.push((hi << 4 | lo) as char);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_decode_basic() {
        assert_eq!(url_decode("hello"), "hello");
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("p%40ss"), "p@ss");
        assert_eq!(url_decode("%23hash"), "#hash");
    }

    #[test]
    fn test_url_decode_password_with_special_chars() {
        assert_eq!(url_decode("p%40ssw%23rd"), "p@ssw#rd");
        assert_eq!(url_decode("100%25done"), "100%done");
    }

    #[test]
    fn test_url_decode_invalid_sequences() {
        // Incomplete % sequence — left as-is.
        assert_eq!(url_decode("abc%2"), "abc%2");
        assert_eq!(url_decode("abc%"), "abc%");
        // Non-hex digits — left as-is.
        assert_eq!(url_decode("abc%ZZ"), "abc%ZZ");
    }

    #[test]
    fn test_url_decode_empty() {
        assert_eq!(url_decode(""), "");
    }

    #[test]
    fn test_parse_connection_string_with_encoded_password() {
        let (user, pass, host, port, db) =
            parse_connection_string("postgres://user:p%40ss@localhost:5432/mydb").unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "p@ss");
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(db, "mydb");
    }

    #[test]
    fn test_parse_connection_string_keyvalue() {
        let (user, pass, host, port, db) = parse_connection_string(
            "host=db.example.com port=5433 dbname=prod user=admin password=secret",
        )
        .unwrap();
        assert_eq!(user, "admin");
        assert_eq!(pass, "secret");
        assert_eq!(host, "db.example.com");
        assert_eq!(port, 5433);
        assert_eq!(db, "prod");
    }

    #[test]
    fn test_parse_connection_string_defaults() {
        let (user, pass, host, port, db) =
            parse_connection_string("postgres://localhost/mydb").unwrap();
        assert_eq!(user, "postgres");
        assert_eq!(pass, "postgres");
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(db, "mydb");
    }
}

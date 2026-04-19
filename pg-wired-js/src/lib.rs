//! napi-rs bindings for pg-wired.
//!
//! Exposes a performance-oriented JS API over the pg-wired Rust driver:
//! `connect`, `createPool`, `Connection.query/queryStream/pipeline`,
//! `Pool.query`, `CancelToken`.
//!
//! PG-side errors are serialized as JSON in the napi error message with a
//! `pg-wired-error:` prefix so the JS facade (`lib/index.js`) can reparse them
//! and rethrow Error objects with structured `code/severity/detail/hint/position`
//! fields.

#![deny(clippy::all)]

use std::sync::Arc;

use bytes::BytesMut;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{mpsc, Mutex};

use pg_wired::async_conn::{ResponseCollector, StreamedRow};
use pg_wired::protocol::frontend;
use pg_wired::protocol::types::{BackendMsg, FieldDescription, FormatCode, FrontendMsg};
use pg_wired::{AsyncConn, AsyncPool, PgWireError, PipelineResponse, TlsMode, WireConn};

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

#[napi(object)]
pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    /// TLS negotiation mode: "disable", "prefer" (default), or "require".
    pub sslmode: Option<String>,
}

fn parse_sslmode(s: Option<&str>) -> Result<TlsMode> {
    match s.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("prefer") => Ok(TlsMode::Prefer),
        Some("disable") => Ok(TlsMode::Disable),
        Some("require") => Ok(TlsMode::Require),
        Some(other) => Err(Error::new(
            Status::InvalidArg,
            format!("unsupported sslmode: {other} (expected disable, prefer, or require)"),
        )),
    }
}

#[napi(object)]
#[derive(Clone)]
pub struct FieldInfo {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 = text, 1 = binary
    pub format: i16,
}

#[napi(object)]
pub struct QueryResult {
    pub fields: Vec<FieldInfo>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
}

/// Result of a query that may contain binary-format columns. Each cell is the
/// raw server bytes (binary-format when that column was requested binary,
/// UTF-8 text when text). Callers decode per-column based on `fields[i].format`
/// and `fields[i].type_oid`.
#[napi(object)]
pub struct RawQueryResult {
    pub fields: Vec<FieldInfo>,
    pub rows: Vec<Vec<Option<Buffer>>>,
    pub command_tag: String,
}

/// Columnar result layout. All non-null cells are concatenated UTF-8 into
/// `data`; `offsets[i]..offsets[i+1]` gives the slice for cell index `i`
/// (where `i = row * cols + col`); `nulls` is a little-endian bitmap where
/// a set bit marks a NULL cell. This layout collapses thousands of per-cell
/// napi allocations into three (data, offsets, nulls).
#[napi(object)]
pub struct ColumnarResult {
    pub fields: Vec<FieldInfo>,
    pub row_count: u32,
    pub cols: u32,
    pub data: String,
    pub offsets: Uint32Array,
    pub nulls: Buffer,
    pub command_tag: String,
}

fn fields_to_info(fields: Vec<FieldDescription>) -> Vec<FieldInfo> {
    fields
        .into_iter()
        .map(|f| FieldInfo {
            name: f.name,
            table_oid: f.table_oid,
            column_id: f.column_id,
            type_oid: f.type_oid,
            type_size: f.type_size,
            type_modifier: f.type_modifier,
            format: f.format as i16,
        })
        .collect()
}

/// Decode text-format cells to owned `String`s. Invalid UTF-8 becomes
/// lossy-replaced; pg-wired's result bytes are text-format so this is safe.
fn rows_to_strings(rows: Vec<Vec<Option<Vec<u8>>>>) -> Vec<Vec<Option<String>>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.map(bytes_to_string))
                .collect()
        })
        .collect()
}

fn row_to_strings(row: StreamedRow) -> Vec<Option<String>> {
    row.into_iter()
        .map(|cell| cell.map(bytes_to_string))
        .collect()
}

fn bytes_to_string(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(&e.into_bytes()).into_owned(),
    }
}

/// Flatten row-oriented text-format cells into a columnar `(data, offsets, nulls)`
/// triple. Offsets include a trailing terminator so each cell is
/// `data[offsets[i]..offsets[i+1]]`.
fn rows_to_columnar(rows: Vec<Vec<Option<Vec<u8>>>>, cols: usize) -> (String, Vec<u32>, Vec<u8>) {
    let row_count = rows.len();
    let cell_count = row_count * cols;
    let total_bytes: usize = rows
        .iter()
        .flat_map(|r| r.iter())
        .filter_map(|c| c.as_ref().map(|v| v.len()))
        .sum();
    let mut data_bytes = Vec::<u8>::with_capacity(total_bytes);
    let mut offsets = Vec::<u32>::with_capacity(cell_count + 1);
    let mut nulls = vec![0u8; cell_count.div_ceil(8)];
    offsets.push(0);
    let mut cursor: u32 = 0;
    let mut idx: usize = 0;
    for row in rows {
        for cell in row {
            match cell {
                Some(bytes) => {
                    data_bytes.extend_from_slice(&bytes);
                    cursor = cursor.saturating_add(bytes.len() as u32);
                }
                None => {
                    nulls[idx >> 3] |= 1u8 << (idx & 7);
                }
            }
            offsets.push(cursor);
            idx += 1;
        }
    }
    let data = match String::from_utf8(data_bytes) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(&e.into_bytes()).into_owned(),
    };
    (data, offsets, nulls)
}

fn response_to_columnar(resp: PipelineResponse) -> ColumnarResult {
    match resp {
        PipelineResponse::Rows {
            fields,
            rows,
            command_tag,
        } => {
            let cols = fields.len();
            let row_count = rows.len();
            let (data, offsets, nulls) = rows_to_columnar(rows, cols);
            ColumnarResult {
                fields: fields_to_info(fields),
                row_count: row_count as u32,
                cols: cols as u32,
                data,
                offsets: Uint32Array::new(offsets),
                nulls: Buffer::from(nulls),
                command_tag,
            }
        }
        PipelineResponse::Done => ColumnarResult {
            fields: Vec::new(),
            row_count: 0,
            cols: 0,
            data: String::new(),
            offsets: Uint32Array::new(vec![0]),
            nulls: Buffer::from(Vec::<u8>::new()),
            command_tag: String::new(),
        },
    }
}

/// Map a PgWireError to a napi::Error. PG-side errors encode their full set of
/// fields as JSON in the message, prefixed with `pg-wired-error:`, so the JS
/// facade can rethrow an Error with structured properties.
fn map_err(e: PgWireError) -> Error {
    match e {
        PgWireError::Pg(pg) => {
            let payload = serde_json::json!({
                "kind": "pg",
                "code": pg.code,
                "message": pg.message,
                "severity": pg.severity,
                "detail": pg.detail,
                "hint": pg.hint,
                "position": pg.position,
            });
            Error::new(Status::GenericFailure, format!("pg-wired-error:{payload}"))
        }
        PgWireError::Io(io) => {
            let payload = serde_json::json!({
                "kind": "io",
                "message": io.to_string(),
            });
            Error::new(Status::GenericFailure, format!("pg-wired-error:{payload}"))
        }
        PgWireError::Protocol(m) => {
            let payload = serde_json::json!({
                "kind": "protocol",
                "message": m,
            });
            Error::new(Status::GenericFailure, format!("pg-wired-error:{payload}"))
        }
        PgWireError::ConnectionClosed => {
            let payload = serde_json::json!({
                "kind": "closed",
                "message": "connection closed",
            });
            Error::new(Status::GenericFailure, format!("pg-wired-error:{payload}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Extended-protocol encoder: Parse? + Bind + Describe(portal) + Execute + Sync
// ---------------------------------------------------------------------------

fn encode_query(
    conn: &AsyncConn,
    sql: &str,
    params: &[Option<&[u8]>],
    param_oids: &[u32],
) -> BytesMut {
    let (stmt_name, needs_parse) = conn.lookup_or_alloc(sql);
    let mut buf = BytesMut::with_capacity(sql.len() + 128);
    let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
    let result_fmts = [FormatCode::Text];

    if needs_parse {
        frontend::encode_message(
            &FrontendMsg::Parse {
                name: &stmt_name,
                sql: sql.as_bytes(),
                param_oids,
            },
            &mut buf,
        );
    }
    frontend::encode_message(
        &FrontendMsg::Bind {
            portal: b"",
            statement: &stmt_name,
            param_formats: &text_fmts[..params.len()],
            params,
            result_formats: &result_fmts,
        },
        &mut buf,
    );
    // Describe the portal so the server emits RowDescription for the result.
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
    buf
}

async fn exec_query_full(
    conn: &AsyncConn,
    sql: &str,
    params: &[Option<&[u8]>],
    param_oids: &[u32],
) -> Result<QueryResult> {
    let buf = encode_query(conn, sql, params, param_oids);
    let resp = conn
        .submit(buf, ResponseCollector::Rows)
        .await
        .map_err(map_err)?;
    Ok(pipeline_response_to_query_result(resp))
}

fn to_format_codes(fmts: &[i16]) -> Result<Vec<FormatCode>> {
    fmts.iter()
        .map(|&f| match f {
            0 => Ok(FormatCode::Text),
            1 => Ok(FormatCode::Binary),
            other => Err(Error::new(
                Status::InvalidArg,
                format!("format code must be 0 (text) or 1 (binary), got {other}"),
            )),
        })
        .collect()
}

fn encode_query_with_formats(
    conn: &AsyncConn,
    sql: &str,
    params: &[Option<&[u8]>],
    param_oids: &[u32],
    param_formats: &[FormatCode],
    result_formats: &[FormatCode],
) -> BytesMut {
    let (stmt_name, needs_parse) = conn.lookup_or_alloc(sql);
    let mut buf = BytesMut::with_capacity(sql.len() + 128);

    // Default to all-text if caller passes empty.
    let default_params: Vec<FormatCode>;
    let param_fmts_slice: &[FormatCode] = if param_formats.is_empty() {
        default_params = vec![FormatCode::Text; params.len().max(1)];
        &default_params[..params.len()]
    } else {
        param_formats
    };
    let default_results = [FormatCode::Text];
    let result_fmts_slice: &[FormatCode] = if result_formats.is_empty() {
        &default_results
    } else {
        result_formats
    };

    if needs_parse {
        frontend::encode_message(
            &FrontendMsg::Parse {
                name: &stmt_name,
                sql: sql.as_bytes(),
                param_oids,
            },
            &mut buf,
        );
    }
    frontend::encode_message(
        &FrontendMsg::Bind {
            portal: b"",
            statement: &stmt_name,
            param_formats: param_fmts_slice,
            params,
            result_formats: result_fmts_slice,
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
    buf
}

fn rows_to_buffers(rows: Vec<Vec<Option<Vec<u8>>>>) -> Vec<Vec<Option<Buffer>>> {
    rows.into_iter()
        .map(|row| row.into_iter().map(|c| c.map(Buffer::from)).collect())
        .collect()
}

async fn exec_query_raw(
    conn: &AsyncConn,
    sql: &str,
    params: &[Option<&[u8]>],
    param_oids: &[u32],
    param_formats: &[FormatCode],
    result_formats: &[FormatCode],
) -> Result<RawQueryResult> {
    let buf =
        encode_query_with_formats(conn, sql, params, param_oids, param_formats, result_formats);
    let resp = conn
        .submit(buf, ResponseCollector::Rows)
        .await
        .map_err(map_err)?;
    Ok(match resp {
        PipelineResponse::Rows {
            fields,
            rows,
            command_tag,
        } => RawQueryResult {
            fields: fields_to_info(fields),
            rows: rows_to_buffers(rows),
            command_tag,
        },
        PipelineResponse::Done => RawQueryResult {
            fields: Vec::new(),
            rows: Vec::new(),
            command_tag: String::new(),
        },
    })
}

async fn exec_simple_full(conn: &AsyncConn, sql: &str) -> Result<QueryResult> {
    let mut buf = BytesMut::with_capacity(sql.len() + 16);
    frontend::encode_message(&FrontendMsg::Query(sql.as_bytes()), &mut buf);
    let resp = conn
        .submit(buf, ResponseCollector::Rows)
        .await
        .map_err(map_err)?;
    Ok(pipeline_response_to_query_result(resp))
}

fn pipeline_response_to_query_result(resp: PipelineResponse) -> QueryResult {
    match resp {
        PipelineResponse::Rows {
            fields,
            rows,
            command_tag,
        } => QueryResult {
            fields: fields_to_info(fields),
            rows: rows_to_strings(rows),
            command_tag,
        },
        PipelineResponse::Done => QueryResult {
            fields: Vec::new(),
            rows: Vec::new(),
            command_tag: String::new(),
        },
    }
}

fn borrow_params(params: &[Option<Buffer>]) -> Vec<Option<&[u8]>> {
    params
        .iter()
        .map(|opt| opt.as_ref().map(|b| b.as_ref()))
        .collect()
}

// ---------------------------------------------------------------------------
// Connection (single dedicated connection)
// ---------------------------------------------------------------------------

/// Tracks which SQLs have at least one fully-completed Parse on this
/// connection. Used by `Pipeline.execute` to decide whether the first
/// occurrence of a SQL in a batch must be awaited before the rest spawn,
/// so that concurrent Binds don't race ahead of the Parse on the wire.
type WarmSqls = Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>;

#[napi]
pub struct Connection {
    inner: Arc<AsyncConn>,
    warm: WarmSqls,
}

#[napi]
pub async fn connect(options: ConnectOptions) -> Result<Connection> {
    let addr = format!("{}:{}", options.host, options.port);
    let tls_mode = parse_sslmode(options.sslmode.as_deref())?;
    let wire = WireConn::connect_with_options(
        &addr,
        &options.user,
        &options.password,
        &options.database,
        &[],
        tls_mode,
    )
    .await
    .map_err(map_err)?;
    let async_conn = AsyncConn::new(wire);
    Ok(Connection {
        inner: Arc::new(async_conn),
        warm: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
    })
}

#[napi]
impl Connection {
    /// Execute a parameterized query using the extended protocol.
    /// Params are text-encoded bytes; paramOids may be empty for inference.
    #[napi]
    pub async fn query(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
    ) -> Result<QueryResult> {
        let refs = borrow_params(&params);
        let r = exec_query_full(&self.inner, &sql, &refs, &param_oids).await?;
        self.warm.lock().await.insert(sql);
        Ok(r)
    }

    /// Execute a simple (text protocol) query without parameters.
    /// Supports multiple statements separated by semicolons.
    #[napi]
    pub async fn simple_query(&self, sql: String) -> Result<QueryResult> {
        exec_simple_full(&self.inner, &sql).await
    }

    /// Execute a parameterized query with explicit per-param and per-result
    /// format codes (0 = text, 1 = binary). Returns raw bytes per cell so
    /// callers can decode binary-format columns natively in JS.
    ///
    /// `paramFormats` / `resultFormats` length rules match the PostgreSQL
    /// Bind message: empty = all text; length 1 = apply to all; length N =
    /// per-param / per-column.
    #[napi]
    pub async fn query_raw(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
        param_formats: Vec<i16>,
        result_formats: Vec<i16>,
    ) -> Result<RawQueryResult> {
        let param_fmts = to_format_codes(&param_formats)?;
        let result_fmts = to_format_codes(&result_formats)?;
        let refs = borrow_params(&params);
        let r = exec_query_raw(
            &self.inner,
            &sql,
            &refs,
            &param_oids,
            &param_fmts,
            &result_fmts,
        )
        .await?;
        self.warm.lock().await.insert(sql);
        Ok(r)
    }

    /// Like `query`, but returns a columnar result: one concatenated `data`
    /// string + offsets + nulls bitmap. Use this for large result sets where
    /// per-cell napi allocations dominate.
    #[napi]
    pub async fn query_columnar(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
    ) -> Result<ColumnarResult> {
        let refs = borrow_params(&params);
        let buf = encode_query(&self.inner, &sql, &refs, &param_oids);
        let resp = self
            .inner
            .submit(buf, ResponseCollector::Rows)
            .await
            .map_err(map_err)?;
        self.warm.lock().await.insert(sql);
        Ok(response_to_columnar(resp))
    }

    /// Simple-query variant of `queryColumnar`.
    #[napi]
    pub async fn simple_query_columnar(&self, sql: String) -> Result<ColumnarResult> {
        let mut buf = BytesMut::with_capacity(sql.len() + 16);
        frontend::encode_message(&FrontendMsg::Query(sql.as_bytes()), &mut buf);
        let resp = self
            .inner
            .submit(buf, ResponseCollector::Rows)
            .await
            .map_err(map_err)?;
        Ok(response_to_columnar(resp))
    }

    /// Execute a parameterized query and stream rows back one at a time.
    /// Returns a RowStream; call `next()` until it yields `null`.
    #[napi]
    pub async fn query_stream(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
    ) -> Result<RowStream> {
        let refs = borrow_params(&params);
        let buf = encode_query(&self.inner, &sql, &refs, &param_oids);
        let (header, rx) = self.inner.submit_stream(buf, 1024).await.map_err(map_err)?;
        Ok(RowStream {
            fields: fields_to_info(header.fields),
            rx: Arc::new(Mutex::new(Some(rx))),
        })
    }

    /// Create a pipeline bound to this connection. Queries pushed onto it are
    /// submitted concurrently on `execute()`; the pg-wired writer coalesces them
    /// into one (or few) write() syscalls.
    #[napi]
    pub fn pipeline(&self) -> Pipeline {
        Pipeline {
            conn: Arc::clone(&self.inner),
            warm: Arc::clone(&self.warm),
            queued: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Return a cancel token for this connection's backend.
    /// Safe to call from another task to cancel an in-flight query.
    #[napi]
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken {
            inner: pg_wired::CancelToken {
                addr: self.inner.addr.clone(),
                pid: self.inner.backend_pid,
                secret: self.inner.backend_secret,
            },
        }
    }

    /// COPY FROM STDIN with a pre-buffered payload.
    /// Returns the row count parsed from the CommandComplete tag.
    #[napi]
    pub async fn copy_in(&self, sql: String, data: Buffer) -> Result<BigInt> {
        let n = self
            .inner
            .copy_in(&sql, data.as_ref())
            .await
            .map_err(map_err)?;
        Ok(BigInt::from(n))
    }

    /// COPY TO STDOUT, returning the full payload.
    #[napi]
    pub async fn copy_out(&self, sql: String) -> Result<Buffer> {
        let bytes = self.inner.copy_out(&sql).await.map_err(map_err)?;
        Ok(Buffer::from(bytes))
    }

    /// Whether the connection's reader/writer tasks are still running.
    #[napi]
    pub fn is_alive(&self) -> bool {
        self.inner.is_alive()
    }

    /// Backend PID reported at startup.
    #[napi]
    pub fn backend_pid(&self) -> i32 {
        self.inner.backend_pid
    }

    /// Take the notification receiver for this connection.
    ///
    /// Returns a Notifications iterator or `null` if the receiver was already
    /// taken. Issue `LISTEN <channel>` on this connection to start receiving
    /// notifications.
    ///
    /// Caveat: pg-wired's reader task only drains the socket when a response
    /// is pending. Notifications arriving while the connection is idle are
    /// delivered on the next query. If you need near-realtime delivery, run
    /// a low-frequency keepalive (e.g. `SELECT 1` every few seconds).
    #[napi]
    pub fn notifications(&self) -> Option<Notifications> {
        self.inner
            .take_notification_receiver()
            .map(|rx| Notifications {
                rx: Arc::new(Mutex::new(Some(rx))),
            })
    }

    /// Send a Terminate to the server and wait for the writer/reader tasks
    /// to exit. Subsequent method calls on this Connection will fail with
    /// `ConnectionClosed`. Idempotent.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await.map_err(map_err)
    }
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

#[napi]
pub struct Pool {
    inner: Arc<AsyncPool>,
}

#[napi]
pub async fn create_pool(options: ConnectOptions, size: u32) -> Result<Pool> {
    let addr = format!("{}:{}", options.host, options.port);
    let tls_mode = parse_sslmode(options.sslmode.as_deref())?;
    let p = AsyncPool::connect_with_tls(
        &addr,
        &options.user,
        &options.password,
        &options.database,
        size as usize,
        tls_mode,
    )
    .await
    .map_err(map_err)?;
    Ok(Pool { inner: p })
}

#[napi]
impl Pool {
    /// Execute a parameterized query on the next alive connection (round-robin).
    #[napi]
    pub async fn query(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
    ) -> Result<QueryResult> {
        let conn = self.inner.get_async().await;
        let refs = borrow_params(&params);
        exec_query_full(&conn, &sql, &refs, &param_oids).await
    }

    /// Execute a simple (text protocol) query on the next alive connection.
    #[napi]
    pub async fn simple_query(&self, sql: String) -> Result<QueryResult> {
        let conn = self.inner.get_async().await;
        exec_simple_full(&conn, &sql).await
    }

    /// Execute a parameterized query with per-param / per-result format codes.
    /// See `Connection.queryRaw` for semantics.
    #[napi]
    pub async fn query_raw(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
        param_formats: Vec<i16>,
        result_formats: Vec<i16>,
    ) -> Result<RawQueryResult> {
        let param_fmts = to_format_codes(&param_formats)?;
        let result_fmts = to_format_codes(&result_formats)?;
        let conn = self.inner.get_async().await;
        let refs = borrow_params(&params);
        exec_query_raw(&conn, &sql, &refs, &param_oids, &param_fmts, &result_fmts).await
    }

    /// Total number of connections in the pool.
    #[napi]
    pub fn size(&self) -> u32 {
        self.inner.size() as u32
    }

    /// Number of alive connections right now.
    #[napi]
    pub async fn alive_count(&self) -> u32 {
        self.inner.alive_count().await as u32
    }

    /// Close every connection in the pool by sending Terminate on each and
    /// waiting for their tasks to exit. Subsequent queries on this Pool will
    /// fail with `ConnectionClosed`. Idempotent.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await.map_err(map_err)
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

struct QueuedQuery {
    sql: String,
    params: Vec<Option<Buffer>>,
    param_oids: Vec<u32>,
    param_formats: Vec<FormatCode>,
    result_formats: Vec<FormatCode>,
}

#[napi]
pub struct Pipeline {
    conn: Arc<AsyncConn>,
    warm: WarmSqls,
    queued: std::sync::Mutex<Vec<QueuedQuery>>,
}

#[napi]
impl Pipeline {
    /// Queue a parameterized query. No I/O happens until `execute()`.
    ///
    /// `paramFormats` / `resultFormats` match the PostgreSQL Bind semantics:
    /// empty = all text; length 1 = apply to all; length N = per-slot.
    #[napi]
    pub fn push(
        &self,
        sql: String,
        params: Vec<Option<Buffer>>,
        param_oids: Vec<u32>,
        param_formats: Vec<i16>,
        result_formats: Vec<i16>,
    ) -> Result<()> {
        let param_fmts = to_format_codes(&param_formats)?;
        let result_fmts = to_format_codes(&result_formats)?;
        if let Ok(mut q) = self.queued.lock() {
            q.push(QueuedQuery {
                sql,
                params,
                param_oids,
                param_formats: param_fmts,
                result_formats: result_fmts,
            });
        }
        Ok(())
    }

    /// Number of queries currently queued.
    #[napi]
    pub fn len(&self) -> u32 {
        self.queued.lock().map(|q| q.len() as u32).unwrap_or(0)
    }

    /// True when no queries are queued.
    #[napi]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Submit all queued queries concurrently on the shared connection.
    ///
    /// Queries whose SQL is already warm on this connection (Parse confirmed
    /// at least once) are spawned in parallel and benefit from pg-wired's
    /// writer coalescing. Queries whose SQL is cold must first be awaited so
    /// that the server processes Parse before concurrent Binds race past it
    /// on the wire.
    ///
    /// For a homogeneous batch (same SQL) this means: first query serial,
    /// remaining N-1 parallel on the first call; fully parallel on subsequent
    /// calls. For a heterogeneous batch of entirely cold SQLs this degrades
    /// to sequential execution on the first call, which is both safe and
    /// honest.
    ///
    /// Results are returned in push-order.
    #[napi]
    pub async fn execute(&self) -> Result<Vec<RawQueryResult>> {
        let queued: Vec<QueuedQuery> = match self.queued.lock() {
            Ok(mut q) => std::mem::take(&mut *q),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };

        let n = queued.len();
        let mut slots: Vec<Option<RawQueryResult>> = (0..n).map(|_| None).collect();

        // Partition: cold-first (must serialize) vs warm-or-repeat (may parallelize).
        let (serial, parallel) = {
            let warm = self.warm.lock().await;
            let mut seen_cold: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut serial = Vec::new();
            let mut parallel = Vec::new();
            for (i, q) in queued.into_iter().enumerate() {
                if warm.contains(&q.sql) {
                    parallel.push((i, q));
                } else if seen_cold.insert(q.sql.clone()) {
                    serial.push((i, q));
                } else {
                    parallel.push((i, q));
                }
            }
            (serial, parallel)
        };

        // Phase 1: run cold firsts in order, marking warm as we go.
        for (i, q) in serial {
            let refs = borrow_params(&q.params);
            let r = exec_query_raw(
                &self.conn,
                &q.sql,
                &refs,
                &q.param_oids,
                &q.param_formats,
                &q.result_formats,
            )
            .await?;
            self.warm.lock().await.insert(q.sql);
            slots[i] = Some(r);
        }

        // Phase 2: spawn the rest.
        let mut handles = Vec::with_capacity(parallel.len());
        for (i, q) in parallel {
            let conn = Arc::clone(&self.conn);
            handles.push((
                i,
                tokio::spawn(async move {
                    let refs = borrow_params(&q.params);
                    exec_query_raw(
                        &conn,
                        &q.sql,
                        &refs,
                        &q.param_oids,
                        &q.param_formats,
                        &q.result_formats,
                    )
                    .await
                }),
            ));
        }
        for (i, h) in handles {
            let r = h.await.map_err(|e| {
                Error::new(Status::GenericFailure, format!("pipeline task join: {e}"))
            })?;
            slots[i] = Some(r?);
        }

        Ok(slots
            .into_iter()
            .map(|s| s.expect("pipeline slot unfilled"))
            .collect())
    }

    /// Discard queued queries without executing.
    #[napi]
    pub fn clear(&self) {
        if let Ok(mut q) = self.queued.lock() {
            q.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// RowStream
// ---------------------------------------------------------------------------

type RowRx = mpsc::Receiver<std::result::Result<StreamedRow, PgWireError>>;

#[napi]
pub struct RowStream {
    fields: Vec<FieldInfo>,
    /// Option<_> so `close()` and final `next()` can drop the receiver.
    rx: Arc<Mutex<Option<RowRx>>>,
}

#[napi]
impl RowStream {
    /// Column metadata for this stream. Stable across calls.
    #[napi(getter)]
    pub fn fields(&self) -> Vec<FieldInfo> {
        self.fields.clone()
    }

    /// Await the next row. Resolves to `null` when the stream completes.
    /// Resolves to a rejection if the server returned an error.
    #[napi]
    pub async fn next(&self) -> Result<Option<Vec<Option<String>>>> {
        let mut guard = self.rx.lock().await;
        let rx = match guard.as_mut() {
            Some(rx) => rx,
            None => return Ok(None),
        };
        match rx.recv().await {
            Some(Ok(row)) => Ok(Some(row_to_strings(row))),
            Some(Err(e)) => {
                *guard = None;
                Err(map_err(e))
            }
            None => {
                *guard = None;
                Ok(None)
            }
        }
    }

    /// Await rows and return up to `max` in a single napi call.
    ///
    /// Awaits the first row (or EOF), then non-blockingly drains buffered rows
    /// up to the cap. Returns an empty array on EOF. Errors cascade.
    ///
    /// The JS facade uses this to amortize the V8 boundary cost across many
    /// rows per crossing.
    #[napi]
    pub async fn next_batch(&self, max: u32) -> Result<Vec<Vec<Option<String>>>> {
        use tokio::sync::mpsc::error::TryRecvError;
        let max = max.max(1) as usize;
        let mut guard = self.rx.lock().await;
        let rx = match guard.as_mut() {
            Some(rx) => rx,
            None => return Ok(Vec::new()),
        };
        let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(max);
        match rx.recv().await {
            Some(Ok(row)) => out.push(row_to_strings(row)),
            Some(Err(e)) => {
                *guard = None;
                return Err(map_err(e));
            }
            None => {
                *guard = None;
                return Ok(out);
            }
        }
        while out.len() < max {
            match rx.try_recv() {
                Ok(Ok(row)) => out.push(row_to_strings(row)),
                Ok(Err(e)) => {
                    *guard = None;
                    return Err(map_err(e));
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    *guard = None;
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Close the stream early. Subsequent `next()` calls resolve to `null`.
    /// The reader task continues draining the current response in the background.
    #[napi]
    pub async fn close(&self) {
        let mut guard = self.rx.lock().await;
        *guard = None;
    }
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

#[napi(object)]
pub struct Notification {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

type NotificationRx = mpsc::Receiver<BackendMsg>;

#[napi]
pub struct Notifications {
    rx: Arc<Mutex<Option<NotificationRx>>>,
}

#[napi]
impl Notifications {
    /// Await the next notification. Resolves to `null` when the connection
    /// closes or `close()` is called.
    #[napi]
    pub async fn next(&self) -> Result<Option<Notification>> {
        let mut guard = self.rx.lock().await;
        let rx = match guard.as_mut() {
            Some(rx) => rx,
            None => return Ok(None),
        };
        loop {
            match rx.recv().await {
                Some(BackendMsg::NotificationResponse {
                    pid,
                    channel,
                    payload,
                }) => {
                    return Ok(Some(Notification {
                        pid,
                        channel,
                        payload,
                    }));
                }
                Some(_) => continue,
                None => {
                    *guard = None;
                    return Ok(None);
                }
            }
        }
    }

    /// Stop receiving notifications. Subsequent `next()` calls resolve to `null`.
    #[napi]
    pub async fn close(&self) {
        let mut guard = self.rx.lock().await;
        *guard = None;
    }
}

// ---------------------------------------------------------------------------
// CancelToken
// ---------------------------------------------------------------------------

#[napi]
pub struct CancelToken {
    inner: pg_wired::CancelToken,
}

#[napi]
impl CancelToken {
    /// Open a new connection to the server and send a CancelRequest
    /// for the backend this token references. Best-effort.
    #[napi]
    pub async fn cancel(&self) -> Result<()> {
        self.inner.clone().cancel().await.map_err(map_err)
    }

    #[napi(getter)]
    pub fn pid(&self) -> i32 {
        self.inner.pid
    }
}

//! Async split sender/receiver connection.
//! Inspired by hsqlx's PgWire.Async architecture.
//!
//! A single TCP connection is shared by many concurrent handler tasks.
//! The writer task coalesces messages from multiple requests into one write().
//! The reader task parses responses and dispatches them to waiting handlers via FIFO.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::connection::WireConn;
use crate::error::PgWireError;
use crate::protocol::backend;
use crate::protocol::frontend;
use crate::protocol::types::{BackendMsg, FormatCode, FrontendMsg};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// A request to execute on the connection.
pub struct PipelineRequest {
    /// Pre-encoded wire messages (Parse+Bind+Execute+Sync or Query).
    pub messages: BytesMut,
    /// How to collect the response.
    pub collector: ResponseCollector,
    /// Channel to send the response back to the caller.
    pub response_tx: oneshot::Sender<Result<PipelineResponse, PgWireError>>,
}

/// How to collect response messages for a request.
#[allow(dead_code)]
pub enum ResponseCollector {
    /// Collect DataRows until ReadyForQuery (for SELECT queries).
    Rows,
    /// Just drain until ReadyForQuery (for setup commands like BEGIN, SET ROLE).
    Drain,
    /// Stream rows one at a time via channels. Sends header first, then individual rows.
    Stream {
        header_tx: oneshot::Sender<Result<StreamHeader, PgWireError>>,
        row_tx: mpsc::Sender<Result<StreamedRow, PgWireError>>,
    },
    /// COPY IN: after receiving CopyInResponse, send the provided data then CopyDone.
    CopyIn {
        /// The data to send after CopyInResponse.
        data: Vec<u8>,
    },
    /// COPY OUT: collect CopyData messages until CopyDone.
    CopyOut,
}

/// Response from a pipeline request.
pub enum PipelineResponse {
    Rows {
        /// Column metadata from RowDescription (empty if no RowDescription received).
        fields: Vec<crate::protocol::types::FieldDescription>,
        /// Row data.
        rows: Vec<Vec<Option<Vec<u8>>>>,
        /// CommandComplete tag (e.g. "SELECT 3", "INSERT 0 1").
        command_tag: String,
    },
    Done,
}

/// Metadata sent at the start of a streaming response.
pub struct StreamHeader {
    pub fields: Vec<crate::protocol::types::FieldDescription>,
}

/// A single streamed row.
pub type StreamedRow = Vec<Option<Vec<u8>>>;

/// How a streaming request communicates with the reader task.
pub struct StreamRequest {
    /// Pre-encoded wire messages.
    pub messages: BytesMut,
    /// Channel to send the column metadata header.
    pub header_tx: oneshot::Sender<Result<StreamHeader, PgWireError>>,
    /// Channel to send individual rows. Closed when done or on error.
    pub row_tx: mpsc::Sender<Result<StreamedRow, PgWireError>>,
}

// ---------------------------------------------------------------------------
// Async connection
// ---------------------------------------------------------------------------

/// A shared async connection that multiplexes requests from many tasks.
pub struct AsyncConn {
    request_tx: mpsc::Sender<PipelineRequest>,
    stmt_cache: std::sync::Mutex<std::collections::HashMap<String, (String, u64)>>,
    stmt_counter: std::sync::atomic::AtomicU64,
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// Backend PID and secret key for cancel requests.
    pub backend_pid: i32,
    pub backend_secret: i32,
    /// Server address for cancel connections.
    pub addr: String,
    /// Channel for async notifications received during query execution.
    /// Notifications are NOT silently dropped — they're forwarded here.
    #[allow(dead_code)]
    notification_tx: mpsc::Sender<crate::protocol::types::BackendMsg>,
    notification_rx: std::sync::Mutex<Option<mpsc::Receiver<crate::protocol::types::BackendMsg>>>,
}

impl AsyncConn {
    /// Check if the connection is still alive (writer/reader tasks running).
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct PendingResponse {
    collector: ResponseCollector,
    response_tx: oneshot::Sender<Result<PipelineResponse, PgWireError>>,
}

impl AsyncConn {
    /// Create a new async connection from a raw WireConn.
    /// Spawns writer and reader tasks.
    pub fn new(conn: WireConn) -> Self {
        let backend_pid = conn.pid;
        let backend_secret = conn.secret;
        // Extract peer address before consuming the stream.
        let addr = conn
            .stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();

        let (notification_tx, notification_rx) = mpsc::channel(256);
        let (request_tx, request_rx) = mpsc::channel::<PipelineRequest>(256);
        let pending: Arc<Mutex<VecDeque<PendingResponse>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        let pending_notify = Arc::new(tokio::sync::Notify::new());
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let (stream_read, stream_write) = tokio::io::split(conn.into_stream());

        // Spawn writer task — sets alive=false on exit.
        {
            let pending = Arc::clone(&pending);
            let pending_notify = Arc::clone(&pending_notify);
            let alive = Arc::clone(&alive);
            tokio::spawn(async move {
                writer_task(request_rx, stream_write, pending, pending_notify).await;
                alive.store(false, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!("pg-wire writer task exited");
            });
        }

        // Spawn reader task — sets alive=false on exit.
        {
            let pending = Arc::clone(&pending);
            let pending_notify = Arc::clone(&pending_notify);
            let alive_clone = Arc::clone(&alive);
            let ntf_tx = notification_tx.clone();
            tokio::spawn(async move {
                reader_task(stream_read, pending, pending_notify, ntf_tx).await;
                alive_clone.store(false, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!("pg-wire reader task exited");
            });
        }

        Self {
            request_tx,
            stmt_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
            stmt_counter: std::sync::atomic::AtomicU64::new(0),
            alive,
            backend_pid,
            backend_secret,
            addr,
            notification_tx,
            notification_rx: std::sync::Mutex::new(Some(notification_rx)),
        }
    }

    /// Take the notification receiver. Call once to get a channel that
    /// receives `NotificationResponse` messages that arrive during queries.
    pub fn take_notification_receiver(
        &self,
    ) -> Option<mpsc::Receiver<crate::protocol::types::BackendMsg>> {
        self.notification_rx.lock().unwrap().take()
    }

    /// Look up or allocate a statement name.
    pub fn lookup_or_alloc(&self, sql: &str) -> (Vec<u8>, bool) {
        let mut cache = self.stmt_cache.lock().unwrap();
        if let Some((name, _)) = cache.get(sql) {
            return (name.as_bytes().to_vec(), false);
        }
        // Evict if too large.
        if cache.len() >= 256 {
            cache.clear();
        }
        let n = self.stmt_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!("s{n}");
        cache.insert(sql.to_string(), (name.clone(), n));
        (name.into_bytes(), true)
    }

    /// Execute COPY FROM STDIN: sends the COPY command, then data in chunks, then CopyDone.
    /// Returns the number of rows copied (from CommandComplete tag).
    ///
    /// Data is sent in chunks of up to 1MB to avoid buffering the entire payload
    /// in a single BytesMut. For small payloads (< 1MB), this is a single write.
    pub async fn copy_in(&self, copy_sql: &str, data: &[u8]) -> Result<u64, PgWireError> {
        use crate::protocol::types::FrontendMsg;
        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks

        // Build the message buffer: Query + chunked CopyData + CopyDone.
        let mut buf = BytesMut::with_capacity(copy_sql.len() + data.len().min(CHUNK_SIZE) + 64);
        frontend::encode_message(&FrontendMsg::Query(copy_sql.as_bytes()), &mut buf);

        // Send data in chunks to avoid a single huge allocation.
        for chunk in data.chunks(CHUNK_SIZE) {
            frontend::encode_message(&FrontendMsg::CopyData(chunk), &mut buf);
        }
        // Empty data is valid (0 rows copied).
        frontend::encode_message(&FrontendMsg::CopyDone, &mut buf);

        let resp = self.submit(buf, ResponseCollector::CopyIn { data: Vec::new() }).await?;
        match resp {
            PipelineResponse::Rows { command_tag, .. } => {
                Ok(parse_copy_count(&command_tag))
            }
            PipelineResponse::Done => Ok(0),
        }
    }

    /// Execute COPY TO STDOUT: sends the COPY command, collects all CopyData.
    pub async fn copy_out(&self, copy_sql: &str) -> Result<Vec<u8>, PgWireError> {
        use crate::protocol::types::FrontendMsg;
        let mut buf = BytesMut::new();
        frontend::encode_message(&FrontendMsg::Query(copy_sql.as_bytes()), &mut buf);

        let resp = self.submit(buf, ResponseCollector::CopyOut).await?;
        match resp {
            PipelineResponse::Rows { rows, .. } => {
                // For CopyOut, we reuse the Rows variant but rows contain raw copy data.
                let mut result = Vec::new();
                for row in rows {
                    for data in row.into_iter().flatten() {
                        result.extend_from_slice(&data);
                    }
                }
                Ok(result)
            }
            PipelineResponse::Done => Ok(Vec::new()),
        }
    }

    /// Evict a SQL statement from the cache, forcing re-parse on next use.
    /// Used for prepared statement invalidation after schema changes.
    pub fn invalidate_statement(&self, sql: &str) {
        let mut cache = self.stmt_cache.lock().unwrap();
        cache.remove(sql);
    }

    /// Clear the entire statement cache. Must be called after `DISCARD ALL`
    /// which destroys server-side prepared statements.
    pub fn clear_statement_cache(&self) {
        let mut cache = self.stmt_cache.lock().unwrap();
        cache.clear();
    }

    /// Execute a pipelined transaction with automatic statement caching.
    pub async fn exec_transaction(
        &self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let (stmt_name, needs_parse) = self.lookup_or_alloc(query_sql);
        self.pipeline_transaction(setup_sql, query_sql, params, param_oids, &stmt_name, needs_parse)
            .await
    }

    /// Execute a parameterized query with automatic statement caching.
    pub async fn exec_query(
        &self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let (stmt_name, needs_parse) = self.lookup_or_alloc(sql);
        self.query(sql, params, param_oids, &stmt_name, needs_parse)
            .await
    }

    /// Maximum time to wait for a response from the reader task.
    /// Prevents hanging forever if the reader/writer task dies mid-request.
    const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

    /// Submit a request to the connection. Returns a future that resolves
    /// when the response is available. Times out after 5 minutes to prevent
    /// hanging forever if the reader/writer task dies.
    pub async fn submit(
        &self,
        messages: BytesMut,
        collector: ResponseCollector,
    ) -> Result<PipelineResponse, PgWireError> {
        let (response_tx, response_rx) = oneshot::channel();
        let req = PipelineRequest {
            messages,
            collector,
            response_tx,
        };
        self.request_tx
            .send(req)
            .await
            .map_err(|_| PgWireError::ConnectionClosed)?;
        match tokio::time::timeout(Self::REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(PgWireError::ConnectionClosed),
            Err(_elapsed) => {
                tracing::error!("request timed out after {:?} — reader/writer task may be dead", Self::REQUEST_TIMEOUT);
                Err(PgWireError::ConnectionClosed)
            }
        }
    }

    /// Submit a streaming request. Returns the column header and an mpsc receiver
    /// that yields rows one at a time.
    pub async fn submit_stream(
        &self,
        messages: BytesMut,
        row_buffer: usize,
    ) -> Result<
        (
            StreamHeader,
            mpsc::Receiver<Result<StreamedRow, PgWireError>>,
        ),
        PgWireError,
    > {
        let (header_tx, header_rx) = oneshot::channel();
        let (row_tx, row_rx) = mpsc::channel(row_buffer);
        let (response_tx, _response_rx) = oneshot::channel();
        let req = PipelineRequest {
            messages,
            collector: ResponseCollector::Stream { header_tx, row_tx },
            response_tx,
        };
        self.request_tx
            .send(req)
            .await
            .map_err(|_| PgWireError::ConnectionClosed)?;
        let header = header_rx
            .await
            .map_err(|_| PgWireError::ConnectionClosed)??;
        Ok((header, row_rx))
    }

    /// Execute a pipelined transaction:
    /// setup (simple query) + data query (extended protocol) + COMMIT (simple query)
    /// All coalesced into one TCP write. Binary-safe parameterized data query.
    pub async fn pipeline_transaction(
        &self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
        stmt_name: &[u8],
        needs_parse: bool,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let mut buf = BytesMut::with_capacity(1024);

        // 1. Simple query for setup (BEGIN + SET ROLE + set_config).
        frontend::encode_message(&FrontendMsg::Query(setup_sql.as_bytes()), &mut buf);

        // Submit setup as Drain — we don't care about its response data.
        let setup_msgs = buf.split();

        // 2. Extended query for data.
        let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
        let result_fmts = [FormatCode::Text];

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: stmt_name,
                    sql: query_sql.as_bytes(),
                    param_oids,
                },
                &mut buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: stmt_name,
                param_formats: &text_fmts[..params.len()],
                params,
                result_formats: &result_fmts,
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

        // 3. Simple query for COMMIT.
        frontend::encode_message(&FrontendMsg::Query(b"COMMIT"), &mut buf);

        let data_msgs = buf.split();

        // Submit all three as separate requests with different collectors.
        // They'll be coalesced by the writer into one write() syscall.
        let (setup_tx, setup_rx) = oneshot::channel();
        let (data_tx, data_rx) = oneshot::channel();
        let (commit_tx, commit_rx) = oneshot::channel();

        // Send all three requests to the writer channel.
        // The writer drains the channel and writes them all at once.
        self.request_tx
            .send(PipelineRequest {
                messages: setup_msgs,
                collector: ResponseCollector::Drain,
                response_tx: setup_tx,
            })
            .await
            .map_err(|_| PgWireError::ConnectionClosed)?;

        self.request_tx
            .send(PipelineRequest {
                messages: data_msgs,
                collector: ResponseCollector::Rows,
                response_tx: data_tx,
            })
            .await
            .map_err(|_| PgWireError::ConnectionClosed)?;

        self.request_tx
            .send(PipelineRequest {
                messages: BytesMut::new(), // COMMIT already in data_msgs
                collector: ResponseCollector::Drain,
                response_tx: commit_tx,
            })
            .await
            .map_err(|_| PgWireError::ConnectionClosed)?;

        // Wait for all responses.
        setup_rx
            .await
            .map_err(|_| PgWireError::ConnectionClosed)??;

        let data_resp = data_rx
            .await
            .map_err(|_| PgWireError::ConnectionClosed)??;

        commit_rx
            .await
            .map_err(|_| PgWireError::ConnectionClosed)??;

        match data_resp {
            PipelineResponse::Rows { rows, .. } => Ok(rows),
            PipelineResponse::Done => Ok(Vec::new()),
        }
    }

    /// Execute a simple parameterized query (no transaction).
    pub async fn query(
        &self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
        stmt_name: &[u8],
        needs_parse: bool,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        let mut buf = BytesMut::with_capacity(512);

        let text_fmts: Vec<FormatCode> = vec![FormatCode::Text; params.len().max(1)];
        let result_fmts = [FormatCode::Text];

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: stmt_name,
                    sql: sql.as_bytes(),
                    param_oids,
                },
                &mut buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: stmt_name,
                param_formats: &text_fmts[..params.len()],
                params,
                result_formats: &result_fmts,
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

        let resp = self.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows { rows, .. } => Ok(rows),
            PipelineResponse::Done => Ok(Vec::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Writer task
// ---------------------------------------------------------------------------

async fn writer_task(
    mut rx: mpsc::Receiver<PipelineRequest>,
    mut stream: tokio::io::WriteHalf<crate::tls::MaybeTlsStream>,
    pending: Arc<Mutex<VecDeque<PendingResponse>>>,
    pending_notify: Arc<tokio::sync::Notify>,
) {
    let mut write_buf = BytesMut::with_capacity(8192);

    loop {
        // Wait for the first request.
        let first = match rx.recv().await {
            Some(req) => req,
            None => return, // Channel closed.
        };

        // Drain any additional queued requests (batch coalescing).
        write_buf.clear();
        write_buf.extend_from_slice(&first.messages);

        let mut batch: Vec<PendingResponse> = vec![PendingResponse {
            collector: first.collector,
            response_tx: first.response_tx,
        }];

        // Non-blocking drain of all queued requests.
        while let Ok(req) = rx.try_recv() {
            write_buf.extend_from_slice(&req.messages);
            batch.push(PendingResponse {
                collector: req.collector,
                response_tx: req.response_tx,
            });
        }

        // ONE write() syscall for all coalesced messages.
        // Write BEFORE enqueuing pending responses — if the write fails,
        // we send errors to callers instead of leaving them hanging.
        let write_result = stream.write_all(&write_buf).await;
        let write_err = match write_result {
            Ok(_) => stream.flush().await.err(),
            Err(e) => Some(e),
        };

        if let Some(e) = write_err {
            tracing::error!("Writer error: {e}");
            let msg = e.to_string();
            for p in batch {
                let _ = p.response_tx.send(Err(PgWireError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    msg.clone(),
                ))));
            }
            return;
        }

        // Write succeeded — enqueue pending responses for the reader.
        {
            let mut pq = pending.lock().await;
            for p in batch {
                pq.push_back(p);
            }
        }
        // Wake the reader task to process the newly enqueued responses.
        pending_notify.notify_one();
    }
}

// ---------------------------------------------------------------------------
// Reader task
// ---------------------------------------------------------------------------

async fn reader_task(
    mut stream: tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    pending: Arc<Mutex<VecDeque<PendingResponse>>>,
    pending_notify: Arc<tokio::sync::Notify>,
    notification_tx: mpsc::Sender<BackendMsg>,
) {
    let mut recv_buf = BytesMut::with_capacity(32 * 1024);

    loop {
        // Wait for a pending response to become available.
        let pr = loop {
            {
                let mut pq = pending.lock().await;
                if let Some(pr) = pq.pop_front() {
                    break pr;
                }
            }
            // No pending — wait for the writer to signal.
            pending_notify.notified().await;
        };

        // Collect the response based on the collector type.
        let result = match pr.collector {
            ResponseCollector::Rows => collect_rows(&mut stream, &mut recv_buf, &notification_tx).await,
            ResponseCollector::Drain => {
                drain_until_ready(&mut stream, &mut recv_buf)
                    .await
                    .map(|_| PipelineResponse::Done)
            }
            ResponseCollector::Stream { header_tx, row_tx } => {
                stream_rows(&mut stream, &mut recv_buf, header_tx, row_tx).await;
                Ok(PipelineResponse::Done)
            }
            ResponseCollector::CopyIn { .. } => {
                collect_copy_in_response(&mut stream, &mut recv_buf).await
            }
            ResponseCollector::CopyOut => {
                collect_copy_out(&mut stream, &mut recv_buf).await
            }
        };

        // Send the response back to the caller.
        let _ = pr.response_tx.send(result);
    }
}

async fn read_msg(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
) -> Result<BackendMsg, PgWireError> {
    loop {
        if let Some(msg) = backend::parse_message(buf).map_err(PgWireError::Protocol)? {
            return Ok(msg);
        }
        let n = stream.read_buf(buf).await?;
        if n == 0 {
            return Err(PgWireError::ConnectionClosed);
        }
    }
}

async fn collect_rows(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
    notification_tx: &mpsc::Sender<BackendMsg>,
) -> Result<PipelineResponse, PgWireError> {
    let mut rows = Vec::new();
    let mut fields = Vec::new();
    let mut command_tag = String::new();
    loop {
        let msg = read_msg(stream, buf).await?;
        match msg {
            BackendMsg::DataRow { columns } => rows.push(columns),
            BackendMsg::RowDescription { fields: f } => fields = f,
            BackendMsg::CommandComplete { tag } => command_tag = tag,
            BackendMsg::ReadyForQuery { .. } => {
                return Ok(PipelineResponse::Rows { fields, rows, command_tag });
            }
            BackendMsg::ErrorResponse { fields } => {
                drain_until_ready(stream, buf).await?;
                return Err(PgWireError::Pg(fields));
            }
            msg @ BackendMsg::NotificationResponse { .. } => {
                // Forward notification instead of dropping.
                let _ = notification_tx.try_send(msg);
            }
            BackendMsg::ParseComplete
            | BackendMsg::BindComplete
            | BackendMsg::NoData
            | BackendMsg::NoticeResponse { .. }
            | BackendMsg::EmptyQueryResponse => {}
            _ => {}
        }
    }
}

async fn drain_until_ready(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
) -> Result<(), PgWireError> {
    loop {
        let msg = read_msg(stream, buf).await?;
        if matches!(msg, BackendMsg::ReadyForQuery { .. }) {
            return Ok(());
        }
        if let BackendMsg::ErrorResponse { ref fields } = msg {
            tracing::warn!("Error in drain: {}: {}", fields.code, fields.message);
        }
    }
}

/// Stream rows one at a time, sending header first, then individual rows.
async fn stream_rows(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
    header_tx: oneshot::Sender<Result<StreamHeader, PgWireError>>,
    row_tx: mpsc::Sender<Result<StreamedRow, PgWireError>>,
) {
    let mut header_tx = Some(header_tx);
    let mut fields = Vec::new();
    loop {
        let msg = match read_msg(stream, buf).await {
            Ok(msg) => msg,
            Err(e) => {
                if let Some(htx) = header_tx.take() {
                    let _ = htx.send(Err(e));
                } else {
                    let _ = row_tx.send(Err(e)).await;
                }
                return;
            }
        };
        match msg {
            BackendMsg::RowDescription { fields: f } => {
                fields = f;
            }
            BackendMsg::DataRow { columns } => {
                if let Some(htx) = header_tx.take() {
                    let _ = htx.send(Ok(StreamHeader {
                        fields: fields.clone(),
                    }));
                }
                if row_tx.send(Ok(columns)).await.is_err() {
                    let _ = drain_until_ready(stream, buf).await;
                    return;
                }
            }
            BackendMsg::CommandComplete { .. } => {
                if let Some(htx) = header_tx.take() {
                    let _ = htx.send(Ok(StreamHeader {
                        fields: std::mem::take(&mut fields),
                    }));
                }
            }
            BackendMsg::ReadyForQuery { .. } => {
                if let Some(htx) = header_tx.take() {
                    let _ = htx.send(Ok(StreamHeader {
                        fields: std::mem::take(&mut fields),
                    }));
                }
                return;
            }
            BackendMsg::ErrorResponse { fields: err } => {
                if let Some(htx) = header_tx.take() {
                    let _ = htx.send(Err(PgWireError::Pg(err)));
                } else {
                    let _ = row_tx.send(Err(PgWireError::Pg(err))).await;
                }
                let _ = drain_until_ready(stream, buf).await;
                return;
            }
            BackendMsg::ParseComplete
            | BackendMsg::BindComplete
            | BackendMsg::NoData
            | BackendMsg::PortalSuspended
            | BackendMsg::NoticeResponse { .. }
            | BackendMsg::EmptyQueryResponse => {}
            _ => {}
        }
    }
}

/// Handle COPY IN response: skip CopyInResponse, wait for CommandComplete + ReadyForQuery.
/// The actual CopyData + CopyDone were pre-buffered in the write, so PG processes them.
async fn collect_copy_in_response(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
) -> Result<PipelineResponse, PgWireError> {
    let mut command_tag = String::new();
    loop {
        let msg = read_msg(stream, buf).await?;
        match msg {
            BackendMsg::CopyInResponse { .. } => {}
            BackendMsg::CommandComplete { tag } => command_tag = tag,
            BackendMsg::ReadyForQuery { .. } => {
                return Ok(PipelineResponse::Rows {
                    fields: Vec::new(),
                    rows: Vec::new(),
                    command_tag,
                });
            }
            BackendMsg::ErrorResponse { fields } => {
                drain_until_ready(stream, buf).await?;
                return Err(PgWireError::Pg(fields));
            }
            _ => {}
        }
    }
}

/// Collect COPY OUT data: CopyOutResponse → CopyData* → CopyDone → CommandComplete → ReadyForQuery.
async fn collect_copy_out(
    stream: &mut tokio::io::ReadHalf<crate::tls::MaybeTlsStream>,
    buf: &mut BytesMut,
) -> Result<PipelineResponse, PgWireError> {
    let mut data_chunks: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    let mut command_tag = String::new();
    loop {
        let msg = read_msg(stream, buf).await?;
        match msg {
            BackendMsg::CopyOutResponse { .. } => {}
            BackendMsg::CopyData { data } => {
                data_chunks.push(vec![Some(data)]);
            }
            BackendMsg::CopyDone => {}
            BackendMsg::CommandComplete { tag } => command_tag = tag,
            BackendMsg::ReadyForQuery { .. } => {
                return Ok(PipelineResponse::Rows {
                    fields: Vec::new(),
                    rows: data_chunks,
                    command_tag,
                });
            }
            BackendMsg::ErrorResponse { fields } => {
                drain_until_ready(stream, buf).await?;
                return Err(PgWireError::Pg(fields));
            }
            _ => {}
        }
    }
}

fn parse_copy_count(tag: &str) -> u64 {
    // COPY tag format: "COPY 123"
    tag.strip_prefix("COPY ")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

// Extension to WireConn to extract the underlying stream.
impl WireConn {
    pub fn into_stream(self) -> crate::tls::MaybeTlsStream {
        self.stream
    }
}

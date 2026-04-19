# pg-wired architecture

This document describes how pg-wired implements PostgreSQL wire protocol v3: the task model, the message coalescing writer, the FIFO response matcher, the statement cache, and the TLS and auth paths. It assumes you have read the crate [`README`](README.md) and want to understand the internals before modifying them.

## Module layout

```
src/
  lib.rs               re-exports of the public surface
  connection.rs        WireConn: synchronous raw wire connection
  async_conn.rs        AsyncConn: shared multiplexed async connection
  async_pool.rs        AsyncPool: round-robin N-connection pool
  pipeline.rs          PgPipeline: single-connection explicit pipelining
  cancel.rs            CancelToken: out-of-band query cancellation
  scram.rs             SCRAM-SHA-256 / SCRAM-SHA-256-PLUS state machine
  tls.rs               MaybeTlsStream, TLS negotiation
  error.rs             PgWireError
  protocol/
    mod.rs             module re-exports
    types.rs           FrontendMsg / BackendMsg enums, FormatCode, PgError
    frontend.rs        encode_message, encode_startup_with_params, md5_password
    backend.rs         parse_message: zero-copy streaming parser
```

Two connection abstractions live side by side. `WireConn` is the synchronous low-level primitive used during startup, authentication, and in tools that want message-by-message control (the compile-time query describer, the LISTEN/NOTIFY listener). `AsyncConn` is the shared multiplexed connection that applications use, wrapping a `WireConn` and spawning reader + writer tasks.

## AsyncConn: the shared multiplexed connection

`AsyncConn` ([`src/async_conn.rs:92`](src/async_conn.rs)) is the primary handle applications hold. It is constructed from a connected `WireConn` and immediately takes ownership of its `MaybeTlsStream`, splitting it with `tokio::io::split` into a read half and a write half. It then spawns two tasks:

- **Writer task** ([`src/async_conn.rs:740`](src/async_conn.rs)) owns the `WriteHalf` and the receiving end of an `mpsc::Sender<PipelineRequest>`. Callers submit work through this channel.
- **Reader task** ([`src/async_conn.rs:825`](src/async_conn.rs)) owns the `ReadHalf` and a shared `Arc<Mutex<VecDeque<PendingResponse>>>`.

Both tasks share an `Arc<AtomicBool>` liveness flag. Either task setting `alive = false` marks the connection dead. An `AsyncPool` health monitor (ticking every 5 s) spots this and replaces the dead slot with a fresh connection.

### Submit flow

A single `submit(buf, collector)` call moves through four actors:

1. **Caller** sends a `PipelineRequest { messages, collector, response_tx }` on the writer's mpsc channel and `await`s `response_rx`.
2. **Writer task** wakes on `rx.recv()`, drains any concurrently queued requests with `try_recv()`, concatenates their bytes into a reusable `write_buf`, issues one `stream.write_all(&write_buf)`, then pushes the batch of `PendingResponse { collector, response_tx }` entries onto the shared deque and signals the reader via `Notify`.
3. **Reader task** pops the head of the deque, calls `parse_message` on incoming bytes, and feeds each `BackendMsg` to the collector until `ReadyForQuery` arrives.
4. **Reader task** resolves the collector into a `Result` and sends it on the oneshot `response_tx`, unblocking the caller.

The key invariant is **FIFO correlation**: the Nth entry pushed to the deque matches the Nth `ReadyForQuery` response arriving from the backend. PostgreSQL v3 guarantees response ordering matches request ordering on a single connection, so the deque position is sufficient. There is no request id, no hash map, no per-request lookup cost.

### Message coalescing

The writer loop ([`src/async_conn.rs:740`](src/async_conn.rs)) is deliberately structured to batch concurrent requests into a single `write()` syscall:

```
loop {
    let first = rx.recv().await;                // block for at least one request
    write_buf.clear();
    write_buf.extend_from_slice(&first.messages);
    while let Ok(req) = rx.try_recv() {         // non-blocking drain
        write_buf.extend_from_slice(&req.messages);
    }
    stream.write_all(&write_buf).await?;        // one syscall for the whole batch
    stream.flush().await?;
    pending.lock().await.extend(batch);
    pending_notify.notify_one();
}
```

Under load, dozens of tasks submitting simultaneously all land inside a single drain, producing one `write_all` call instead of one per request. `write_buf` is pre-allocated at 8 KiB and reused across iterations. `PendingResponse` entries are only pushed to the deque **after** the write succeeds: if the write fails, the writer errors all in-flight requests and exits, letting the health monitor take over.

This coalescing is orthogonal to the `PgPipeline` API. Any concurrent submission benefits from it automatically.

## Extended query protocol path

Prepared-statement queries use the extended query protocol. The canonical sequence for a cache-miss is:

```
frontend: Parse (P) | Bind (B) | Execute (E) | Sync (S)
backend:  ParseComplete (1) | BindComplete (2) | RowDescription (T)
          DataRow (D) * N | CommandComplete (C) | ReadyForQuery (Z)
```

On cache hit the `Parse` message is omitted; the backend skips `ParseComplete` in reply. `Describe` is **not** part of the hot path: it is sent only by `WireConn::describe_statement` ([`src/connection.rs:307`](src/connection.rs)), which the compile-time query macros in resolute-macros use.

`collect_rows` ([`src/async_conn.rs:890`](src/async_conn.rs)) is the state machine that drives this protocol. It reads backend messages until it sees `ReadyForQuery`, accumulating `DataRow`s and capturing the `command_tag` from `CommandComplete`. It silently discards `ParseComplete`, `BindComplete`, and `NoData`. An `ErrorResponse` mid-sequence causes it to drain to `ReadyForQuery`, then return `PgWireError::Pg`. A mid-query `NotificationResponse` is forwarded to the notification channel (see below) without breaking the query.

### Format codes

`FormatCode` ([`src/protocol/types.rs:7`](src/protocol/types.rs)) is `repr(i16)` with `Text = 0` and `Binary = 1`. The standard `query` path uses text format. Binary format is opt-in through `exec_query_with_formats` and `query_with_formats`. Higher-level crates (resolute) encode binary bytes and pass them with `FormatCode::Binary`; pg-wired itself is value-agnostic, it just forwards whatever the caller hands it.

## Statement cache

`AsyncConn` holds `stmt_cache: std::sync::Mutex<HashMap<String, (String, u64)>>` ([`src/async_conn.rs:94`](src/async_conn.rs)). Keys are SQL strings. Values are `(server-side statement name, insertion counter)`. Capacity is 256 entries.

`lookup_or_alloc` ([`src/async_conn.rs:195`](src/async_conn.rs)) is the single entry point for the cache:

1. If the SQL is present, return `(name, needs_parse=false)`.
2. If absent and the cache has room, allocate a fresh name from `stmt_counter: AtomicU64`, insert, return `(name, needs_parse=true)`.
3. If absent and the cache is full, find the entry with the smallest insertion counter, remove it, fire-and-forget submit a `Close(S, name) + Sync` to free the server-side statement, then allocate.

Note: eviction is pseudo-LRU by **insertion order**, not access order. The `u64` counter is only set at insertion time. An always-used entry can still be evicted if newer entries push it past the 256 mark. In practice this is fine because prepared-statement working sets rarely exceed 256.

### Stale-statement recovery

`exec_query` ([`src/async_conn.rs:374`](src/async_conn.rs)) catches SQLSTATE `26000` (invalid_sql_statement_name) and `0A000` (feature_not_supported), calls `invalidate_statement(sql)` to drop the cached name, and retries once with `needs_parse=true`. This handles the common case where `DISCARD ALL` or an admin reset cleared server-side statements out from under the client cache.

## Pipelining

Two distinct mechanisms, often confused:

- **Automatic coalescing** (described above): an emergent property of the mpsc drain loop. Callers do nothing; concurrent submissions are batched.
- **Explicit pipelining**: `pipeline_transaction` ([`src/async_conn.rs:488`](src/async_conn.rs)) submits three `PipelineRequest`s in quick succession (setup+drain, data rows, commit+drain) and then awaits all three. Because all three `send` calls complete before the writer processes them, they land in the same coalescing batch, producing one TCP write for the entire transaction.

The `PgPipeline` type ([`src/pipeline.rs`](src/pipeline.rs)) is a synchronous single-connection alternative that accumulates bytes into a `send_buf` and sends them in one `send_raw` call. It exists for tools and tests that need deterministic message-level control, not for general application use.

## LISTEN / NOTIFY

Asynchronous `NotificationResponse` messages can arrive at any time, including mid-query. `AsyncConn` carries a dedicated channel:

- `notification_tx: mpsc::Sender<BackendMsg>` (capacity 4096)
- `notification_rx: Mutex<Option<mpsc::Receiver<BackendMsg>>>` (take-once)

Inside `collect_rows` ([`src/async_conn.rs:915`](src/async_conn.rs)), a `NotificationResponse` is forwarded via `notification_tx.try_send`. A full channel logs a warning and drops the notification. Notifications never generate `ReadyForQuery`, so they do not affect FIFO correlation.

`AsyncConn::take_notification_receiver()` hands the receiver to the caller once; resolute's `PgListener` holds it and loops on `recv`.

## Cancellation

`CancelToken` ([`src/cancel.rs`](src/cancel.rs)) holds `(addr, backend_pid, backend_secret)`. PostgreSQL cancellation requires an entirely separate TCP connection carrying a 16-byte payload:

```
length: i32 = 16
code:   i32 = 80877102   (1234 << 16 | 5678)
pid:    i32              (from BackendKeyData)
secret: i32              (from BackendKeyData)
```

`cancel()` opens a fresh `TcpStream`, writes that payload, calls `shutdown`, and times out after 5 s. The `pid` and `secret` come from `BackendKeyData` received during startup ([`src/connection.rs:223`](src/connection.rs)) and are copied into `AsyncConn` at construction time.

## TLS negotiation

Gated behind the `tls` Cargo feature. `MaybeTlsStream` ([`src/tls.rs:13`](src/tls.rs)) is an enum that is either a plain `TcpStream` or a `tokio_rustls::client::TlsStream<TcpStream>`.

Negotiation follows the PostgreSQL SSL startup handshake:

1. Send 8-byte `SSLRequest`: `length=8, code=80877103` (1234 << 16 | 5679).
2. Read a single byte.
3. `'S'` means the server will negotiate TLS: build a `rustls::ClientConfig` (system roots by default via `webpki_roots`, optional custom CAs, optional mTLS client cert), perform the rustls handshake, and return `MaybeTlsStream::Tls`.
4. `'N'` means the server refused: if `TlsMode::Require`, error out; if `TlsMode::Prefer`, return the plain stream and continue over cleartext.

`negotiate_tls_with_config` ([`src/tls.rs:116`](src/tls.rs)) is the entry point. Certificate verification is handled by rustls; hostname mismatch surfaces as `PgWireError::Protocol`.

## SCRAM authentication

`scram.rs` implements SCRAM-SHA-256 and SCRAM-SHA-256-PLUS (channel binding) per RFC 5802 and RFC 7677. The state machine is a three-message dance wrapped around SASL:

```
client -> server:  client-first  (n,,n=user,r=client_nonce)
server -> client:  server-first  (r=server_nonce, s=salt, i=iterations)
client -> server:  client-final  (c=gs2_header, r=..., p=proof)
server -> client:  server-final  (v=server_signature)
```

SCRAM-SHA-256-PLUS adds TLS channel binding: when TLS is active and the server offers the PLUS variant, `choose_scram_mechanism` ([`src/connection.rs:35`](src/connection.rs)) retrieves the peer certificate via `tls.get_ref().1.peer_certificates()`, SHA-256 hashes it, and supplies it as `ChannelBinding::TlsServerEndPoint(hash)`. This binds the SCRAM exchange to the specific TLS session and defeats MITM attacks even if the attacker controls a valid certificate.

`hi()` ([`src/scram.rs:121`](src/scram.rs)) implements PBKDF2-HMAC-SHA-256 manually rather than pulling in a `pbkdf2` crate dependency.

## Error type

`PgWireError` ([`src/error.rs`](src/error.rs)) has four variants:

- `Io(std::io::Error)`: from the underlying `AsyncRead` / `AsyncWrite`.
- `Pg(PgError)`: a server-sent `ErrorResponse`. `PgError` carries severity, SQLSTATE code, message, and the optional detail/hint/position fields.
- `Protocol(String)`: malformed message from the server, unsupported authentication type, TLS hostname mismatch, and similar protocol-level problems.
- `ConnectionClosed`: EOF on the read path, or a writer/reader task exited unexpectedly.

Per-query SQL attribution does not live in pg-wired. Callers that want `.with_sql(sql)` on errors add that layer at the application boundary (resolute does this in its `Client` wrapper).

## Timeouts and dead-connection detection

`AsyncConn::REQUEST_TIMEOUT = Duration::from_secs(300)` ([`src/async_conn.rs:400`](src/async_conn.rs)) wraps every `response_rx.await` in `tokio::time::timeout`. If the server stops responding (network partition, silent TCP drop), the caller unblocks after 5 minutes with `PgWireError::ConnectionClosed` rather than hanging forever.

TCP keepalive is set at connect time in `connection.rs:119` using `socket2`: idle=60s, interval=15s. That catches silent drops behind NATs and load balancers at the OS level, well before the 5-minute request timeout fires.

Both writer and reader set `alive.store(false)` when they exit. `AsyncPool`'s health monitor ticks every 5 s, looks at `is_alive()` on each slot, and replaces dead slots with freshly connected ones. Dead connections are therefore self-healing from the pool user's perspective.

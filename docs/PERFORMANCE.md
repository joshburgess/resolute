# Performance

This document explains where resolute's performance wins come from: the binary encode fast path, statement caching, TCP write coalescing, FIFO response matching, and the lock-free atomics used pervasively through the stack. It also describes the benchmark setup so the numbers in the top-level README are reproducible.

If you want a higher-level view of the architecture, start with the per-crate `ARCHITECTURE.md` files:

- [`pg-wired/ARCHITECTURE.md`](../pg-wired/ARCHITECTURE.md)
- [`pg-pool/ARCHITECTURE.md`](../pg-pool/ARCHITECTURE.md)
- [`resolute/ARCHITECTURE.md`](../resolute/ARCHITECTURE.md)
- [`resolute-macros/ARCHITECTURE.md`](../resolute-macros/ARCHITECTURE.md)

## Benchmarks

Two Criterion bench files live in [`resolute/benches/`](../resolute/benches/):

- **`encode_decode.rs`**. Pure in-process encode / decode throughput. No network. Six types covered (`i32`, `i64`, `String`, `uuid::Uuid`, `DateTime<Utc>`, `serde_json::Value`) plus the named-param rewriter.
- **`query_latency.rs`**. End-to-end round-trips against a real PostgreSQL instance. Six scenarios: `SELECT 1`, parameterized `SELECT $1::int4`, 3-column / 3-param select, 100-row `generate_series`, INSERT+DELETE, and a warm-cache reuse loop.

Both use `criterion_group!` / `criterion_main!`. Run with:

```bash
cargo bench -p resolute
```

The latency benches expect PostgreSQL listening on port 54322 (the default in the project's docker-compose). Results land in `target/criterion/` as HTML reports.

### Methodology note

The encode benches are symmetric between resolute and sqlx for message handling but asymmetric for buffer reuse: resolute pre-allocates a `BytesMut::with_capacity(N)` and calls `buf.clear()` per iteration, reusing the capacity. sqlx allocates a fresh `PgArgumentBuffer::default()` per iteration. This asymmetry reflects the realistic production path on both sides:

- resolute's writer task owns a single long-lived `BytesMut` (see `async_conn.rs:746`) and never allocates on hot calls.
- sqlx's `PgArguments` is built per query and re-allocated each time.

The latency benches use a single Tokio current-thread runtime, a single resolute `Client`, and a sqlx `PgPool` with `max_connections(1)`. The sqlx cache-hit bench warms the pool with one prior call before measuring, matching the state resolute reaches on its second call.

### Reported numbers

The numbers in the top-level README (`encode i32: 3.3 ns vs sqlx 14 ns`, `SELECT 1: 78 µs vs 189 µs`, etc.) come from these two bench files, measured on an Apple M4 Max with `postgres:17-alpine` running locally over a Docker socket. They are not stored as artifacts in the repo: re-run `cargo bench -p resolute` on your hardware to regenerate. Exact ratios will shift with hardware, network stack, and PostgreSQL version, but the relative advantages of each mechanism described below are structural.

## Why the encode path is fast

The `Encode` trait in [`resolute/src/encode.rs`](../resolute/src/encode.rs) writes directly into a caller-supplied `&mut BytesMut` using `bytes::BufMut`. A typical impl:

```rust
impl Encode for i32 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int4 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(*self);  // single big-endian write into existing capacity
    }
}
```

`put_i32` does not allocate when capacity exists. `put_i64`, `put_f32`, and `put_f64` are the same one-instruction writes. UUID is `buf.put_slice(self.as_bytes())`, copying the 16-byte array directly. Strings are `buf.put_slice(self.as_bytes())` with no UTF-8 validation or re-encoding because Rust's `String` is already valid UTF-8.

The wrapper that produces a complete `Bind` parameter, `encode_param`, reserves a 4-byte length placeholder, calls the encode closure, and patches the length in place by slicing into the buffer:

```rust
let start = buf.len();
buf.put_i32(0);                                  // placeholder
value.encode(buf);
let len = (buf.len() - start - 4) as i32;
buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
```

No intermediate `Vec<u8>`, no format-string machinery, no runtime type lookup. The hot-path encode is just big-endian writes into a reused buffer.

## Statement cache: what changes on hit vs miss

The cache lives on `AsyncConn` ([`pg-wired/src/async_conn.rs:94`](../pg-wired/src/async_conn.rs)) as `std::sync::Mutex<HashMap<String, (String, u64)>>`, capped at 256 entries with pseudo-LRU eviction (by insertion order). Each cached entry maps a SQL string to a server-side prepared statement name.

**Cache miss** sends 4 frontend messages: `Parse`, `Bind`, `Execute`, `Sync`. The backend replies with `ParseComplete`, `BindComplete`, `DataRow*`, `CommandComplete`, `ReadyForQuery`. The server must parse, analyze, plan, and cache the statement before it can bind and execute.

**Cache hit** sends 3 frontend messages: `Bind`, `Execute`, `Sync`. The backend skips `ParseComplete`, replying with `BindComplete`, `DataRow*`, `CommandComplete`, `ReadyForQuery`. The server reuses the cached plan: no parse, no analyze, no planner invocation.

For small queries the planner cost is a meaningful fraction of query latency. For repeated queries (the common case for application workloads), the warm-cache path is dominant and the per-call cost drops significantly below the first-call cost.

### Stale statement recovery

`exec_query` ([`pg-wired/src/async_conn.rs:374`](../pg-wired/src/async_conn.rs)) catches SQLSTATE `26000` (invalid_sql_statement_name) and `0A000` (feature_not_supported), invalidates the cache entry, and retries once with `Parse` included. This handles the case where the server dropped the cached statement (typically via `DISCARD ALL` issued by pooling logic), without surfacing the transient error to the caller.

## TCP write coalescing

The writer task in [`pg-wired/src/async_conn.rs:740`](../pg-wired/src/async_conn.rs) is designed to produce **one `write()` syscall per batch of concurrent submissions**:

```rust
loop {
    let first = rx.recv().await;                // block for the first request
    write_buf.clear();
    write_buf.extend_from_slice(&first.messages);
    while let Ok(req) = rx.try_recv() {         // non-blocking drain
        write_buf.extend_from_slice(&req.messages);
    }
    stream.write_all(&write_buf).await?;        // single syscall
    stream.flush().await?;
    pending.lock().await.extend(batch);
    pending_notify.notify_one();
}
```

Under contention this matters. If 20 tasks submit queries within the same scheduling interval:

- Without coalescing: 20 `write()` syscalls, 20 TCP frames, 20 round trips through kernel queues.
- With coalescing: 1 `write()` syscall, probably 1 TCP frame (or a small number if the batch exceeds MTU), 1 trip through the writeback machinery.

This optimization is invisible to callers. No API change, no batching buffer in user code: it emerges from the mpsc drain loop. Coalescing is orthogonal to explicit pipelining (`pipeline_transaction`), which is a way for a single caller to enqueue several messages before any await point so they all land in the same batch.

`write_buf` is pre-allocated at 8 KiB and reused across iterations. Exceeding 8 KiB grows the capacity; it never shrinks back, so steady-state memory use is proportional to peak batch size.

## FIFO response matching: no per-request hash

PostgreSQL wire protocol v3 guarantees responses arrive in the same order as requests on a given connection. resolute exploits this directly:

- The writer pushes `PendingResponse { collector, response_tx }` entries to the back of an `Arc<Mutex<VecDeque<PendingResponse>>>`.
- The reader pops from the front when a `ReadyForQuery` completes a response sequence.

No request id, no correlation map, no hash lookup. The Nth entry in the deque matches the Nth response off the wire, period. This is O(1) per request with one pointer write on the producer side and one pointer move on the consumer side. It also means the code is dead simple: every entry is a direct pointer to the oneshot channel that will wake the caller.

The tradeoff is that a single stuck query (say, `pg_sleep(600)`) blocks the head of the deque for subsequent queries on the same connection. This is a property of PostgreSQL itself, not of resolute: the server processes queries on a connection sequentially. The pool (multiple connections) is the answer for parallelism, not reordering on one connection.

## Lock-free paths in the pool

pg-pool uses atomics wherever a lock is not strictly necessary ([`pg-pool/src/pool.rs:221`](../pg-pool/src/pool.rs)):

| Atomic | Purpose |
|---|---|
| `total_count: AtomicUsize` | Live connections, for capacity gating |
| `in_use_count: AtomicUsize` | Checked-out count |
| `waiter_count: AtomicUsize` | Pending waiters |
| `total_checkouts: AtomicU64` | Cumulative metrics |
| `total_created: AtomicU64` | Cumulative metrics |
| `total_destroyed: AtomicU64` | Cumulative metrics |
| `total_timeouts: AtomicU64` | Cumulative metrics |
| `draining: AtomicBool` | Shutdown gate |

Metrics snapshot reads all of them with `Acquire` / `Relaxed` loads without touching any mutex. The capacity gate (`total_count < max_size` check) is also lock-free: a racy `fetch_add` followed by rollback-on-failure is cheaper and simpler than a compare-and-swap loop under high contention.

Only two async mutexes are held: one around the idle `VecDeque<IdleConn>` and one around the waiter `VecDeque<Waiter>`. These are critical sections measured in nanoseconds, not held during IO.

## Pool round-robin dispatch

`AsyncPool::get` ([`pg-wired/src/async_pool.rs:96`](../pg-wired/src/async_pool.rs)) uses a single `counter.fetch_add(1, Relaxed) % len` to pick a connection slot:

```rust
let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.slots.len();
```

Relaxed ordering is fine because the counter is only used for load balancing, not synchronization. No hashing, no free-list, no mutex: pure monotonic counter modulo length. Hot paths hit a single atomic increment.

## Presizing and allocation discipline

Buffers are pre-sized based on measured working-set patterns:

| Buffer | Size | File |
|---|---|---|
| Writer send buffer | 8 KiB (grows on demand) | `pg-wired/src/async_conn.rs:746` |
| Reader receive buffer | 32 KiB | `pg-wired/src/async_conn.rs:831` |
| WireConn raw receive | 32 KiB | `pg-wired/src/connection.rs:23` |
| PgPipeline send buffer | 4 KiB | `pg-wired/src/pipeline.rs:28` |
| Per-query encode buffer | 512 bytes | `pg-wired/src/async_conn.rs:634` |
| Idle slot deque | pre-sized to `max_size` | `pg-pool/src/pool.rs` |

`parse_data_row` ([`pg-wired/src/protocol/backend.rs`](../pg-wired/src/protocol/backend.rs)) is the hottest path on the decode side. Its comment ("the hot path, bounds checks are explicit for safety while keeping allocations minimal") accompanies a `Vec::with_capacity(num_cols)` that precludes column-by-column reallocations. The same pattern repeats in `parse_row_description` and `parse_parameter_description`.

There is no custom global allocator. No `SmallVec`. No `#[global_allocator]` override. The discipline is entirely at the level of capacity hints and buffer reuse, which is portable across allocator choices.

## Structural comparison to sqlx

The performance gap between resolute and sqlx is not a single optimization; it is the cumulative effect of several choices:

- **Binary format by default vs text format by default.** sqlx encodes most parameters as text, which requires at least one allocation and often decimal formatting for numbers. resolute writes big-endian bytes into a reused buffer.
- **Owning the wire layer vs tokio-postgres.** Layer-through-layer abstractions in sqlx require trait dispatch and Box allocations that resolute elides by being the wire layer.
- **TCP write coalescing.** sqlx has one `write` per query in the default configuration. resolute bundles contemporaneous queries into one syscall.
- **FIFO deque vs correlation map.** resolute's response matching is O(1) with no hash; sqlx must map responses to the correct future through its connection driver.
- **No async-trait box per call.** resolute's `Executor` uses RPIT (`impl Future`), not `Box<dyn Future>`, so generic calls do not allocate a boxed future per invocation.

Each of these is modest on its own. They compound, and the compounding is most visible on the very small queries where setup cost dominates (encode benchmarks) and on high-concurrency workloads where coalescing pays off (query latency under load).

## What is intentionally not optimized

- **Parsing text format.** resolute supports text decode for compatibility (e.g., `simple_query`), but it is not on the hot path and is not tuned.
- **Large result set buffering.** Default `query` collects all rows into a `Vec<Row>`. For large scans, use `query_stream` (buffer-limited) or `COPY OUT`.
- **Reconnect aggressiveness.** `ReconnectingClient` reconnects on IO errors but does not preemptively ping idle connections. Use `test_on_checkout = true` on the pool or an application-level health check if you need aggressive detection.

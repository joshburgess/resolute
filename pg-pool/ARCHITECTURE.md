# pg-pool architecture

pg-pool is a generic async connection pool. It does not know about PostgreSQL: any type that implements [`Poolable`](src/pool.rs) can be pooled. This document covers the internals: the trait, the data layout, the acquire and release paths, lifecycle hooks, and the drain protocol. Read the [`README`](README.md) first for the user-facing API.

## Module layout

```
src/
  lib.rs         re-exports, feature gating
  pool.rs        the entire pool engine (772 lines)
  wire.rs        WirePoolable: pools pg_wired::WireConn        (feature = "wire")
  async_wire.rs  AsyncPoolable: pools pg_wired::AsyncConn     (feature = "wire")
```

The pool engine is deliberately kept in a single file. The `wire` feature provides two prebuilt `Poolable` implementations for pg-wired so downstream crates (resolute) do not have to write adapters.

## The Poolable trait

```rust
// src/pool.rs:15
pub trait Poolable: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn connect(
        addr: &str, user: &str, password: &str, database: &str,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn has_pending_data(&self) -> bool;

    fn reset(&self) -> impl Future<Output = bool> + Send {
        async { true }
    }
}
```

Three hooks, all async except `has_pending_data`:

- `connect` is the factory. The pool calls it when a new connection is needed.
- `has_pending_data` is a fast, synchronous health predicate. Returning `true` means the connection is in a bad state (unread bytes, dropped connection) and must be destroyed rather than reused.
- `reset` is optional cleanup run on checkin. The default is a no-op. `AsyncPoolable` overrides it to send `DISCARD ALL` and clear the statement cache ([`src/async_wire.rs:37`](src/async_wire.rs)). A return of `false` signals reset failure and triggers destruction.

The trait is intentionally narrow: there is no `close` or `disconnect`. When the pool decides to destroy a connection it simply drops the value and lets the connection's own `Drop` impl handle cleanup.

## Core data structures

```rust
// src/pool.rs:216
pub struct ConnPool<C: Poolable> {
    config: ConnPoolConfig,
    hooks: LifecycleHooks<C>,
    idle: Mutex<VecDeque<IdleConn<C>>>,     // tokio::sync::Mutex
    waiters: Mutex<VecDeque<Waiter<C>>>,     // tokio::sync::Mutex
    total_count: AtomicUsize,
    in_use_count: AtomicUsize,
    waiter_count: AtomicUsize,
    total_checkouts: AtomicU64,
    total_created: AtomicU64,
    total_destroyed: AtomicU64,
    total_timeouts: AtomicU64,
    draining: AtomicBool,
    drain_complete: Notify,
    shutdown_tx: mpsc::Sender<()>,
}
```

Two async mutexes guard the only two shared collections. Everything else is atomic, so metrics reads and the capacity gate never take a lock.

- `idle` holds `IdleConn<C> { conn, expires_at: Instant }`. Expiry is checked on both eviction (in the maintenance task) and checkout (in `try_get_idle`).
- `waiters` holds `Waiter<C> { tx: oneshot::Sender<C> }`. FIFO queue backed by `VecDeque`.
- `total_count` is the authoritative count of live connections (idle + in use). Incremented with `fetch_add(1, Release)` before a new connection is created, rolled back on failure.
- `in_use_count` is the checked-out count. `total_count - in_use_count` is roughly `idle.len()`, used in metrics with `saturating_sub` to tolerate brief inconsistency.

The entire pool is always wrapped in `Arc<ConnPool<C>>`.

## Acquire path (checkout)

`ConnPool::get` ([`src/pool.rs:279`](src/pool.rs)) walks four phases:

1. **Drain check** (line 280). `draining.load(Acquire)` is read first. A draining pool rejects new checkouts with `PoolError::Draining`.
2. **`before_acquire` hook** (line 284). Fires before any contention, giving users a place to hook tracing or admission control.
3. **Fast path: an idle connection exists** (line 361, `try_get_idle`). Lock `idle`, `pop_front`. While popping, eagerly discard expired entries (`Instant::now() >= expires_at`) and optionally dirty entries (`has_pending_data()` when `test_on_checkout = true`). Each eviction fires `on_destroy`. First clean entry is returned.
4. **Slow path: create or wait**. If `total_count < max_size`, call `create_and_track` (line 403) which does `fetch_add(1, Release)` then attempts `C::connect()`. If `connect` fails or another task raced past and the count ended up over `max_size`, the increment is rolled back. Otherwise `on_create` fires and the new connection is returned. If `total_count == max_size`, enqueue a waiter (step below).

### Waiter enqueue

```rust
// src/pool.rs:319
let (tx, rx) = oneshot::channel();
waiters.lock().await.push_back(Waiter { tx });
waiter_count.fetch_add(1, Release);
drop(waiters);
match tokio::time::timeout(config.checkout_timeout, rx).await {
    Ok(Ok(conn))   => { waiter_count.fetch_sub(1, Release); /* ... */ }
    Ok(Err(_))     => PoolError::Closed,           // pool dropped the sender
    Err(_)         => {                             // timeout
        total_timeouts.fetch_add(1, Relaxed);
        // eagerly prune dead senders so memory does not accumulate
        waiters.lock().await.retain(|w| !w.tx.is_closed());
        PoolError::Timeout
    }
}
```

The FIFO property falls out of `push_back` + `pop_front`. A timed-out waiter's `rx` is dropped, closing the corresponding `tx` in the queue; the `retain` call eagerly reclaims that memory. On the release path, the sender-closed state is also checked with `tx.is_closed()`, so a dropped waiter is skipped rather than receiving a connection it no longer wants.

## Release path (checkin)

Triggered by `PoolGuard::drop` ([`src/pool.rs:418`](src/pool.rs)), which spawns `return_conn_async` (line 424):

```
1. in_use_count.fetch_sub(1, Release)        // decremented FIRST
2. if has_pending_data()  -> destroy
3. if !reset().await      -> destroy
4. on_checkin hook (with &C)
5. if draining            -> destroy
6. pop waiter, try tx.send(conn); on Err(conn) skip and try next
7. no waiters left: push IdleConn with fresh expires_at
8. after_release hook
```

The `in_use_count` is decremented **before** `reset()` runs. This is the fix for a counter-leak bug where a panicking `reset` would leave the count permanently incremented. The comment in source ([`src/pool.rs:428`](src/pool.rs)) spells it out: "decremented before reset() is called so the pool remains consistent."

Waiter dispatch is resilient to dropped receivers. `tx.send(conn)` returns the connection back in `Err(conn)` if the other side is gone; the loop simply tries the next waiter. Only when no live waiter remains does the connection go back to the idle deque.

## Lifecycle hooks

Six hooks, defined in [`LifecycleHooks<C>`](src/pool.rs#L149):

| Hook | Receives | When |
|---|---|---|
| `before_acquire` | nothing | Start of `get()`, before any contention |
| `on_create` | `&C` | After `C::connect()` succeeds |
| `on_checkout` | `&C` | Just before returning a `PoolGuard` |
| `on_checkin` | `&C` | After `reset()` passes, before waiter dispatch |
| `after_release` | nothing | After a connection is fully disposed |
| `on_destroy` | nothing | When a connection is destroyed |

All hooks are `Box<dyn Fn(...) + Send + Sync>`: immutable `Fn`, not `FnMut`. Hooks that need mutable state must use interior mutability (`Arc<AtomicU64>`, `Arc<Mutex<_>>`).

Hooks run with **no pool locks held**. In `drain()` this is enforced by explicitly draining the idle deque into a local `Vec` before calling `on_destroy` ([`src/pool.rs:565`](src/pool.rs)). In `return_conn_async`, `on_checkin` fires before `waiters.lock()` is taken; `after_release` and `on_destroy` fire after the lock is released. The doc comment on `ConnPool` ([`src/pool.rs:213`](src/pool.rs)) warns that a hook calling back into `get()` will deadlock, so hooks must not re-enter the pool.

`after_release` is the cleanup hook that fires on every non-panic release path: returned-to-idle, returned-to-waiter, and destroyed. It is useful for always-fire telemetry without caring about outcome.

## Drain protocol

`drain()` ([`src/pool.rs:560`](src/pool.rs)) is the shutdown path. Race-free shutdown is non-trivial because in-flight checkouts can return connections at any time.

```
1. draining.store(true, Release)
2. lock idle:
     move all entries into local vec
     total_count -= n
     total_destroyed += n
   unlock idle
   call on_destroy for each evicted entry      (outside the lock)
3. lock waiters:
     take all (dropping tx closes receivers, waiters get PoolError::Closed)
     waiter_count -= n
   unlock waiters
4. while total_count > 0:
     drain_complete.notified().await
5. send on shutdown_tx to stop the maintenance task
```

Step 4 is the coordination point. Each in-flight connection returning through `return_conn_async` sees `draining = true` and destroys itself (step 5 of the release path). After each destruction, `maybe_notify_drain` checks `total_count == 0` and calls `drain_complete.notify_one()` ([`src/pool.rs:501`](src/pool.rs)). The `drain()` loop wakes, re-checks, and either exits or waits again. Multiple concurrent returns are safe: `notify_one` is a wake token, extra notifications are idempotent.

## Maintenance task

A single `tokio::spawn` started at pool construction ([`src/pool.rs:271`](src/pool.rs)) runs forever on a `tokio::time::interval` (default 10 s). Each tick:

1. Check `draining`; exit if true (also exits on `shutdown_rx` signal).
2. Lock `idle`, `retain(|entry| now < entry.expires_at)`. Expired entries are dropped; `total_count` and `total_destroyed` are updated with the evicted count in one atomic step each.
3. If current idle count < `min_idle` and `total_count < max_size`, create replacement connections via `create_and_track` and push them to `idle`.

Eagerly evicting on checkout (in `try_get_idle`) means the maintenance task is not strictly required for correctness: it trims idle connections to respect `max_lifetime` even on a quiet pool, and pre-warms up to `min_idle`.

## Min / max connections

- `min_idle` connections are created synchronously during `ConnPool::new` ([`src/pool.rs:258`](src/pool.rs)). Failures log a warning (`tracing::warn`) but do not abort construction; the pool starts with fewer than requested.
- `warm_up(target)` is an explicit post-construction bulk creator for callers that want predictable latency on the first few requests.
- On demand, the checkout slow path creates a connection whenever `total_count < max_size`.
- When `total_count == max_size`, callers wait in the queue until `checkout_timeout` expires.

The capacity gate uses optimistic `fetch_add` + rollback rather than a compare-and-swap loop. This matters under high contention: the increment is cheap and the rollback path is rare.

## Metrics

```rust
// src/pool.rs:183
pub struct PoolMetrics {
    pub total: usize,
    pub idle: usize,
    pub in_use: usize,
    pub waiters: usize,
    pub total_checkouts: u64,
    pub total_created: u64,
    pub total_destroyed: u64,
    pub total_timeouts: u64,
}
```

Every field is read directly from an atomic without locking. `idle` is computed as `total.saturating_sub(in_use)` to tolerate the brief window where one counter has been updated but the other has not. The doc comment at [`src/pool.rs:513`](src/pool.rs) calls this out explicitly: metrics may be momentarily off by one under high concurrency, but are always consistent with a real program state that existed shortly before or after the snapshot.

## Hook safety and the common footguns

- **Do not re-enter the pool from a hook.** Calling `get()` from inside `on_checkout` deadlocks.
- **Hooks are `Fn`, not `FnMut`.** Use `Arc<AtomicU64>` or `Arc<Mutex<_>>` if you need mutable captured state.
- **Hook panics are not caught.** A panicking hook propagates through the task, typically killing the task that triggered it. The pool is defensive about counter consistency (decrement before reset, rollback on connect failure), but user-visible state is the user's responsibility.
- **`has_pending_data` must be fast and non-blocking.** It runs under the checkout fast path; a slow implementation regresses the whole pool.
- **`reset` runs on every checkin.** Keep it cheap, or conditionally skip it in your Poolable impl.

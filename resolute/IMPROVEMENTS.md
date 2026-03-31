# resolute improvement log

## Completed

1. **Better error types** — Added Pool, Timeout, MissingParam, Io, Config, QueryFailed variants. SQL attached to errors on failure.
2. **Structured tracing** — debug spans on query/execute with sql, rows, elapsed_us. Info on connect. Warn on errors and pool timeouts.
3. **Prometheus metrics** — Lock-free atomic counters. `metrics::snapshot()` and `metrics::gather()` (Prometheus exposition format).
4. **Graceful reconnection** — `ReconnectingClient` with ArcSwap for lock-free reads. Mutex only during reconnect to prevent thundering herd.
5. **Connection validation hook** — `Client::connect_with_init(addr, user, pass, db, &["SET search_path TO ..."])`.
6. **Property-based tests** — Proptest roundtrips for all Encode+Decode types + fuzz testing for named param parser and text array parser.
7. **Test database helper** — `TestDb::create()` + `#[resolute::test]` attribute macro.
8. **Streaming backpressure** — Configurable buffer size via `Client::DEFAULT_STREAM_BUFFER`.
9. **Pool connection reuse** — Fixed: pool now manages AsyncConn directly. DISCARD ALL on return, statement cache cleared.
10. **Date/timestamp infinity** — PgTimestamp and PgDate types handle infinity/neg_infinity. Chrono decoders reject infinity with clear error.
11. **SCRAM-SHA-256-PLUS** — ChannelBinding enum supports tls-server-end-point binding.
12. **Notification delivery** — NotificationResponse forwarded during queries instead of dropped.
13. **Request timeout** — 5-minute timeout on AsyncConn submit prevents hanging on dead reader/writer.
14. **Streaming COPY** — Data sent in 1MB chunks instead of single buffer.

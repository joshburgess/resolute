//! Concurrent and load-shape benchmarks: resolute vs sqlx.
//!
//! Covers two scenarios microbench files normally miss:
//!
//! 1. **Concurrent load on a pool.** Multiple tasks contending for a small
//!    pool, exercising the writer-task mpsc, TCP write coalescing, FIFO
//!    response matching, and the lock-free pool atomics.
//! 2. **Server-bound work.** Queries where Postgres itself is the dominant
//!    cost (heavy aggregations, large result decode), so the driver overhead
//!    is a smaller fraction. The expectation is that the resolute / sqlx gap
//!    closes here, which is the honest picture.
//!
//! Requires: docker compose up -d (PostgreSQL on port 54322)
//! Run: cargo bench -p resolute --bench concurrent_load

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use resolute::test_db::{
    test_addr as addr, test_database as db, test_database_url, test_password as pass,
    test_user as user,
};
use resolute::{SharedPool, ExclusivePool};
use sqlx::postgres::PgPoolOptions;

fn sqlx_url() -> String {
    test_database_url()
}

/// Multi-thread runtime so spawned tasks actually run in parallel.
fn rt_mt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// Current-thread runtime: one OS thread, all tasks multiplex onto it. Best
/// for showing the writer-task coalescing effect because submissions queue up
/// before the writer drains them.
fn rt_st() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// 1. Concurrent SELECT 1 on a 4-connection pool, 16 concurrent tasks.
//
// Tests fan-out under contention. With pool size = 4 and 16 tasks, each
// connection sees ~4 sequential queries. Coalescing applies to the per-conn
// mpsc when multiple tasks happen to land on the same connection.
// ---------------------------------------------------------------------------

fn bench_concurrent_select_4c_16t(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_select_4c_16t");
    let rt = rt_mt();

    let pt_pool = Arc::new(
        rt.block_on(ExclusivePool::connect(addr(), user(), pass(), db(), 4))
            .unwrap(),
    );
    rt.block_on(pt_pool.warm_up(4));

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(16);
                for _ in 0..16 {
                    let p = Arc::clone(&pt_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await.unwrap();
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    // Shared (multiplexed) pool: 16 tasks submit concurrently to 4 conns; the
    // writer task on each conn coalesces submissions into batched writes.
    // No exclusive checkout, no waiter queue.
    let shared_pool = Arc::new(
        rt.block_on(SharedPool::connect(addr(), user(), pass(), db(), 4))
            .unwrap(),
    );

    group.bench_function("resolute_shared", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(16);
                for _ in 0..16 {
                    let p = Arc::clone(&shared_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await;
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let sqlx_pool = rt
        .block_on(
            PgPoolOptions::new()
                .min_connections(4)
                .max_connections(4)
                .connect(&sqlx_url()),
        )
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(16);
                for _ in 0..16 {
                    let p = sqlx_pool.clone();
                    handles.push(tokio::spawn(async move {
                        let row: (i32,) = sqlx::query_as("SELECT 1::int4")
                            .fetch_one(&p)
                            .await
                            .unwrap();
                        std::hint::black_box(row);
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Single-connection coalescing: pool size 1, 8 concurrent tasks.
//
// All 8 queries serialize on one TCP connection (Postgres processes
// sequentially per-conn). Resolute's writer task can batch submissions into
// a single write() syscall when they arrive close in time. This is the
// purest test of TCP write coalescing.
// ---------------------------------------------------------------------------

fn bench_coalesce_single_conn_8t(c: &mut Criterion) {
    let mut group = c.benchmark_group("coalesce_single_conn_8t");
    let rt = rt_st();

    let pt_pool = Arc::new(
        rt.block_on(ExclusivePool::connect(addr(), user(), pass(), db(), 1))
            .unwrap(),
    );
    rt.block_on(pt_pool.warm_up(1));

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(8);
                for _ in 0..8 {
                    let p = Arc::clone(&pt_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await.unwrap();
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let shared_pool = Arc::new(
        rt.block_on(SharedPool::connect(addr(), user(), pass(), db(), 1))
            .unwrap(),
    );

    group.bench_function("resolute_shared", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(8);
                for _ in 0..8 {
                    let p = Arc::clone(&shared_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await;
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let sqlx_pool = rt
        .block_on(
            PgPoolOptions::new()
                .min_connections(1)
                .max_connections(1)
                .connect(&sqlx_url()),
        )
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(8);
                for _ in 0..8 {
                    let p = sqlx_pool.clone();
                    handles.push(tokio::spawn(async move {
                        let row: (i32,) = sqlx::query_as("SELECT 1::int4")
                            .fetch_one(&p)
                            .await
                            .unwrap();
                        std::hint::black_box(row);
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Heavy-pool fan-out: 8 connections, 64 tasks. Closer to a busy
// application: many concurrent requests, oversubscribed pool, real
// queueing in the waiter list.
// ---------------------------------------------------------------------------

fn bench_concurrent_select_8c_64t(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_select_8c_64t");
    let rt = rt_mt();

    let pt_pool = Arc::new(
        rt.block_on(ExclusivePool::connect(addr(), user(), pass(), db(), 8))
            .unwrap(),
    );
    rt.block_on(pt_pool.warm_up(8));

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(64);
                for _ in 0..64 {
                    let p = Arc::clone(&pt_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await.unwrap();
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let shared_pool = Arc::new(
        rt.block_on(SharedPool::connect(addr(), user(), pass(), db(), 8))
            .unwrap(),
    );

    group.bench_function("resolute_shared", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(64);
                for _ in 0..64 {
                    let p = Arc::clone(&shared_pool);
                    handles.push(tokio::spawn(async move {
                        let conn = p.get().await;
                        let rows = conn.query("SELECT 1::int4", &[]).await.unwrap();
                        let _: i32 = rows[0].get(0).unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    let sqlx_pool = rt
        .block_on(
            PgPoolOptions::new()
                .min_connections(8)
                .max_connections(8)
                .connect(&sqlx_url()),
        )
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::with_capacity(64);
                for _ in 0..64 {
                    let p = sqlx_pool.clone();
                    handles.push(tokio::spawn(async move {
                        let row: (i32,) = sqlx::query_as("SELECT 1::int4")
                            .fetch_one(&p)
                            .await
                            .unwrap();
                        std::hint::black_box(row);
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Server-bound work: SELECT count(*) FROM generate_series(1, 100_000).
//
// The server has to materialize 100k rows and aggregate them. Driver-side
// cost is a tiny fraction of the total. The gap should compress significantly
// or vanish here; that's the honest part of the picture.
// ---------------------------------------------------------------------------

fn bench_server_bound_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_bound_aggregate_100k");
    let rt = rt_st();

    let pt_client = rt
        .block_on(resolute::Client::connect(addr(), user(), pass(), db()))
        .unwrap();

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query("SELECT count(*) FROM generate_series(1, 100000)", &[])
                    .await
                    .unwrap();
                let _: i64 = rows[0].get(0).unwrap();
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(&sqlx_url()))
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row: (i64,) = sqlx::query_as("SELECT count(*) FROM generate_series(1, 100000)")
                    .fetch_one(&sqlx_pool)
                    .await
                    .unwrap();
                std::hint::black_box(row);
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Large result set: 10_000 rows, single i32 column.
//
// Driver overhead = N row decode operations. Tests the decode hot path
// (parse_data_row + Encode<i32>::decode) at scale rather than per-row.
// ---------------------------------------------------------------------------

fn bench_large_result_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_result_10k_rows");
    let rt = rt_st();

    let pt_client = rt
        .block_on(resolute::Client::connect(addr(), user(), pass(), db()))
        .unwrap();

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query("SELECT generate_series(1, 10000)::int4", &[])
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 10_000);
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(&sqlx_url()))
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows: Vec<(i32,)> = sqlx::query_as("SELECT generate_series(1, 10000)::int4")
                    .fetch_all(&sqlx_pool)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 10_000);
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Wide-row decode: 10 columns per row, 1000 rows. Stresses the per-row
// decode loop with a realistic shape (mixed types).
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn bench_wide_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("wide_rows_10col_1k");
    let rt = rt_st();

    let sql = "SELECT \
        i::int4 AS a, \
        (i * 2)::int8 AS b, \
        ('row_' || i)::text AS c, \
        (i % 2 = 0) AS d, \
        i::float8 AS e, \
        (i + 1)::int4 AS f, \
        (i * 3)::int8 AS g, \
        ('val_' || i)::text AS h, \
        (i % 3 = 0) AS i_col, \
        (i * 1.5)::float8 AS j \
        FROM generate_series(1, 1000) AS i";

    let pt_client = rt
        .block_on(resolute::Client::connect(addr(), user(), pass(), db()))
        .unwrap();

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client.query(sql, &[]).await.unwrap();
                assert_eq!(rows.len(), 1000);
                let _: i32 = rows[0].get(0).unwrap();
                let _: i64 = rows[0].get(1).unwrap();
                let _: String = rows[0].get(2).unwrap();
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(&sqlx_url()))
        .unwrap();

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows: Vec<(i32, i64, String, bool, f64, i32, i64, String, bool, f64)> =
                    sqlx::query_as(sql).fetch_all(&sqlx_pool).await.unwrap();
                assert_eq!(rows.len(), 1000);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_select_4c_16t,
    bench_coalesce_single_conn_8t,
    bench_concurrent_select_8c_64t,
    bench_server_bound_aggregate,
    bench_large_result_10k,
    bench_wide_rows,
);
criterion_main!(benches);

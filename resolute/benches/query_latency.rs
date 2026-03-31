//! Side-by-side query latency benchmarks: resolute vs sqlx against real PostgreSQL.
//!
//! Requires: docker compose up -d (PostgreSQL on port 54322)
//! Run: cargo bench -p resolute --bench query_latency

use criterion::{criterion_group, criterion_main, Criterion};
use resolute::Client;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row as SqlxRow;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";
const SQLX_URL: &str = "postgres://postgres:postgres@127.0.0.1:54322/postgrest_test";

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// SELECT 1 (minimal round-trip)
// ---------------------------------------------------------------------------

fn bench_select_1(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_1");
    let rt = rt();

    // resolute
    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
                let _: i32 = rows[0].get(0).unwrap();
            });
        });
    });

    // sqlx
    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row: (i32,) = sqlx::query_as("SELECT 1::int4")
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
// Parameterized SELECT (single i32 param)
// ---------------------------------------------------------------------------

fn bench_select_param(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_param_i32");
    let rt = rt();

    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query("SELECT $1::int4 AS n", &[&42i32])
                    .await
                    .unwrap();
                let _: i32 = rows[0].get(0).unwrap();
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row: (i32,) = sqlx::query_as("SELECT $1::int4")
                    .bind(42i32)
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
// Multi-column SELECT (3 columns, 3 params)
// ---------------------------------------------------------------------------

fn bench_select_3col(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_3col");
    let rt = rt();

    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query(
                        "SELECT $1::int4 AS a, $2::text AS b, $3::bool AS c",
                        &[&1i32, &"hello", &true],
                    )
                    .await
                    .unwrap();
                let _: i32 = rows[0].get(0).unwrap();
                let _: String = rows[0].get(1).unwrap();
                let _: bool = rows[0].get(2).unwrap();
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row = sqlx::query("SELECT $1::int4 AS a, $2::text AS b, $3::bool AS c")
                    .bind(1i32)
                    .bind("hello")
                    .bind(true)
                    .fetch_one(&sqlx_pool)
                    .await
                    .unwrap();
                let _: i32 = row.get("a");
                let _: String = row.get("b");
                let _: bool = row.get("c");
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 100 rows
// ---------------------------------------------------------------------------

fn bench_select_100_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_100_rows");
    let rt = rt();

    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query("SELECT generate_series(1, 100)::int4 AS n", &[])
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 100);
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows: Vec<(i32,)> =
                    sqlx::query_as("SELECT generate_series(1, 100)::int4")
                        .fetch_all(&sqlx_pool)
                        .await
                        .unwrap();
                assert_eq!(rows.len(), 100);
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// INSERT + DELETE cycle
// ---------------------------------------------------------------------------

fn bench_insert_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_delete_cycle");
    let rt = rt();

    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    rt.block_on(pt_client.simple_query(
        "CREATE TABLE IF NOT EXISTS bench_cycle (id int PRIMARY KEY, val text)",
    ))
    .unwrap();

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                pt_client
                    .execute(
                        "INSERT INTO bench_cycle VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET val = $2",
                        &[&1i32, &"bench"],
                    )
                    .await
                    .unwrap();
                pt_client
                    .execute("DELETE FROM bench_cycle WHERE id = $1", &[&1i32])
                    .await
                    .unwrap();
            });
        });
    });

    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                sqlx::query(
                    "INSERT INTO bench_cycle VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET val = $2",
                )
                .bind(1i32)
                .bind("bench")
                .execute(&sqlx_pool)
                .await
                .unwrap();
                sqlx::query("DELETE FROM bench_cycle WHERE id = $1")
                    .bind(1i32)
                    .execute(&sqlx_pool)
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Statement cache hit (second execution of same query)
// ---------------------------------------------------------------------------

fn bench_cache_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_hit");
    let rt = rt();

    let pt_client = rt.block_on(Client::connect(ADDR, USER, PASS, DB)).unwrap();
    rt.block_on(pt_client.query("SELECT $1::int4 AS n", &[&1i32]))
        .unwrap();

    group.bench_function("resolute", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows = pt_client
                    .query("SELECT $1::int4 AS n", &[&42i32])
                    .await
                    .unwrap();
                let _: i32 = rows[0].get(0).unwrap();
            });
        });
    });

    // sqlx also has statement caching via PgPool
    let sqlx_pool = rt
        .block_on(PgPoolOptions::new().max_connections(1).connect(SQLX_URL))
        .unwrap();
    // Warm cache.
    rt.block_on(async {
        let _: (i32,) = sqlx::query_as("SELECT $1::int4")
            .bind(1i32)
            .fetch_one(&sqlx_pool)
            .await
            .unwrap();
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row: (i32,) = sqlx::query_as("SELECT $1::int4")
                    .bind(42i32)
                    .fetch_one(&sqlx_pool)
                    .await
                    .unwrap();
                std::hint::black_box(row);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_select_1,
    bench_select_param,
    bench_select_3col,
    bench_select_100_rows,
    bench_insert_delete,
    bench_cache_hit,
);
criterion_main!(benches);

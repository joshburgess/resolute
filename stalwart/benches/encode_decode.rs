//! Side-by-side encode benchmarks: stalwart vs sqlx.
//!
//! Decode benchmarks only cover stalwart since sqlx doesn't expose raw decode.
//! For end-to-end decode comparison, see the query_latency benchmark.
//!
//! Run: cargo bench -p stalwart --bench encode_decode

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqlx::postgres::Postgres;

// ---------------------------------------------------------------------------
// Helpers to avoid trait ambiguity
// ---------------------------------------------------------------------------

fn pt_encode<T: stalwart::Encode>(val: &T, buf: &mut bytes::BytesMut) {
    stalwart::Encode::encode(val, buf);
}

fn pt_decode<T: stalwart::Decode>(buf: &[u8]) -> T {
    T::decode(buf).unwrap()
}

fn sqlx_encode<'q, T: sqlx::Encode<'q, Postgres>>(
    val: T,
    buf: &mut sqlx::postgres::PgArgumentBuffer,
) {
    let _ = sqlx::Encode::<Postgres>::encode(val, buf);
}

// ---------------------------------------------------------------------------
// i32
// ---------------------------------------------------------------------------

fn bench_encode_i32(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_i32");

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            pt_encode(&42i32, &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(black_box(42i32), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_decode_i32(c: &mut Criterion) {
    let mut buf = bytes::BytesMut::new();
    pt_encode(&42i32, &mut buf);
    let bytes = buf.freeze();
    c.bench_function("decode_i32/stalwart", |b| {
        b.iter(|| black_box(pt_decode::<i32>(&bytes)));
    });
}

// ---------------------------------------------------------------------------
// i64
// ---------------------------------------------------------------------------

fn bench_encode_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_i64");

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            pt_encode(&123456789012345i64, &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(black_box(123456789012345i64), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_decode_i64(c: &mut Criterion) {
    let mut buf = bytes::BytesMut::new();
    pt_encode(&123456789012345i64, &mut buf);
    let bytes = buf.freeze();
    c.bench_function("decode_i64/stalwart", |b| {
        b.iter(|| black_box(pt_decode::<i64>(&bytes)));
    });
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

fn bench_encode_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_string_39b");
    let s = "hello world, this is a benchmark string";

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            pt_encode(&s.to_string(), &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(black_box(s), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_decode_string(c: &mut Criterion) {
    let s = "hello world, this is a benchmark string";
    let mut buf = bytes::BytesMut::new();
    pt_encode(&s.to_string(), &mut buf);
    let bytes = buf.freeze();
    c.bench_function("decode_string_39b/stalwart", |b| {
        b.iter(|| black_box(pt_decode::<String>(&bytes)));
    });
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

fn bench_encode_uuid(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_uuid");
    let id = uuid::Uuid::new_v4();

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(32);
        b.iter(|| {
            buf.clear();
            pt_encode(&id, &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(black_box(id), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_decode_uuid(c: &mut Criterion) {
    let id = uuid::Uuid::new_v4();
    let mut buf = bytes::BytesMut::new();
    pt_encode(&id, &mut buf);
    let bytes = buf.freeze();
    c.bench_function("decode_uuid/stalwart", |b| {
        b.iter(|| black_box(pt_decode::<uuid::Uuid>(&bytes)));
    });
}

// ---------------------------------------------------------------------------
// Timestamptz
// ---------------------------------------------------------------------------

fn bench_encode_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_timestamptz");
    let ts = chrono::Utc::now();

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(32);
        b.iter(|| {
            buf.clear();
            pt_encode(&ts, &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(black_box(ts), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

fn bench_decode_timestamp(c: &mut Criterion) {
    let ts = chrono::Utc::now();
    let mut buf = bytes::BytesMut::new();
    pt_encode(&ts, &mut buf);
    let bytes = buf.freeze();
    c.bench_function("decode_timestamptz/stalwart", |b| {
        b.iter(|| black_box(pt_decode::<chrono::DateTime<chrono::Utc>>(&bytes)));
    });
}

// ---------------------------------------------------------------------------
// JSONB
// ---------------------------------------------------------------------------

fn bench_encode_jsonb(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_jsonb");
    let val = serde_json::json!({"key": "value", "num": 42, "arr": [1,2,3]});

    group.bench_function("stalwart", |b| {
        let mut buf = bytes::BytesMut::with_capacity(256);
        b.iter(|| {
            buf.clear();
            pt_encode(&val, &mut buf);
            black_box(&buf);
        });
    });

    group.bench_function("sqlx", |b| {
        b.iter(|| {
            let mut buf = sqlx::postgres::PgArgumentBuffer::default();
            sqlx_encode(sqlx::types::Json(black_box(&val)), &mut buf);
            black_box(&buf);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Named param rewrite (stalwart only — sqlx has no equivalent)
// ---------------------------------------------------------------------------

fn bench_named_param_rewrite(c: &mut Criterion) {
    let sql = "SELECT * FROM users WHERE org_id = :org_id AND name = :name AND active = :active AND created > :since ORDER BY :name";
    c.bench_function("named_param_rewrite_4params", |b| {
        b.iter(|| {
            black_box(stalwart::named_params::rewrite(black_box(sql)));
        });
    });
}

criterion_group!(
    benches,
    bench_encode_i32,
    bench_decode_i32,
    bench_encode_i64,
    bench_decode_i64,
    bench_encode_string,
    bench_decode_string,
    bench_encode_uuid,
    bench_decode_uuid,
    bench_encode_timestamp,
    bench_decode_timestamp,
    bench_encode_jsonb,
    bench_named_param_rewrite,
);
criterion_main!(benches);

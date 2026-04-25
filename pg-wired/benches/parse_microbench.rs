//! Microbenchmarks for the DataRow parser. No I/O — just buffer in,
//! parse out. Sensitive to per-iteration cost in `parse_data_row` /
//! `read_cell_entry`, which is invisible in the wall-clock concurrent
//! benchmarks (those are 83% kevent/park time).

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_wired::protocol::backend::parse_message;

/// Build a DataRow message: tag(1) + len(4) + num_cols(2) + cells.
/// Each cell is len(4) + data(len bytes).
fn build_data_row(cell_sizes: &[usize]) -> BytesMut {
    let body_size: usize = 2 + cell_sizes.iter().map(|s| 4 + s).sum::<usize>();
    let mut buf = BytesMut::with_capacity(5 + body_size);
    buf.put_u8(b'D');
    buf.put_i32((body_size + 4) as i32);
    buf.put_i16(cell_sizes.len() as i16);
    for &n in cell_sizes {
        buf.put_i32(n as i32);
        buf.put_bytes(b'x', n);
    }
    buf
}

fn build_many_data_rows(cell_sizes: &[usize], num_rows: usize) -> BytesMut {
    let one = build_data_row(cell_sizes);
    let mut buf = BytesMut::with_capacity(one.len() * num_rows);
    for _ in 0..num_rows {
        buf.extend_from_slice(&one);
    }
    buf
}

fn bench_parse_data_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_data_row");

    // Single 10-column wide-row, mid-sized cells. Mirrors the shape used by
    // the wide_rows_10col_1k bench (i32, i64, text, bool, f64, repeated).
    // Average cell sizes: 4, 8, ~6, 1, 8, 4, 8, ~6, 1, 8 ≈ 54 bytes.
    let cells = vec![4usize, 8, 6, 1, 8, 4, 8, 6, 1, 8];

    // Single-row parse. Maximally sensitive to per-call setup cost.
    group.bench_function("10col_single", |b| {
        let template = build_data_row(&cells);
        b.iter_batched(
            || template.clone(),
            |mut buf| {
                let _ = black_box(parse_message(&mut buf));
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // 1000 rows in one buffer, parsed back-to-back. Mirrors the bench
    // workload's actual parsing pattern; `parse_data_row` is called 1000
    // times per buffer so per-cell costs amplify 10000x (10 cols × 1000).
    group.bench_function("10col_1000rows", |b| {
        let template = build_many_data_rows(&cells, 1000);
        b.iter_batched(
            || template.clone(),
            |mut buf| {
                while !buf.is_empty() {
                    match parse_message(&mut buf) {
                        Ok(Some(msg)) => {
                            black_box(msg);
                        }
                        _ => break,
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Narrow rows (3 cols, common shape for scalar selects).
    group.bench_function("3col_1000rows", |b| {
        let cells = vec![4usize, 8, 16];
        let template = build_many_data_rows(&cells, 1000);
        b.iter_batched(
            || template.clone(),
            |mut buf| {
                while !buf.is_empty() {
                    match parse_message(&mut buf) {
                        Ok(Some(msg)) => {
                            black_box(msg);
                        }
                        _ => break,
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Heap path (>12 cols, falls out of inline cell storage).
    group.bench_function("20col_1000rows", |b| {
        let cells: Vec<usize> = (0..20).map(|i| 4 + (i % 8)).collect();
        let template = build_many_data_rows(&cells, 1000);
        b.iter_batched(
            || template.clone(),
            |mut buf| {
                while !buf.is_empty() {
                    match parse_message(&mut buf) {
                        Ok(Some(msg)) => {
                            black_box(msg);
                        }
                        _ => break,
                    }
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_parse_data_row);
criterion_main!(benches);

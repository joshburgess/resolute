# Benchmarking

This page covers how to run the workspace benchmarks, save baselines, and
compare runs. For the actual numbers and methodology, see
[`PERFORMANCE.md`](PERFORMANCE.md).

## Where the benches live

| Crate     | Bench               | What it measures                                          |
|-----------|---------------------|-----------------------------------------------------------|
| `pg-wired`| `parse_microbench`  | Pure DataRow parsing, no I/O. Used to validate hot-path changes. |
| `resolute`| `query_latency`     | Single-query latency for a typical SELECT workload.       |
| `resolute`| `concurrent_load`   | Throughput under concurrent client load.                  |
| `resolute`| `encode_decode`     | Per-type encode and decode microbenches.                  |

All benches use [Criterion](https://github.com/bheisler/criterion.rs) with
HTML reports enabled. Output lands in `target/criterion/`.

## Prerequisites

The query and concurrent-load benches require a running Postgres instance
matching the test setup (`docker compose up -d` from the repo root). The
`parse_microbench` and `encode_decode` benches do no I/O and run anywhere.

## Running

```bash
# Single bench, single crate.
cargo bench -p pg-wired --bench parse_microbench
cargo bench -p resolute --bench query_latency

# All benches in a crate.
cargo bench -p resolute

# Filter to a specific group or function (Criterion regex).
cargo bench -p resolute --bench query_latency -- "select_by_id"
```

## Saving and comparing baselines

Criterion compares against the previous run by default, but the saved
results live under `target/criterion/` and are wiped by `cargo clean`. For
comparisons that survive a clean rebuild (and for PR review), save a named
baseline:

```bash
# On main, before your changes:
cargo bench -p resolute --bench query_latency -- --save-baseline main

# Make changes, then compare:
cargo bench -p resolute --bench query_latency -- --baseline main
```

Criterion prints `change: -3.2% (p < 0.05)` style deltas relative to the
named baseline. Anything outside `±5%` is worth investigating; anything
outside `±10%` is almost certainly a real shift, not measurement noise.

To overwrite a baseline (after merging the change you measured):

```bash
cargo bench -p resolute --bench query_latency -- --save-baseline main
```

## Reading the HTML report

Criterion writes a per-bench `report/index.html` under
`target/criterion/<group>/`. Open it in a browser for distribution plots,
violin plots, and regression tracking. The CLI summary covers most cases,
but the HTML view is the right place to check for bimodal distributions
(usually a sign of GC, JIT, or scheduler interference).

## Methodology notes

- **Warm-up matters.** Criterion handles this automatically, but if you are
  comparing two builds, run them in the same shell session against the same
  warm Postgres. A cold container start adds tens of milliseconds to the
  first sample.
- **Pin the runtime.** Run benches with `taskset -c 0,1` (Linux) or with
  CPU power management settings configured. Background load distorts results.
- **A/B/A interleaving.** For tight comparisons (sub-5% deltas), run the
  baseline, the candidate, and the baseline again, then average across the
  paired runs. The
  [`Resolved A. Removed unsafe`](PRE_RELEASE_AUDIT.md) entry in the audit
  shows this methodology applied to a parser change.

## Performance gates in PRs

Benchmark results are advisory unless the change is performance-motivated. If
your PR claims a speedup or you suspect a regression, include before / after
numbers from `--baseline main` in the PR description. Otherwise, CI does
not run benchmarks.

## Cleaning up

`target/criterion/` is gitignored. To wipe local results:

```bash
rm -rf target/criterion
```

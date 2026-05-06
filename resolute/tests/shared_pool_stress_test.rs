//! Stress test for `SharedPool` under concurrent access.
//!
//! Reproduces a flake observed in `concurrent_load.rs`: under N concurrent
//! callers on a small multiplexed pool, occasional queries fail with
//! "prepared statement \"s0\" does not exist" (PgError 26000). The bug
//! has been seen on PostgreSQL 16 in CI and is timing-dependent (passes
//! on rerun). This harness drives proptest's shrinker against deterministic
//! seeds so the bug, when it surfaces, can be minimized to a small input.
//!
//! Requires: docker compose up -d (PostgreSQL on port 54322).

#![allow(dead_code)]

use std::sync::Arc;

use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;
use resolute::test_db::{
    test_addr as addr, test_database as db, test_password as pass, test_user as user,
};
use resolute::SharedPool;

#[derive(Debug, Clone, Copy)]
enum Action {
    /// `SELECT $1::int4` with a known parameter; result must equal the param.
    SelectParam(i32),
    /// Literal `SELECT 1::int4` (no params); the exact shape used by the
    /// failing bench. Cache always reuses the same statement here.
    SelectLiteral,
    /// `SELECT $1::int8` (different statement than SelectParam, distinct
    /// cache entry); pressures the per-connection statement cache by
    /// alternating between two statements per connection.
    SelectInt8(i64),
}

fn arb_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        2 => Just(Action::SelectLiteral),
        2 => any::<i32>().prop_map(Action::SelectParam),
        1 => any::<i64>().prop_map(Action::SelectInt8),
    ]
}

async fn run_one(pool: &Arc<SharedPool>, action: Action) -> Result<(), String> {
    let client = pool.get().await;
    match action {
        Action::SelectLiteral => {
            let rows = client
                .query("SELECT 1::int4", &[])
                .await
                .map_err(|e| format!("SELECT 1::int4 failed: {e}"))?;
            let v: i32 = rows[0]
                .get(0)
                .map_err(|e| format!("decode int4 from literal: {e}"))?;
            if v != 1 {
                return Err(format!("expected 1, got {v}"));
            }
        }
        Action::SelectParam(n) => {
            let rows = client
                .query("SELECT $1::int4", &[&n])
                .await
                .map_err(|e| format!("SELECT $1::int4 (n={n}) failed: {e}"))?;
            let v: i32 = rows[0]
                .get(0)
                .map_err(|e| format!("decode int4 from param: {e}"))?;
            if v != n {
                return Err(format!("expected {n}, got {v}"));
            }
        }
        Action::SelectInt8(n) => {
            let rows = client
                .query("SELECT $1::int8", &[&n])
                .await
                .map_err(|e| format!("SELECT $1::int8 (n={n}) failed: {e}"))?;
            let v: i64 = rows[0]
                .get(0)
                .map_err(|e| format!("decode int8 from param: {e}"))?;
            if v != n {
                return Err(format!("expected {n}, got {v}"));
            }
        }
    }
    Ok(())
}

/// Number of times each proptest case is replayed. The bug is
/// timing-dependent: a single run of a small action sequence may pass
/// by luck. Running each case N times lets the shrinker treat
/// "fails at least once in N tries" as a reliable failure, so it can
/// converge on a minimal counterexample.
const REPLAYS_PER_CASE: usize = 25;

fn run_case_once(pool_size: usize, actions: &[Action]) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;
    rt.block_on(async {
        let pool = Arc::new(
            SharedPool::connect(addr(), user(), pass(), db(), pool_size)
                .await
                .map_err(|e| format!("pool connect: {e}"))?,
        );
        let mut handles = Vec::with_capacity(actions.len());
        for action in actions {
            let p = Arc::clone(&pool);
            let action = *action;
            handles.push(tokio::spawn(async move { run_one(&p, action).await }));
        }
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(msg)) => return Err(msg),
                Err(join_err) => return Err(format!("task panicked: {join_err}")),
            }
        }
        Ok::<_, String>(())
    })
}

fn run_case(pool_size: usize, actions: Vec<Action>) -> Result<(), String> {
    for attempt in 0..REPLAYS_PER_CASE {
        if let Err(msg) = run_case_once(pool_size, &actions) {
            return Err(format!("attempt {attempt}: {msg}"));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        max_shrink_iters: 4096,
        .. ProptestConfig::default()
    })]

    #[test]
    fn shared_pool_concurrent_select(
        pool_size in 2usize..=8,
        actions in prop::collection::vec(arb_action(), 16..=96),
    ) {
        if let Err(msg) = run_case(pool_size, actions.clone()) {
            return Err(TestCaseError::fail(format!(
                "pool_size={pool_size} actions={} :: {msg}",
                actions.len()
            )));
        }
    }
}

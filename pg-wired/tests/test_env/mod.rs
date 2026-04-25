//! Shared env-driven test connection settings for pg-wired integration tests.

use std::sync::OnceLock;

fn cached(slot: &'static OnceLock<String>, var: &str, default: &str) -> &'static str {
    slot.get_or_init(|| std::env::var(var).unwrap_or_else(|_| default.to_string()))
        .as_str()
}

pub fn addr() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    cached(&V, "RESOLUTE_TEST_ADDR", "127.0.0.1:54322")
}

pub fn user() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    cached(&V, "RESOLUTE_TEST_USER", "postgres")
}

pub fn pass() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    cached(&V, "RESOLUTE_TEST_PASSWORD", "postgres")
}

pub fn db() -> &'static str {
    static V: OnceLock<String> = OnceLock::new();
    cached(&V, "RESOLUTE_TEST_DB", "postgrest_test")
}

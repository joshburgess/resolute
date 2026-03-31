//! Offline query metadata cache.
//!
//! Stores query metadata as JSON files in `.sqlx/query-{hash}.json`.
//! Compatible with CI/Docker builds where no database is available.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Cached column metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedColumn {
    pub name: String,
    pub type_oid: u32,
    /// True if the column can be NULL.
    #[serde(default)]
    pub nullable: bool,
}

/// A single cached query entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// The original SQL string.
    pub sql: String,
    /// FNV-1a hash of the SQL.
    pub hash: u64,
    /// Parameter type OIDs.
    pub param_oids: Vec<u32>,
    /// Result column metadata.
    pub columns: Vec<CachedColumn>,
}

/// Find the `.sqlx` cache directory.
/// Walks up from the crate's manifest dir to find the workspace root.
fn cache_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR points to the crate being compiled.
    // Walk up to find .sqlx/ (could be at workspace root or crate root).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));

    // Check crate root first, then walk up.
    let mut dir = manifest_dir.clone();
    loop {
        let candidate = dir.join(".sqlx");
        if candidate.is_dir() {
            return candidate;
        }
        if !dir.pop() {
            break;
        }
    }

    // Default: .sqlx/ next to the workspace Cargo.toml.
    // Walk up from manifest dir looking for workspace root (has [workspace] in Cargo.toml).
    let mut dir = manifest_dir;
    loop {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists() {
            if let Ok(contents) = std::fs::read_to_string(&cargo_toml) {
                if contents.contains("[workspace]") {
                    return dir.join(".sqlx");
                }
            }
        }
        if !dir.pop() {
            break;
        }
    }

    // Fallback: current directory.
    PathBuf::from(".sqlx")
}

/// Cache file path for a query hash.
fn cache_path(hash: u64) -> PathBuf {
    cache_dir().join(format!("query-{hash:016x}.json"))
}

/// Read cached metadata for a query hash.
pub fn read_cache(hash: u64) -> Option<CacheEntry> {
    let path = cache_path(hash);
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Write query metadata to the cache.
pub fn write_cache(entry: &CacheEntry) -> Result<(), String> {
    let dir = cache_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create .sqlx directory: {e}"))?;

    let path = dir.join(format!("query-{:016x}.json", entry.hash));
    let json = serde_json::to_string_pretty(entry)
        .map_err(|e| format!("Failed to serialize cache entry: {e}"))?;

    std::fs::write(&path, json)
        .map_err(|e| format!("Failed to write cache file {}: {e}", path.display()))?;

    Ok(())
}

pub mod engine;
pub mod engine_atomic;
mod engine_helpers;
mod engine_index;
pub mod engine_sorted;
mod engine_stats;
pub mod entry;
pub mod expiry_wheel;
mod hash_helpers;
pub mod hash_table;
pub mod index;
pub mod scan;
pub mod slab;
pub mod sorted_index;

pub use engine::KvEngine;
pub use engine_atomic::{AtomicError, CasResult};
pub use engine_stats::{ExpiredKey, KvStats};

/// Get current wall-clock time in milliseconds since Unix epoch.
///
/// Used by KV engine handlers and the core loop for TTL calculations.
/// Returns 0 on clock failure (extremely rare, only on broken systems).
pub fn current_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

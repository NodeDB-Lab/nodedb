//! Shared time utilities for the security subsystem.

/// Current wall-clock time in seconds since Unix epoch.
///
/// Returns 0 on clock failure (extremely rare, only on broken systems).
/// Used across security modules for timestamps, TTL, and expiry checks.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

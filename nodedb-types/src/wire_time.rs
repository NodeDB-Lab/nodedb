// SPDX-License-Identifier: Apache-2.0

//! Canonical wire timestamp types for NodeDB.
//!
//! All wire-level timestamps use these two concepts:
//!
//! - **Wall-clock instants** — signed 64-bit milliseconds since the Unix epoch
//!   (UTC). Negative values represent dates before 1970-01-01. Type alias:
//!   [`WallMs`].
//! - **Durations / TTLs** — unsigned 64-bit milliseconds. Always non-negative.
//!   Type alias: [`DurMs`].
//!
//! Using these aliases in field declarations makes the intended semantics
//! self-documenting and ensures a single canonical width per concept.

/// A wall-clock instant expressed as signed milliseconds since the Unix epoch
/// (UTC). Negative values represent dates before 1970-01-01.
///
/// This is the canonical wire type for every "when did this happen?" timestamp.
pub type WallMs = i64;

/// A duration or TTL expressed as unsigned milliseconds. Always ≥ 0.
///
/// This is the canonical wire type for every "how long?" field.
pub type DurMs = u64;

/// Return the current wall-clock time as [`WallMs`] (milliseconds since the
/// Unix epoch, UTC).
///
/// Returns `0` (Unix epoch) on the extremely unlikely event that the system
/// clock is before the Unix epoch — a value of 0 is obviously wrong and
/// easier to detect than `i64::MAX`. Logs the condition once per process via
/// `tracing::error!` to alert operators.
///
/// # TODO(post-launch): funnel direct `SystemTime::now()` callers through this
/// helper so all inline clock-read sites also get the once-per-process log.
pub fn current_wall_ms() -> WallMs {
    crate::clock::since_epoch()
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_else(|| {
            use std::sync::atomic::{AtomicBool, Ordering};
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    module = module_path!(),
                    "system clock is before UNIX_EPOCH; using 0 (epoch) \
                     — check NTP/RTC configuration"
                );
            }
            0
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_wall_ms_is_positive_and_reasonable() {
        let ms = current_wall_ms();
        // Must be after 2024-01-01 (1704067200000 ms)
        assert!(ms > 1_704_067_200_000, "wall ms too small: {ms}");
        // Must be before year 2100 (4102444800000 ms)
        assert!(ms < 4_102_444_800_000, "wall ms too large: {ms}");
    }

    #[test]
    fn wall_ms_is_i64_alias() {
        let _x: WallMs = -1_i64; // must compile — negative is valid
        let _y: DurMs = 0_u64;
    }
}

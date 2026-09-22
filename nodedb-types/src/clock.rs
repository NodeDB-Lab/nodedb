// SPDX-License-Identifier: BUSL-1.1

//! Wall-clock access, one target split.
//!
//! `wasm32-unknown-unknown` has no std clock: `SystemTime::now()` panics with
//! "time not implemented on this platform". Builds for that target read the
//! host clock through `js_sys` instead; every other target, `wasm32-wasip1`
//! included, uses std.
//!
//! Callers keep their own conversion, saturation, and pre-epoch fallback —
//! this module only answers "how long since the Unix epoch".

use std::time::Duration;

/// Time elapsed since the Unix epoch.
///
/// `None` when the system clock reads earlier than the epoch. On
/// `wasm32-unknown-unknown` the value comes from `Date.now()` and therefore
/// has millisecond resolution.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn since_epoch() -> Option<Duration> {
    Some(Duration::from_millis(js_sys::Date::now() as u64))
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn since_epoch() -> Option<Duration> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
}

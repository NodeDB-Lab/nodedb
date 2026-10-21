// SPDX-License-Identifier: Apache-2.0

//! The wall clock in nanoseconds since the Unix epoch. Every clock, lease,
//! and cut wait reads wall time here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock nanoseconds since the Unix epoch. A clock before the epoch
/// reads `0` and logs once. A clock past `u64::MAX` saturates.
pub fn wall_now_ns() -> u64 {
    u64::try_from(wall_now_nanos()).unwrap_or(u64::MAX)
}

/// [`wall_now_ns`] as a signed ordinal-space value. A clock past
/// `i64::MAX` (the year 2262) saturates.
pub fn wall_now_ns_i64() -> i64 {
    i64::try_from(wall_now_nanos()).unwrap_or(i64::MAX)
}

fn wall_now_nanos() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_nanos(),
        Err(_) => {
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    module = module_path!(),
                    "system clock is before UNIX_EPOCH; using 0 (epoch) \
                     — check NTP/RTC configuration"
                );
            }
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_accessors_read_one_clock() {
        let unsigned = wall_now_ns();
        let signed = wall_now_ns_i64();
        let later = wall_now_ns();
        assert!(unsigned > 0, "the wall clock is past the epoch");
        assert!(u64::try_from(signed).is_ok_and(|s| s >= unsigned && s <= later));
    }
}

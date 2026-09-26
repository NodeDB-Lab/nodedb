// SPDX-License-Identifier: BUSL-1.1

//! Rate limit for the leader's warning on a withheld lease renewal.
//!
//! A node whose coverage stays below a floor renews every interval, and each
//! renewal is withheld. The warning names the floor and the reported coverage
//! of every short group, once per node per window.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// At most one warning per node in this window.
const WARN_WINDOW: Duration = Duration::from_secs(10);

/// When each node's last warning was logged.
#[derive(Debug, Default)]
pub struct WithheldWarnings {
    last: Mutex<HashMap<u64, Instant>>,
}

impl WithheldWarnings {
    /// Whether a warning about `node_id` logs at `now`. A `true` starts the
    /// node's next window.
    pub fn should_warn(&self, node_id: u64, now: Instant) -> bool {
        let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        match last.get(&node_id) {
            Some(at) if now.duration_since(*at) < WARN_WINDOW => false,
            _ => {
                last.insert(node_id, now);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_warning_per_node_per_window() {
        let warnings = WithheldWarnings::default();
        let start = Instant::now();
        assert!(warnings.should_warn(2, start));
        assert!(!warnings.should_warn(2, start + Duration::from_secs(1)));
        assert!(warnings.should_warn(3, start + Duration::from_secs(1)));
        assert!(warnings.should_warn(2, start + WARN_WINDOW));
    }
}

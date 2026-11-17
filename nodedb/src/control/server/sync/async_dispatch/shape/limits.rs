// SPDX-License-Identifier: BUSL-1.1

//! Per-session bounds on shape subscriptions and snapshot reads.
//!
//! Every shape a session holds costs a registry entry, a snapshot read and a
//! per-delta check. A session holds at most [`MAX_SHAPES_PER_SESSION`] shapes.
//! Every snapshot read on a session's behalf (a `ShapeSubscribe`, a client
//! `ResyncRequest`, a server collection resync) draws from one bucket at the
//! rate [`snapshot_rate`] sets.

use crate::control::server::sync::rate_limit::RateLimitConfig;

/// The most shapes one session holds at once.
pub(super) const MAX_SHAPES_PER_SESSION: usize = 256;

/// Sustained snapshot reads per second one session causes at most.
const SNAPSHOTS_PER_SEC: f64 = 10.0;

/// Snapshot reads one session causes at most in a burst.
const SNAPSHOT_BURST: u32 = 32;

/// The token bucket a session's snapshot reads draw from.
pub(in crate::control::server::sync) fn snapshot_rate() -> RateLimitConfig {
    RateLimitConfig {
        rate_per_sec: SNAPSHOTS_PER_SEC,
        burst: SNAPSHOT_BURST,
    }
}

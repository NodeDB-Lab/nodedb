// SPDX-License-Identifier: BUSL-1.1

//! Typed errors for the segment quarantine subsystem.

use super::key::SegmentKey;

/// Errors produced by the quarantine registry.
#[derive(Debug, thiserror::Error, Clone)]
pub enum QuarantineError {
    /// The segment is quarantined after two consecutive CRC failures. Every
    /// later failing read of it in this process returns this error.
    #[error("segment quarantined: {segment} quarantined_at={quarantined_at_unix_ms}ms")]
    SegmentQuarantined {
        segment: SegmentKey,
        quarantined_at_unix_ms: u64,
    },
}

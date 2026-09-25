// SPDX-License-Identifier: BUSL-1.1

//! Stop the node when WAL catch-up cannot complete.

use crate::control::state::SharedState;
use crate::types::Lsn;

/// Is this the WAL reporting that the requested replay suffix was already
/// truncated away?
///
/// Every other replay failure is potentially transient (a partially written
/// active segment, a reader that cannot see O_DIRECT bytes); this one is not,
/// so the consumer routes it past the retry loop straight to fail-stop.
pub fn is_retained_floor_violation(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Wal(nodedb_wal::WalError::ReplayBelowRetainedFloor { .. })
    )
}

/// Fail-stop on an unrecoverable WAL catch-up failure.
///
/// `reason` states which failure class stopped the node; `attempts` is how many
/// replay attempts preceded it (zero for a failure that is unrecoverable on the
/// first observation and never retried).
///
/// `last_safe_lsn` is only observed for audit and logging; this path never
/// mutates or flushes it. Continuing without a recoverable WAL prefix would let
/// later event side effects overtake missing writes.
pub fn fail_stop_wal_catchup(
    core_id: usize,
    error: &impl std::fmt::Display,
    reason: &'static str,
    attempts: u32,
    last_safe_lsn: Lsn,
    shared_state: &SharedState,
    shutdown_bus: &crate::control::shutdown::ShutdownBus,
) {
    tracing::error!(
        core_id,
        error = %error,
        reason,
        attempts,
        last_safe_lsn = last_safe_lsn.as_u64(),
        "WAL catchup cannot complete; initiating fail-stop shutdown"
    );
    shared_state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        None,
        "event_plane",
        &format!(
            "event consumer core {core_id} WAL catchup stopped after {attempts} attempts at safe LSN {} ({reason}): {error}",
            last_safe_lsn.as_u64()
        ),
    );
    drop(shutdown_bus.initiate());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn max_wal_replay_failure_initiates_shutdown_without_advancing_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, watermark_store, shared_state, _trigger_dlq, _cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        watermark_store.save(0, Lsn::new(10)).unwrap();
        let (shutdown_bus, _) =
            crate::control::shutdown::ShutdownBus::new(Arc::clone(&shared_state.shutdown));

        fail_stop_wal_catchup(
            0,
            &"replay unavailable",
            "WAL catchup replay kept failing",
            10,
            Lsn::new(10),
            &shared_state,
            &shutdown_bus,
        );

        assert!(shared_state.shutdown.is_shutdown());
        assert_eq!(watermark_store.load(0).unwrap(), Lsn::new(10));
    }

    /// A truncated-away suffix is routed past the retry loop: it is not
    /// transient, and continuing would advance the watermark past events that
    /// were never dispatched.
    #[test]
    fn truncated_suffix_is_recognised_as_unrecoverable() {
        assert!(is_retained_floor_violation(&crate::Error::Wal(
            nodedb_wal::WalError::ReplayBelowRetainedFloor {
                from_lsn: 10,
                retained_floor_lsn: 4096,
                earliest_segment: "wal-00000000000000004096.seg".to_string(),
            }
        )));
        assert!(!is_retained_floor_violation(&crate::Error::Wal(
            nodedb_wal::WalError::Sealed
        )));
    }
}

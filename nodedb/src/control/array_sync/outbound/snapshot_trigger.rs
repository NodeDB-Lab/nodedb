//! Snapshot trigger: detect when a subscriber's cursor has fallen behind the
//! op-log GC boundary and initiate a server-driven catch-up.
//!
//! # When this fires
//!
//! After GC collapses ops below `min_ack_hlc`, any subscriber whose
//! `last_pushed_hlc < snapshot_hlc` can no longer receive an op stream.
//! The trigger enqueues a `RetentionFloor` `ArrayRejectMsg` so the Lite peer
//! knows to issue a `CatchupRequest`. Phase H serves that request.
//!
//! # Phase H note
//!
//! Full snapshot serving (building tile blobs and chunking) is implemented
//! in Phase H (`control::array_sync::catchup`). This module only detects
//! the need and signals it via a `RetentionFloor` reject. The Lite device
//! then issues `CatchupRequest` which Phase H handles.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::SyncMessageType;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};
use tracing::warn;

use super::delivery::{ArrayDeliveryRegistry, ArrayFrame};

/// Check whether a subscriber's cursor has fallen behind `snapshot_hlc`.
///
/// If so, enqueue a `RetentionFloor` `ArrayRejectMsg` to the session's
/// delivery channel. The Lite device interprets this as a signal to issue
/// a `CatchupRequest`, which Phase H will serve.
///
/// Returns `true` if the trigger fired (subscriber needs catch-up).
pub fn check_and_trigger(
    session_id: &str,
    array_name: &str,
    last_pushed_hlc: Hlc,
    snapshot_hlc: Hlc,
    delivery: &ArrayDeliveryRegistry,
) -> bool {
    if last_pushed_hlc >= snapshot_hlc {
        return false;
    }

    warn!(
        session = %session_id,
        array = %array_name,
        last_pushed = ?last_pushed_hlc,
        snapshot_boundary = ?snapshot_hlc,
        "array_outbound: subscriber cursor below snapshot boundary — triggering catch-up"
    );

    let reject = ArrayRejectMsg {
        array: array_name.to_string(),
        op_hlc_bytes: last_pushed_hlc.to_bytes(),
        reason: ArrayRejectReason::RetentionFloor,
        detail: format!(
            "subscriber cursor is below snapshot_hlc {:?}; issue CatchupRequest to resync",
            snapshot_hlc
        ),
    };

    if let Some(frame) = encode_reject(&reject) {
        delivery.enqueue(session_id, frame);
    }

    true
}

/// Encode an `ArrayRejectMsg` as a binary `SyncFrame`.
fn encode_reject(msg: &ArrayRejectMsg) -> Option<ArrayFrame> {
    nodedb_types::sync::wire::SyncFrame::try_encode(SyncMessageType::ArrayReject, msg)
        .map(|f| f.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::replica_id::ReplicaId;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    #[tokio::test]
    async fn no_trigger_when_cursor_at_boundary() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        let fired = check_and_trigger("s1", "arr", hlc(100), hlc(100), &reg);
        assert!(!fired);
        // Channel should be empty.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn triggers_when_cursor_below_boundary() {
        let reg = ArrayDeliveryRegistry::new();
        let mut rx = reg.register("s1".into());
        let fired = check_and_trigger("s1", "arr", hlc(50), hlc(100), &reg);
        assert!(fired);
        // A frame should be enqueued.
        let frame = rx.try_recv().expect("frame should be enqueued");
        assert!(!frame.is_empty());
    }
}

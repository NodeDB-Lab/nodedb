//! Per-subscriber HLC cursor helpers.
//!
//! Thin decision functions over [`super::subscriber_state::SubscriberMap`]
//! that answer "should this op be sent?" and record that it was.

use nodedb_array::sync::hlc::Hlc;

use super::subscriber_state::SubscriberMap;

/// Return `true` if `op_hlc` has not yet been delivered to the subscriber.
///
/// An op is eligible when `op_hlc > last_pushed_hlc`, i.e. it arrived
/// after the subscriber's watermark.
///
/// The HLC's total ordering (physical_ms, logical, replica_id) guarantees
/// this is correct under any clock skew.
pub fn should_send(op_hlc: Hlc, last_pushed_hlc: Hlc) -> bool {
    op_hlc > last_pushed_hlc
}

/// Advance the subscriber cursor to `op_hlc` and persist.
///
/// Delegates to `SubscriberMap::mark_sent`.
pub fn mark_sent(map: &SubscriberMap, session_id: &str, array_name: &str, op_hlc: Hlc) {
    map.mark_sent(session_id, array_name, op_hlc);
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::replica_id::ReplicaId;

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, ReplicaId::new(1)).unwrap()
    }

    #[test]
    fn should_send_above_watermark() {
        assert!(should_send(hlc(10), Hlc::ZERO));
        assert!(should_send(hlc(20), hlc(10)));
    }

    #[test]
    fn should_not_send_at_or_below_watermark() {
        assert!(!should_send(hlc(10), hlc(10)));
        assert!(!should_send(hlc(5), hlc(10)));
        assert!(!should_send(Hlc::ZERO, Hlc::ZERO));
    }
}

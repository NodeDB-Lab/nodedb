//! [`ArrayFanout`] — fan out an applied array op to all matching subscribers.
//!
//! Called by the post-apply observer hook (see `apply.rs`) after an
//! `ArrayOp` has been durably committed to the Data Plane and op-log.
//!
//! # Flow
//!
//! 1. Extract the op's coord as `Vec<u64>` for range matching.
//! 2. Query `ShapeRegistry::evaluate_array_mutation` to find matching
//!    `(session_id, shape_id)` pairs.
//! 3. For each matching session:
//!   - Look up the subscriber cursor.
//!   - Check `cursor::should_send` (skips already-delivered ops).
//!   - Check `snapshot_trigger::check_and_trigger` (pivots to catch-up if
//!     cursor is below the GC boundary).
//!   - Encode the op as `ArrayDeltaMsg` and enqueue to the session's
//!     delivery channel.
//!   - Advance the cursor via `cursor::mark_sent`.
//!
//! # Thread safety
//!
//! `ArrayFanout` is `Send + Sync` and is designed to be wrapped in an
//! `Arc` and shared between the inbound session task and the post-apply
//! observer.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use tracing::debug;

use crate::control::server::sync::shape::registry::ShapeRegistry;

use super::cursor;
use super::delivery::ArrayDeliveryRegistry;
use super::merge::MergerRegistry;
use super::snapshot_trigger;
use super::subscriber_state::SubscriberMap;

/// Observer trait: called after each op is durably applied.
///
/// Injected into `OriginApplyEngine` at construction. `ArrayFanout`
/// implements this, but tests can inject a mock.
pub trait ArrayApplyObserver: Send + Sync {
    fn on_op_applied(&self, op: &ArrayOp);
}

/// Fan-out coordinator for applied array ops.
pub struct ArrayFanout {
    /// Shape registry: maps (tenant, array, coord) → matched sessions.
    shapes: Arc<ShapeRegistry>,
    /// Per-session outbound frame channels.
    delivery: Arc<ArrayDeliveryRegistry>,
    /// Per-subscriber HLC cursors.
    cursors: Arc<SubscriberMap>,
    /// Per-array GC boundary HLC. Updated by the GC task; read here to
    /// decide when to trigger catch-up for lagging subscribers.
    snapshot_hlcs: Arc<RwLock<HashMap<String, Hlc>>>,
    /// Cross-shard merger registry for HLC-ordered multi-shard delivery.
    ///
    /// When a subscriber's `coord_range` spans multiple vShards, each shard
    /// independently calls `on_op_applied`. The merger buffers ops from all
    /// shards and drains them in HLC order before forwarding to the session's
    /// delivery channel.
    mergers: Arc<MergerRegistry>,
    /// vShard ID of the shard emitting this fanout instance's ops.
    ///
    /// Passed to the merger so it can track per-shard watermarks.
    shard_id: u16,
    /// Tenant scope for this fanout instance.
    ///
    /// Carries the same limitation as `OriginArrayInbound`: the tenant is
    /// resolved post-auth but the fanout is constructed pre-auth (to share
    /// the snapshot-assembly buffer lifetime). Until Phase I wires per-tenant
    /// fanout instances, `tenant_id = 0` means only tenant-0 array shape
    /// subscriptions receive fan-out. Single-tenant deployments are unaffected.
    tenant_id: u32,
}

impl ArrayFanout {
    /// Construct from shared components.
    pub fn new(
        shapes: Arc<ShapeRegistry>,
        delivery: Arc<ArrayDeliveryRegistry>,
        cursors: Arc<SubscriberMap>,
        snapshot_hlcs: Arc<RwLock<HashMap<String, Hlc>>>,
        mergers: Arc<MergerRegistry>,
        shard_id: u16,
        tenant_id: u32,
    ) -> Self {
        Self {
            shapes,
            delivery,
            cursors,
            snapshot_hlcs,
            mergers,
            shard_id,
            tenant_id,
        }
    }

    /// Remove a session's cursors, delivery channel, and merger buffers.
    ///
    /// Called from the listener on disconnect.
    pub fn remove_session(&self, session_id: &str) {
        self.cursors.remove_session(session_id);
        self.delivery.unregister(session_id);
        self.mergers.remove_session(session_id);
    }

    /// Fan out a single applied op to all subscribed sessions.
    fn fan_out_op(&self, op: &ArrayOp) {
        let coord_u64 = coord_to_u64(&op.coord);
        let matches =
            self.shapes
                .evaluate_array_mutation(self.tenant_id, &op.header.array, &coord_u64);

        if matches.is_empty() {
            return;
        }

        for (session_id, _shape_id) in matches {
            // Encoding is deferred to the merger, which encodes once per
            // delivery call rather than once per fan-out recipient.
            self.deliver_to_session(&session_id, op, op.header.hlc, &[]);
        }
    }

    /// Deliver one op to a single session via the multi-shard merger.
    ///
    /// The merger buffers ops from all vShards and drains them in HLC order,
    /// ensuring subscribers see a consistent stream regardless of which shard
    /// the op originated from.
    fn deliver_to_session(&self, session_id: &str, op: &ArrayOp, op_hlc: Hlc, _op_payload: &[u8]) {
        let cursor = match self.cursors.get(session_id, &op.header.array) {
            Some(c) => c,
            None => {
                // Session has not registered for this array yet — skip.
                return;
            }
        };

        // Check if this op has already been delivered.
        if !cursor::should_send(op_hlc, cursor.last_pushed_hlc) {
            debug!(
                session = %session_id,
                array = %op.header.array,
                op_hlc = ?op_hlc,
                "array_fanout: op already delivered, skipping"
            );
            return;
        }

        // Check if the subscriber cursor has fallen behind the GC boundary.
        let snapshot_hlc = self
            .snapshot_hlcs
            .read()
            .ok()
            .and_then(|m| m.get(&op.header.array).copied())
            .unwrap_or(Hlc::ZERO);

        if snapshot_trigger::check_and_trigger(
            session_id,
            &op.header.array,
            cursor.last_pushed_hlc,
            snapshot_hlc,
            &self.delivery,
        ) {
            // Subscriber needs catch-up — do not send op stream frames.
            return;
        }

        // Route through the multi-shard merger for HLC-ordered delivery.
        let merger = self.mergers.get_or_create(session_id, &op.header.array);
        merger.push_op(self.shard_id, op.clone(), &self.delivery);

        // Advance the cursor so subsequent ops from any shard know the
        // current frontier.
        cursor::mark_sent(&self.cursors, session_id, &op.header.array, op_hlc);
    }
}

impl ArrayApplyObserver for ArrayFanout {
    fn on_op_applied(&self, op: &ArrayOp) {
        self.fan_out_op(op);
    }
}

/// Extract coordinate components as `u64` for range-matching against
/// `ArrayCoordRange`.
///
/// `CoordValue::Int64` and `CoordValue::TimestampMs` are cast to `u64`
/// via bitwise reinterpretation (same bit pattern), which is correct for
/// non-negative index spaces. Negative values and `Float64`/`String`
/// coordinates are coerced to `u64::MAX` so they sort at the top of any
/// range and are delivered to unbounded subscriptions.
fn coord_to_u64(coord: &[nodedb_array::types::coord::value::CoordValue]) -> Vec<u64> {
    use nodedb_array::types::coord::value::CoordValue;
    coord
        .iter()
        .map(|c| match c {
            CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v as u64,
            CoordValue::Float64(v) => v.to_bits(),
            CoordValue::String(_) => u64::MAX,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::sync::shape::{ArrayCoordRange, ShapeDefinition, ShapeType};
    use std::sync::Arc;

    use crate::control::server::sync::shape::registry::ShapeRegistry;

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, replica()).unwrap()
    }

    fn make_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms),
                schema_hlc: hlc(1),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: None,
        }
    }

    fn make_fanout() -> (
        ArrayFanout,
        Arc<ShapeRegistry>,
        Arc<ArrayDeliveryRegistry>,
        Arc<SubscriberMap>,
    ) {
        use super::super::merge::MergerRegistry;
        use super::super::subscriber_state::SubscriberStore;
        let shapes = Arc::new(ShapeRegistry::new());
        let delivery = Arc::new(ArrayDeliveryRegistry::new());
        let store = SubscriberStore::in_memory().unwrap();
        let cursors = Arc::new(SubscriberMap::new(store));
        let snapshot_hlcs = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let mergers = Arc::new(MergerRegistry::new());
        let fanout = ArrayFanout::new(
            Arc::clone(&shapes),
            Arc::clone(&delivery),
            Arc::clone(&cursors),
            snapshot_hlcs,
            mergers,
            0,
            1,
        );
        (fanout, shapes, delivery, cursors)
    }

    #[tokio::test]
    async fn op_delivered_to_matching_subscriber() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        // Register subscriber.
        cursors.register("s1", "prices", None);
        let mut rx = delivery.register("s1".into());

        // Register shape subscription.
        shapes.subscribe(
            "s1",
            1,
            ShapeDefinition {
                shape_id: "sh1".into(),
                tenant_id: 1,
                shape_type: ShapeType::Array {
                    array_name: "prices".into(),
                    coord_range: None,
                },
                description: "all prices".into(),
                field_filter: vec![],
            },
        );

        let op = make_op("prices", 100);
        fanout.on_op_applied(&op);

        // Should have received one frame.
        let frame = rx.try_recv().expect("frame should be delivered");
        assert!(!frame.is_empty());
    }

    #[tokio::test]
    async fn op_not_delivered_to_wrong_array() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        cursors.register("s1", "prices", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            1,
            ShapeDefinition {
                shape_id: "sh1".into(),
                tenant_id: 1,
                shape_type: ShapeType::Array {
                    array_name: "other".into(),
                    coord_range: None,
                },
                description: "other".into(),
                field_filter: vec![],
            },
        );

        let op = make_op("prices", 100);
        fanout.on_op_applied(&op);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn op_not_delivered_when_coord_outside_range() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        cursors.register("s1", "mat", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            1,
            ShapeDefinition {
                shape_id: "sh1".into(),
                tenant_id: 1,
                shape_type: ShapeType::Array {
                    array_name: "mat".into(),
                    coord_range: Some(ArrayCoordRange {
                        start: vec![0],
                        end: Some(vec![9]),
                    }),
                },
                description: "narrow".into(),
                field_filter: vec![],
            },
        );

        // coord = [50], outside [0, 9]
        let op = ArrayOp {
            header: ArrayOpHeader {
                array: "mat".into(),
                hlc: hlc(200),
                schema_hlc: hlc(1),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: 200,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(50)],
            attrs: None,
        };
        fanout.on_op_applied(&op);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_op_not_redelivered() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        cursors.register("s1", "prices", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            1,
            ShapeDefinition {
                shape_id: "sh1".into(),
                tenant_id: 1,
                shape_type: ShapeType::Array {
                    array_name: "prices".into(),
                    coord_range: None,
                },
                description: "all".into(),
                field_filter: vec![],
            },
        );

        let op = make_op("prices", 100);
        fanout.on_op_applied(&op);
        let _first = rx.try_recv().expect("first delivery");

        // Replay same op — should not be re-delivered.
        fanout.on_op_applied(&op);
        assert!(rx.try_recv().is_err());
    }
}

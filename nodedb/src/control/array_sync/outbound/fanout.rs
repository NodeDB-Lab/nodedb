// SPDX-License-Identifier: BUSL-1.1

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
//!   - Route the op through the merger, which encodes and enqueues it.
//!   - The merger advances the cursor only after its frame is queued.
//!
//! # Thread safety
//!
//! `ArrayFanout` is `Send + Sync` and is designed to be wrapped in an
//! `Arc` and shared between the inbound session task and the post-apply
//! observer.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use tracing::debug;

use crate::control::server::sync::shape::{ShapeScope, registry::ShapeRegistry};

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
    /// Shape registry: maps (authenticated scope, array, coord) → matched sessions.
    shapes: Arc<ShapeRegistry>,
    /// Per-session outbound frame channels.
    delivery: Arc<ArrayDeliveryRegistry>,
    /// Per-subscriber HLC cursors.
    cursors: Arc<SubscriberMap>,
    /// Per-database/tenant/array GC boundary HLC. Updated by the GC task;
    /// read here to decide when to trigger catch-up for lagging subscribers.
    snapshot_hlcs: crate::control::array_sync::ArraySnapshotHlcs,
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
    /// Authenticated tenant and database scope for this fanout instance.
    scope: ShapeScope,
}

impl ArrayFanout {
    /// Construct from shared components.
    pub fn new(
        shapes: Arc<ShapeRegistry>,
        delivery: Arc<ArrayDeliveryRegistry>,
        cursors: Arc<SubscriberMap>,
        snapshot_hlcs: crate::control::array_sync::ArraySnapshotHlcs,
        mergers: Arc<MergerRegistry>,
        shard_id: u16,
        scope: ShapeScope,
    ) -> Self {
        Self {
            shapes,
            delivery,
            cursors,
            snapshot_hlcs,
            mergers,
            shard_id,
            scope,
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
        let matches = self
            .shapes
            .evaluate_array_mutation(self.scope, &op.header.array, &coord_u64);

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
        let cursor = match self.cursors.get_in_database(
            session_id,
            self.scope.database_id,
            self.scope.tenant_id,
            &op.header.array,
        ) {
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
            .and_then(|m| {
                m.get(&(
                    self.scope.database_id,
                    self.scope.tenant_id,
                    op.header.array.clone(),
                ))
                .copied()
            })
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
        let merger = self.mergers.get_or_create_with_cursors(
            session_id,
            self.scope.database_id,
            self.scope.tenant_id,
            &op.header.array,
            Arc::clone(&self.cursors),
        );
        merger.push_op(self.shard_id, op.clone(), &self.delivery);
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
    use nodedb_types::DatabaseId;
    use nodedb_types::sync::shape::{ArrayCoordRange, ShapeDefinition, ShapeType};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

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
        make_fanout_with_delivery(Arc::new(ArrayDeliveryRegistry::new()))
    }

    fn make_fanout_with_delivery(
        delivery: Arc<ArrayDeliveryRegistry>,
    ) -> (
        ArrayFanout,
        Arc<ShapeRegistry>,
        Arc<ArrayDeliveryRegistry>,
        Arc<SubscriberMap>,
    ) {
        use super::super::merge::MergerRegistry;
        use super::super::subscriber_state::SubscriberStore;
        let shapes = Arc::new(ShapeRegistry::new());
        let store = SubscriberStore::in_memory().unwrap();
        let cursors = Arc::new(SubscriberMap::new(store));
        let snapshot_hlcs: crate::control::array_sync::ArraySnapshotHlcs =
            Arc::new(RwLock::new(HashMap::new()));
        let mergers = Arc::new(MergerRegistry::new());
        let fanout = ArrayFanout::new(
            Arc::clone(&shapes),
            Arc::clone(&delivery),
            Arc::clone(&cursors),
            snapshot_hlcs,
            mergers,
            0,
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
        );
        (fanout, shapes, delivery, cursors)
    }

    #[tokio::test]
    async fn op_delivered_to_matching_subscriber() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        // Register subscriber.
        cursors.register_in_database("s1", DatabaseId::DEFAULT, 1, "prices", None);
        let mut rx = delivery.register("s1".into());

        // Register shape subscription.
        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
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
        assert!(!frame.bytes.is_empty());
        assert_eq!(frame.array, "prices");
    }

    #[tokio::test]
    async fn same_database_named_array_frontier_is_isolated_by_tenant() {
        use super::super::merge::MergerRegistry;
        use super::super::subscriber_state::SubscriberStore;
        use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

        let database_id = DatabaseId::new(1);
        let shapes = Arc::new(ShapeRegistry::new());
        let delivery = Arc::new(ArrayDeliveryRegistry::new());
        let store = SubscriberStore::in_memory().expect("in-memory subscriber store");
        let cursors = Arc::new(SubscriberMap::new(store));
        let snapshot_hlcs: crate::control::array_sync::ArraySnapshotHlcs = Arc::new(RwLock::new(
            HashMap::from([((database_id, 1, "prices".to_string()), hlc(100))]),
        ));
        let fanout = ArrayFanout::new(
            Arc::clone(&shapes),
            Arc::clone(&delivery),
            Arc::clone(&cursors),
            snapshot_hlcs,
            Arc::new(MergerRegistry::new()),
            0,
            ShapeScope {
                tenant_id: 2,
                database_id,
            },
        );

        cursors.register_in_database("s1", database_id, 2, "prices", None);
        let mut rx = delivery.register("s1".into());
        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 2,
                database_id,
            },
            ShapeDefinition {
                shape_id: "sh1".into(),
                tenant_id: 2,
                shape_type: ShapeType::Array {
                    array_name: "prices".into(),
                    coord_range: None,
                },
                description: "same database/name in another tenant".into(),
                field_filter: vec![],
            },
        );

        fanout.on_op_applied(&make_op("prices", 50));

        let frame = SyncFrame::from_bytes(&rx.try_recv().expect("delta should be delivered").bytes)
            .expect("decode delta frame");
        assert_eq!(frame.msg_type, SyncMessageType::ArrayDelta);
    }

    #[tokio::test]
    async fn op_not_delivered_to_wrong_array() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        cursors.register_in_database("s1", DatabaseId::DEFAULT, 1, "prices", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
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

        cursors.register_in_database("s1", DatabaseId::DEFAULT, 1, "mat", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
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
    async fn full_delivery_channel_does_not_skip_cursor_and_lags_session() {
        let (fanout, shapes, delivery, cursors) =
            make_fanout_with_delivery(Arc::new(ArrayDeliveryRegistry::with_test_capacity(1)));
        cursors.register_in_database("s1", DatabaseId::DEFAULT, 1, "prices", None);
        let mut rx = delivery.register("s1".into());
        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
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

        fanout.on_op_applied(&make_op("prices", 100));
        fanout.on_op_applied(&make_op("prices", 200));
        assert_eq!(
            cursors
                .get_in_database("s1", DatabaseId::DEFAULT, 1, "prices")
                .expect("cursor")
                .last_pushed_hlc,
            hlc(100),
            "a rejected frame must not advance past the missing op"
        );
        assert!(rx.try_recv().is_ok(), "the first frame was queued");

        fanout.on_op_applied(&make_op("prices", 300));
        assert!(
            rx.try_recv().is_err(),
            "lagged session rejects later frames"
        );
        assert_eq!(
            cursors
                .get_in_database("s1", DatabaseId::DEFAULT, 1, "prices")
                .expect("cursor")
                .last_pushed_hlc,
            hlc(100)
        );
    }

    #[tokio::test]
    async fn duplicate_op_not_redelivered() {
        let (fanout, shapes, delivery, cursors) = make_fanout();

        cursors.register_in_database("s1", DatabaseId::DEFAULT, 1, "prices", None);
        let mut rx = delivery.register("s1".into());

        shapes.subscribe(
            "s1",
            ShapeScope {
                tenant_id: 1,
                database_id: nodedb_types::DatabaseId::DEFAULT,
            },
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

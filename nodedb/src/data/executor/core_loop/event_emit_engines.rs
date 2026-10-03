// SPDX-License-Identifier: BUSL-1.1

//! KV and graph write events: the engine-shaped emitters over
//! [`CoreLoop::emit_event_with_row_id_as`].

use nodedb_query::msgpack_scan;

use super::CoreLoop;
use super::event_emit::{GraphEdgeEvent, RowWriteEvent};

/// Bundled arguments for [`CoreLoop::emit_kv_write_event_as`]. The bodies
/// are as the engine stores them.
pub(in crate::data::executor) struct KvWriteEvent<'a> {
    pub collection: &'a str,
    pub op: crate::event::WriteOp,
    pub key: &'a [u8],
    pub new_stored: Option<&'a [u8]>,
    pub old_stored: Option<&'a [u8]>,
}

impl CoreLoop {
    /// Emit a KV write event. Both row images are shaped into the
    /// `{key, value}` row every KV read returns (`msgpack_scan::kv_row_msgpack`),
    /// so the Event Plane decodes a KV row the same way as any other row.
    /// `new_stored` and `old_stored` are the bodies as the engine stores them.
    pub(in crate::data::executor) fn emit_kv_write_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        collection: &str,
        op: crate::event::WriteOp,
        key: &[u8],
        new_stored: Option<&[u8]>,
        old_stored: Option<&[u8]>,
    ) {
        self.emit_kv_write_event_as(
            task,
            task.request.event_source,
            KvWriteEvent {
                collection,
                op,
                key,
                new_stored,
                old_stored,
            },
        );
    }

    /// [`Self::emit_kv_write_event`] under `source` instead of the request's.
    pub(in crate::data::executor) fn emit_kv_write_event_as(
        &mut self,
        task: &super::super::task::ExecutionTask,
        source: crate::event::EventSource,
        event: KvWriteEvent<'_>,
    ) {
        let KvWriteEvent {
            collection,
            op,
            key,
            new_stored,
            old_stored,
        } = event;
        let key_str = String::from_utf8_lossy(key);
        let new_row = new_stored.map(|body| msgpack_scan::kv_row_msgpack(&key_str, body));
        let old_row = old_stored.map(|body| msgpack_scan::kv_row_msgpack(&key_str, body));
        let identity = crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref());
        self.emit_event_with_row_id_as(
            task,
            source,
            RowWriteEvent {
                collection,
                op,
                row_id: crate::event::types::RowId::row(identity),
                new_value: new_row.as_deref(),
                old_value: old_row.as_deref(),
                image_fault: None,
            },
        );
    }

    /// Emit a CDC write event for a node-label mutation on the nameable
    /// `__graph_node_labels__` stream ([`crate::event::graph_cdc::GRAPH_LABEL_STREAM`]).
    ///
    /// `SetNodeLabels` maps to [`crate::event::WriteOp::Insert`] with the added
    /// labels as `new_value`; `RemoveNodeLabels` maps to
    /// [`crate::event::WriteOp::Delete`] with the removed labels as `old_value`.
    /// The `row_id` is the (stable) node id. The label delta is serialized by
    /// [`crate::event::graph_cdc::graph_label_delta_value`] — the same encoder the
    /// WAL-replay path uses — so forward and replayed events are byte-identical.
    ///
    /// The node-label write is already durable at its WAL LSN by the time this
    /// runs (the record was appended in the Control Plane before dispatch), so we
    /// advance the core watermark to it — exactly as every other write chokepoint
    /// does via `note_write_lsn` — before emitting. That makes the forward
    /// event's LSN equal the WAL record's LSN the Event-Plane replay uses,
    /// satisfying watermark dedup.
    pub(in crate::data::executor) fn emit_graph_label_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        node_id: &str,
        labels: &[String],
        op: crate::event::WriteOp,
    ) {
        self.emit_graph_label_event_as(task, task.request.event_source, node_id, labels, op);
    }

    /// [`Self::emit_graph_label_event`] under `source` instead of the
    /// request's.
    pub(in crate::data::executor) fn emit_graph_label_event_as(
        &mut self,
        task: &super::super::task::ExecutionTask,
        source: crate::event::EventSource,
        node_id: &str,
        labels: &[String],
        op: crate::event::WriteOp,
    ) {
        // A committed-redo apply moves the watermark once the record settled.
        if self.redo_apply.scope.is_none()
            && let Some(lsn) = task.wal_lsn()
            && lsn > self.watermark
        {
            self.watermark = lsn;
        }
        let value = crate::event::graph_cdc::graph_label_delta_value(labels);
        let stream = crate::event::graph_cdc::GRAPH_LABEL_STREAM;
        let (new_value, old_value): (Option<&[u8]>, Option<&[u8]>) =
            if matches!(op, crate::event::WriteOp::Delete) {
                (None, Some(value.as_slice()))
            } else {
                (Some(value.as_slice()), None)
            };
        let identity = crate::engine::document::store::RowIdentity::from_user_key(node_id);
        self.emit_event_with_row_id_as(
            task,
            source,
            RowWriteEvent {
                collection: stream,
                op,
                row_id: crate::event::types::RowId::row(identity),
                new_value,
                old_value,
                image_fault: None,
            },
        );
    }

    /// Emit a CDC write event for a graph edge mutation on the edge's own
    /// `collection`. The stable `row_id` is composed from the `(src, label, dst)`
    /// identity triple via [`crate::event::graph_cdc::edge_row_id`] — the same
    /// composition the WAL-replay path uses — and carries the endpoints' bound
    /// surrogates from the plan.
    ///
    /// The caller MUST have advanced the core watermark to this edge's WAL LSN
    /// (via `note_edge_write_lsn`) before calling, so the forward event's LSN
    /// matches the WAL-replay reconstruction.
    pub(in crate::data::executor) fn emit_graph_edge_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        edge: GraphEdgeEvent<'_>,
    ) {
        let row_id = crate::event::types::RowId::edge(
            crate::event::types::EdgeEndpoints {
                src: edge.src_id,
                src_surrogate: edge.src_surrogate,
                dst: edge.dst_id,
                dst_surrogate: edge.dst_surrogate,
            },
            edge.label,
        );
        let (new_value, old_value): (Option<&[u8]>, Option<&[u8]>) =
            if matches!(edge.op, crate::event::WriteOp::Delete) {
                (None, None)
            } else {
                (edge.properties, None)
            };
        self.emit_event_with_row_id(task, edge.collection, edge.op, row_id, new_value, old_value);
    }
}

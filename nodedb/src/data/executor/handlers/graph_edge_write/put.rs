// SPDX-License-Identifier: BUSL-1.1

//! `EdgePut`: single-edge upsert, with optional transactional undo.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;

use super::shared::{EdgePutParams, owns_logical_edge_stats};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_edge_put(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
    ) -> Response {
        self.execute_edge_put_with_undo(task, params, None)
    }

    /// Edge upsert with optional transactional compensation.
    ///
    /// When `undo` is `Some`, the `UndoEntry::EdgeWrite` is recorded after
    /// the edge-store version is written and before the fallible CSR
    /// mutation. It names the version the put added, so a rollback removes
    /// exactly that version. An entry recorded before the store write would
    /// name a version that does not exist.
    ///
    /// A put writes a new edge-store version and makes the CSR edge live with
    /// the weight in `properties`, so a successful put always reports exactly
    /// one edge affected.
    pub(in crate::data::executor) fn execute_edge_put_with_undo(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
        undo: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        let EdgePutParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } = params;
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge put");
        let database_id = task.request.database_id.as_u64();

        if self.is_node_deleted(database_id, tid, src_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: src_id.to_string(),
                },
            );
        }
        if self.is_node_deleted(database_id, tid, dst_id) {
            return self.response_error(
                task,
                ErrorCode::RejectedDanglingEdge {
                    missing_node: dst_id.to_string(),
                },
            );
        }

        let ord = self
            .active_graph_system_from
            .unwrap_or_else(|| self.hlc.next_ordinal());
        // Under a Calvin batch, `epoch_system_ms` is the deterministic epoch
        // timestamp; outside Calvin (every path today — no Calvin edge writes
        // yet) it is None and we fall back to the HLC-derived wall time,
        // identical to the prior behavior.
        let valid_from_ms = match self.epoch_system_ms {
            Some(ms) => ms,
            None => nodedb_types::ordinal_to_ms(ord),
        };
        let target = EdgeTarget {
            database_id,
            tid,
            collection,
            src_id,
            label,
            dst_id,
        };
        // The CSR state the undo puts back, read only when an undo is kept.
        let csr_prior = undo.is_some().then(|| self.capture_edge_csr(&target));
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.put_edge_version_recorded(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            )
            .with_surrogates(src_surrogate, dst_surrogate),
            properties,
            ord,
            valid_from_ms,
            i64::MAX,
            owns_logical_edge_stats(task, src_id),
        ) {
            Ok(version) => {
                // Edge-store version is now durable; the compensation entry is
                // valid from here on even if the CSR mutation below fails.
                if let (Some(undo), Some(csr)) = (undo, csr_prior) {
                    undo.push(UndoEntry::EdgeWrite(Box::new(target.undo(version, csr))));
                }
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                let partition = self.csr_partition_mut(database_id, tid);
                let csr_result =
                    partition.put_edge_in_collection(src_id, label, dst_id, collection, weight);
                match csr_result {
                    Ok(_) => {
                        // Populate the per-node surrogates so future bitmap-gated
                        // traversals can check membership without a separate lookup.
                        partition.set_node_surrogate(src_id, src_surrogate);
                        partition.set_node_surrogate(dst_id, dst_surrogate);
                        self.checkpoint_coordinator.mark_dirty("sparse", 1);
                        self.note_edge_write_lsn(task, tid, collection, src_id, label, dst_id);
                        // CDC: emit after `note_edge_write_lsn` so the core
                        // watermark (the event's LSN) already reflects this
                        // edge's WAL LSN, matching the WAL-replay reconstruction.
                        self.emit_graph_edge_event(
                            task,
                            crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                                collection,
                                src_id,
                                label,
                                dst_id,
                                op: crate::event::WriteOp::Insert,
                                properties: Some(properties),
                            },
                        );
                        self.response_affected(task, 1)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::event::WriteOp;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::types::Lsn;
    use nodedb_types::Surrogate;

    use super::super::shared::test_support::{affected_count, make_core, make_task_with_lsn};

    #[test]
    fn edge_put_emits_cdc_insert_on_its_collection() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let task = make_task_with_lsn(77);
        let resp = h.core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: b"w=1",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 1, "a put always affects one edge");

        let event = consumers[0]
            .try_recv()
            .expect("edge put must emit a CDC WriteEvent");
        assert_eq!(event.collection.as_ref(), "knows");
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(
            event.lsn,
            Lsn::new(77),
            "event LSN matches the edge's WAL LSN"
        );
        assert_eq!(event.new_value.as_deref(), Some(b"w=1".as_slice()));
    }
}

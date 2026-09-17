// SPDX-License-Identifier: BUSL-1.1

//! Edge write handlers: EdgePut, EdgePutBatch, EdgeDelete, EdgeDeleteBatch.
//!
//! Split out of `graph.rs` to keep that file under the file-size limit; see
//! its module doc for the scoping rules that also apply here.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, VShardId};

/// Dual-homed edges are physically present on both endpoint homes. The source
/// home is the canonical owner of logical graph cardinality, so only that
/// participant updates persistent stats counters.
pub(in crate::data::executor) fn owns_logical_edge_stats(
    task: &ExecutionTask,
    src_id: &str,
) -> bool {
    task.request.vshard_id == VShardId::from_key(src_id.as_bytes())
}

/// Bundled arguments for [`CoreLoop::execute_edge_put`].
pub(in crate::data::executor) struct EdgePutParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    pub properties: &'a [u8],
    pub src_surrogate: nodedb_types::Surrogate,
    pub dst_surrogate: nodedb_types::Surrogate,
}

/// Bundled arguments for [`CoreLoop::execute_edge_delete`].
pub(in crate::data::executor) struct EdgeDeleteParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    /// Compiled RLS write-policy filters the plan carried.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

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
    /// When `undo` is `Some`, the `UndoEntry::PutEdge` is recorded at the one
    /// correct point: *after* the edge-store version is durably written and
    /// *before* the fallible CSR mutation. Recording it earlier (before the
    /// dangling-endpoint validation or the edge-store write) would leave a
    /// compensation entry for an operation that never touched storage — on
    /// rollback that entry would soft-delete or re-insert a version that never
    /// existed, corrupting bitemporal edge history.
    pub(in crate::data::executor) fn execute_edge_put_with_undo(
        &mut self,
        task: &ExecutionTask,
        params: EdgePutParams<'_>,
        undo: Option<&mut Vec<crate::data::executor::handlers::transaction::undo::UndoEntry>>,
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

        // Capture the pre-image only when a compensation record is requested.
        let old_properties = if undo.is_some() {
            self.edge_store
                .get_edge(
                    database_id,
                    TenantId::new(tid),
                    collection,
                    src_id,
                    label,
                    dst_id,
                )
                .ok()
                .flatten()
        } else {
            None
        };

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
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.put_edge_versioned_with_stats(
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
            Ok(()) => {
                // Edge-store version is now durable; the compensation entry is
                // valid from here on even if the CSR mutation below fails.
                if let Some(undo) = undo {
                    undo.push(
                        crate::data::executor::handlers::transaction::undo::UndoEntry::PutEdge {
                            collection: collection.to_string(),
                            src_id: src_id.to_string(),
                            label: label.to_string(),
                            dst_id: dst_id.to_string(),
                            old_properties,
                        },
                    );
                }
                let weight = crate::engine::graph::csr::extract_weight_from_properties(properties);
                let partition = self.csr_partition_mut(database_id, tid);
                let csr_result = if weight != 1.0 {
                    partition
                        .add_edge_weighted_in_collection(src_id, label, dst_id, collection, weight)
                } else {
                    partition.add_edge_in_collection(src_id, label, dst_id, collection)
                };
                match csr_result {
                    Ok(()) => {
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
                        self.response_ok(task)
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

    /// Apply a batched edge insert in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        let database_id = task.request.database_id.as_u64();
        for (idx, edge) in edges.iter().enumerate() {
            if self.is_node_deleted(database_id, tid, &edge.src_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.src_id.clone(),
                    },
                );
            }
            if self.is_node_deleted(database_id, tid, &edge.dst_id) {
                return self.response_error(
                    task,
                    ErrorCode::RejectedDanglingEdge {
                        missing_node: edge.dst_id.clone(),
                    },
                );
            }
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_versioned_with_stats(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                )
                .with_surrogates(edge.src_surrogate, edge.dst_surrogate),
                &[],
                ord,
                valid_from_ms,
                i64::MAX,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(()) => {
                    let partition = self.csr_partition_mut(database_id, tid);
                    if let Err(e) = partition.add_edge_in_collection(
                        &edge.src_id,
                        &edge.label,
                        &edge.dst_id,
                        edge.collection.as_str(),
                    ) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("edge {idx} (label interning): {e}"),
                            },
                        );
                    }
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("edge {idx}: {e}"),
                        },
                    );
                }
            }
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.note_edge_write_lsn(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            );
            // CDC: batch edges are applied with empty properties (see
            // `execute_edge_put_batch`'s hardcoded `&[]`), so `new_value` is an
            // empty payload — a faithful pre-image of what was applied.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    op: crate::event::WriteOp::Insert,
                    properties: Some(&[]),
                },
            );
        }
        self.response_ok(task)
    }

    /// Apply a batched edge delete in a single SPSC round-trip.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(
            core = self.core_id,
            count = edges.len(),
            "edge delete batch"
        );
        let database_id = task.request.database_id.as_u64();
        for edge in edges {
            let ord = self
                .active_graph_system_from
                .unwrap_or_else(|| self.hlc.next_ordinal());
            use crate::engine::graph::edge_store::EdgeRef;
            let _ = self.edge_store.soft_delete_edge_with_stats(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                ord,
                owns_logical_edge_stats(task, &edge.src_id),
            );
            let partition = self.csr_partition_mut(database_id, tid);
            partition.remove_edge_in_collection(
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                edge.collection.as_str(),
            );
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.note_edge_write_lsn(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            );
            // CDC: one Delete event per edge on the edge's own collection.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    op: crate::event::WriteOp::Delete,
                    properties: None,
                },
            );
        }
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_edge_delete(
        &mut self,
        task: &ExecutionTask,
        params: EdgeDeleteParams<'_>,
    ) -> Response {
        self.execute_edge_delete_with_undo(task, params, None)
    }

    /// Edge delete with optional transactional compensation.
    ///
    /// The `UndoEntry::DeleteEdge` is recorded only when a live pre-image
    /// existed *and* the tombstone was durably written — never speculatively
    /// before the write. A phantom entry would otherwise re-insert an edge that
    /// was never deleted when the surrounding transaction rolls back.
    ///
    /// The RLS write policy is decided against that same pre-image and BEFORE
    /// the tombstone: the row a policy governs is the edge that exists now, and
    /// a delete the policy rejects must leave the edge in place.
    pub(in crate::data::executor) fn execute_edge_delete_with_undo(
        &mut self,
        task: &ExecutionTask,
        params: EdgeDeleteParams<'_>,
        undo: Option<&mut Vec<crate::data::executor::handlers::transaction::undo::UndoEntry>>,
    ) -> Response {
        let EdgeDeleteParams {
            tid,
            collection,
            src_id,
            label,
            dst_id,
            rls_write_check,
        } = params;
        debug!(core = self.core_id, tid, %collection, %src_id, %label, %dst_id, "edge delete");
        let database_id = task.request.database_id.as_u64();

        // Capture the pre-image when a compensation record is requested, or
        // when a write policy has to be decided against it. An ungoverned
        // delete with no undo log reads nothing, so the common path pays no
        // extra lookup.
        let old_properties = if undo.is_some()
            || !matches!(
                rls_write_check.decision(),
                nodedb_types::WriteGateDecision::AdmitAll
            ) {
            self.edge_store
                .get_edge(
                    database_id,
                    TenantId::new(tid),
                    collection,
                    src_id,
                    label,
                    dst_id,
                )
                .ok()
                .flatten()
        } else {
            None
        };

        if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_edge_properties(
            rls_write_check,
            old_properties.as_deref(),
            tid,
            collection,
        ) {
            return self.response_error(task, error);
        }

        let ord = self
            .active_graph_system_from
            .unwrap_or_else(|| self.hlc.next_ordinal());
        use crate::engine::graph::edge_store::EdgeRef;
        match self.edge_store.soft_delete_edge_with_stats(
            EdgeRef::new(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                src_id,
                label,
                dst_id,
            ),
            ord,
            owns_logical_edge_stats(task, src_id),
        ) {
            Ok(_) => {
                // Tombstone is durable; record the compensation for a rollback.
                if let (Some(undo), Some(props)) = (undo, old_properties) {
                    undo.push(
                        crate::data::executor::handlers::transaction::undo::UndoEntry::DeleteEdge {
                            collection: collection.to_string(),
                            src_id: src_id.to_string(),
                            label: label.to_string(),
                            dst_id: dst_id.to_string(),
                            old_properties: props,
                        },
                    );
                }
                let partition = self.csr_partition_mut(database_id, tid);
                partition.remove_edge_in_collection(src_id, label, dst_id, collection);
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                self.note_edge_write_lsn(task, tid, collection, src_id, label, dst_id);
                // CDC: emit after `note_edge_write_lsn` so the event LSN matches
                // this edge's WAL LSN (the WAL-replay reconstruction key).
                self.emit_graph_edge_event(
                    task,
                    crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                        collection,
                        src_id,
                        label,
                        dst_id,
                        op: crate::event::WriteOp::Delete,
                        properties: None,
                    },
                );
                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Record a committed edge write's version, keyed by the edge's
    /// `(src, label, dst)` identity, if a WAL LSN was threaded onto the task.
    fn note_edge_write_lsn(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        src_id: &str,
        label: &str,
        dst_id: &str,
    ) {
        let Some(lsn) = task.wal_lsn() else {
            return;
        };
        self.note_write_lsn(
            task.request.database_id,
            TenantId::new(tid),
            collection,
            Some(
                crate::data::executor::core_loop::write_index::KeyRepr::Edge {
                    src: Box::from(src_id),
                    label: Box::from(label),
                    dst: Box::from(dst_id),
                },
            ),
            lsn,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::data::executor::core_loop::CoreLoop;
    use crate::event::WriteOp;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::GraphOp;
    use std::time::{Duration, Instant};

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// A task carrying `wal_lsn` so the edge handlers advance the watermark to
    /// it — the LSN the emitted CDC event then carries. The `plan` field is
    /// unused by the edge handlers (they take params directly).
    fn make_task_with_lsn(lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: "x".to_string(),
                edge_label: None,
                direction: nodedb_graph::Direction::Out,
                rls_filters: Vec::new(),
                collection: None,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: Some(Lsn::new(lsn)),
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

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
                src_surrogate: nodedb_types::Surrogate::new(1),
                dst_surrogate: nodedb_types::Surrogate::new(2),
            },
        );
        assert_eq!(resp.status, Status::Ok);

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

    /// Compiled filters equivalent to a `FOR WRITE` policy on `owner`.
    fn owner_write_check(owner: &str) -> Vec<u8> {
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    /// The delete is decided against the edge's STORED property object before
    /// the tombstone is written, so a rejected delete leaves the edge in place.
    #[test]
    fn edge_delete_rejected_by_the_write_policy_leaves_the_edge() {
        let mut h = make_core();
        let put_task = make_task_with_lsn(90);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: br#"{"owner":"alice"}"#,
                        src_surrogate: nodedb_types::Surrogate::new(1),
                        dst_surrogate: nodedb_types::Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );

        let check = nodedb_types::RlsWriteCheck::Predicate(owner_write_check("mallory"));
        let del_task = make_task_with_lsn(91);
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &check,
            },
        );
        assert_eq!(resp.status, Status::Error);
        assert!(
            h.core
                .edge_store
                .get_edge(
                    DatabaseId::DEFAULT.as_u64(),
                    TenantId::new(1),
                    "knows",
                    "a",
                    "KNOWS",
                    "b",
                )
                .expect("read edge back")
                .is_some(),
            "a refused delete must leave the edge present"
        );
    }

    /// …and a policy the stored properties satisfy lets the delete through.
    #[test]
    fn edge_delete_admitted_by_the_write_policy_applies() {
        let mut h = make_core();
        let put_task = make_task_with_lsn(92);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: br#"{"owner":"alice"}"#,
                        src_surrogate: nodedb_types::Surrogate::new(1),
                        dst_surrogate: nodedb_types::Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );

        let check = nodedb_types::RlsWriteCheck::Predicate(owner_write_check("alice"));
        let del_task = make_task_with_lsn(93);
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &check,
            },
        );
        assert_eq!(resp.status, Status::Ok);
    }

    #[test]
    fn edge_delete_emits_cdc_delete_on_its_collection() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        // Seed the edge so the delete has something to remove.
        let put_task = make_task_with_lsn(80);
        assert_eq!(
            h.core
                .execute_edge_put(
                    &put_task,
                    EdgePutParams {
                        tid: 1,
                        collection: "knows",
                        src_id: "a",
                        label: "KNOWS",
                        dst_id: "b",
                        properties: b"",
                        src_surrogate: nodedb_types::Surrogate::new(1),
                        dst_surrogate: nodedb_types::Surrogate::new(2),
                    },
                )
                .status,
            Status::Ok
        );
        let _ = consumers[0].try_recv(); // drain the put event

        let del_task = make_task_with_lsn(81);
        // This test exercises CDC emission on delete, not the RLS write gate,
        // so it carries no policy — mirrors the old empty-slice "admits
        // everything" convention.
        let no_policy = nodedb_types::RlsWriteCheck::NoPolicyApplies;
        let resp = h.core.execute_edge_delete(
            &del_task,
            EdgeDeleteParams {
                tid: 1,
                collection: "knows",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                rls_write_check: &no_policy,
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let event = consumers[0]
            .try_recv()
            .expect("edge delete must emit a CDC WriteEvent");
        assert_eq!(event.collection.as_ref(), "knows");
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(event.op, WriteOp::Delete);
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Shared param structs and helpers for the edge write handlers.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{RecordHomes, TenantId};

/// Dual-homed edges are physically present on both endpoint homes. The source
/// home is the canonical owner of logical graph cardinality, so only that
/// participant updates persistent stats counters.
pub(in crate::data::executor) fn owns_logical_edge_stats(
    task: &ExecutionTask,
    src_id: &str,
) -> bool {
    task.request.vshard_id == RecordHomes::edge_owner(src_id)
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
    /// The endpoints' bound identities, carried onto the delete's CDC event.
    pub src_surrogate: nodedb_types::Surrogate,
    pub dst_surrogate: nodedb_types::Surrogate,
    /// Compiled RLS write-policy filters the plan carried.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// The system-time ordinal an edge write stamps: the decided ordinal of
    /// the record in apply, or a fresh local one. The local clock folds a
    /// decided ordinal in, so a later local write stamps above it.
    pub(in crate::data::executor) fn graph_write_ordinal(&self) -> i64 {
        match self.apply_scope.graph_system_from {
            Some(ord) => {
                self.hlc.update_from_remote(ord);
                ord
            }
            None => self.hlc.next_ordinal(),
        }
    }

    /// The system time and applied ordinal an edge write stamps: the
    /// ordinal of [`Self::graph_write_ordinal`], applied at the record's
    /// applied ordinal when it carries one. A record whose applied ordinal
    /// is below its system time is refused.
    pub(in crate::data::executor) fn graph_write_stamp(
        &self,
    ) -> crate::Result<crate::engine::graph::edge_store::VersionStamp> {
        let ord = self.graph_write_ordinal();
        crate::engine::graph::edge_store::VersionStamp::applied_at(
            ord,
            self.apply_scope.graph_applied,
        )
    }

    /// Bring the CSR edge `(src, label, dst)` of `collection` to what the
    /// edge store resolves it to after a write: live with the weight in
    /// `current`, or absent.
    pub(in crate::data::executor) fn mirror_edge_csr(
        &mut self,
        database_id: u64,
        tid: u64,
        edge: (&str, &str, &str),
        collection: &str,
        current: Option<&[u8]>,
    ) -> Result<(), nodedb_graph::GraphError> {
        let (src_id, label, dst_id) = edge;
        let weight = current.map(crate::engine::graph::csr::extract_weight_from_properties);
        self.csr_partition_mut(database_id, tid)
            .restore_edge_in_collection(src_id, label, dst_id, collection, weight)
    }

    /// Record a committed edge write's version, keyed by the edge's
    /// `(src, label, dst)` identity, if a WAL LSN was threaded onto the task.
    pub(in crate::data::executor) fn note_edge_write_lsn(
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

/// Fixtures shared by the inline test modules in `put.rs` and `delete.rs`.
#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan, Priority, Request};
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::GraphOp;
    use std::time::{Duration, Instant};

    pub struct CoreHarness {
        pub core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    pub fn make_core() -> CoreHarness {
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
    pub fn make_task_with_lsn(lsn: u64) -> crate::data::executor::task::ExecutionTask {
        crate::data::executor::task::ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Graph(GraphOp::Neighbors {
                node_id: "x".to_string(),
                edge_labels: Vec::new(),
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
            commit_hlc: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// Extract the `affected` count a successful edge write's payload carries.
    pub fn affected_count(resp: &crate::bridge::envelope::Response) -> u64 {
        crate::control::server::shared::sql::staging_predicates::require_affected_count(
            resp.payload.as_bytes(),
        )
        .expect("edge write response must carry an affected count")
    }
}

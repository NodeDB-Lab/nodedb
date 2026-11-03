// SPDX-License-Identifier: BUSL-1.1

//! The graph edge write counters a core reports.
//!
//! An edge write reaches the edge handlers three ways:
//!
//! * A live autocommit write, through the graph dispatch. It counts at once.
//! * A committed transaction's install, through the graph replay arm with a
//!   committed-redo apply open. The install pass holds its counts on the
//!   apply scope. They publish once the record settled, so a rolled-back
//!   install counts nothing.
//! * Restart replay, through the same arm with no apply open. It re-applies
//!   writes an earlier process counted, so it counts nothing.
//!
//! A dual-homed edge is applied on both endpoint homes. Only the source
//! home counts it, the same rule the persistent graph stats follow.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::graph::graph_edge_write::owns_logical_edge_stats;
use crate::data::executor::handlers::transaction::redo_apply::RedoApplyPass;
use crate::data::executor::task::ExecutionTask;

/// Edge writes counted but not yet published to the system metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::data::executor) struct EdgeWriteTally {
    /// Edge versions a put wrote.
    pub written: u64,
    /// Live edges a delete removed.
    pub deleted: u64,
}

impl EdgeWriteTally {
    fn add(&mut self, other: EdgeWriteTally) {
        self.written = self.written.saturating_add(other.written);
        self.deleted = self.deleted.saturating_add(other.deleted);
    }
}

impl CoreLoop {
    /// Count one edge version a put of `src_id`'s edge wrote.
    pub(in crate::data::executor) fn count_edge_written(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
    ) {
        self.count_edge_write(
            task,
            src_id,
            EdgeWriteTally {
                written: 1,
                deleted: 0,
            },
        );
    }

    /// Count one live edge a delete of `src_id`'s edge removed.
    pub(in crate::data::executor) fn count_edge_removed(
        &mut self,
        task: &ExecutionTask,
        src_id: &str,
    ) {
        self.count_edge_write(
            task,
            src_id,
            EdgeWriteTally {
                written: 0,
                deleted: 1,
            },
        );
    }

    fn count_edge_write(&mut self, task: &ExecutionTask, src_id: &str, tally: EdgeWriteTally) {
        if !owns_logical_edge_stats(task, src_id) {
            return;
        }
        // The validate pass writes nothing, so only the install pass reaches
        // a handler. Its counts wait for the record to settle.
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            if scope.pass == RedoApplyPass::Install {
                scope.edge_writes.add(tally);
            }
            return;
        }
        // A decided ordinal with no committed-redo apply open is restart
        // replay: the write it applies again was counted when it ran.
        if self.apply_scope.graph_system_from.is_some() {
            return;
        }
        self.publish_edge_writes(tally);
    }

    /// Add `tally` to the system metrics. A committed-redo install calls
    /// this once its record settled.
    pub(in crate::data::executor) fn publish_edge_writes(&self, tally: EdgeWriteTally) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        if tally.written > 0 {
            metrics.record_graph_edges_written(tally.written);
        }
        if tally.deleted > 0 {
            metrics.record_graph_edges_deleted(tally.deleted);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use nodedb_wal::{TombstoneSet, WalRecord};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Response, Status};
    use crate::control::metrics::SystemMetrics;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
    use crate::data::executor::handlers::graph::{EdgeDeleteParams, EdgePutParams};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{RecordHomes, TenantId, VShardId};
    use crate::wal::{RedoRecord, RedoSubRecord};

    const TID: u64 = 1;
    const COLLECTION: &str = "knows";
    const LABEL: &str = "KNOWS";

    struct Harness {
        core: CoreLoop,
        metrics: Arc<SystemMetrics>,
        _req_tx: nodedb_bridge::buffer::Producer<BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, req_tx, resp_rx) = make_core_with_dir(dir.path());
        let metrics = Arc::new(SystemMetrics::new());
        core.set_metrics(Arc::clone(&metrics));
        Harness {
            core,
            metrics,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    impl Harness {
        fn written(&self) -> u64 {
            self.metrics.graph_edges_written.load(Ordering::Relaxed)
        }

        fn deleted(&self) -> u64 {
            self.metrics.graph_edges_deleted.load(Ordering::Relaxed)
        }

        fn put(&mut self, task: &ExecutionTask, src: &str, dst: &str) -> Response {
            self.core.execute_edge_put(
                task,
                EdgePutParams {
                    tid: TID,
                    collection: COLLECTION,
                    src_id: src,
                    label: LABEL,
                    dst_id: dst,
                    properties: &[],
                    src_surrogate: Surrogate::new(1),
                    dst_surrogate: Surrogate::new(2),
                    valid_from_ms: None,
                    valid_until_ms: None,
                },
            )
        }

        fn delete(&mut self, task: &ExecutionTask, src: &str, dst: &str) -> Response {
            self.core.execute_edge_delete(
                task,
                EdgeDeleteParams {
                    tid: TID,
                    collection: COLLECTION,
                    src_id: src,
                    label: LABEL,
                    dst_id: dst,
                    src_surrogate: Surrogate::new(1),
                    dst_surrogate: Surrogate::new(2),
                    rls_write_check: &RlsWriteCheck::NoPolicyApplies,
                },
            )
        }

        fn out_edges(&self, src: &str) -> usize {
            self.core
                .edge_store
                .neighbors_out(
                    DatabaseId::DEFAULT.as_u64(),
                    TenantId::new(TID),
                    COLLECTION,
                    src,
                    None,
                )
                .expect("neighbors_out")
                .len()
        }
    }

    /// A task on the home that owns `src`'s edges.
    fn owner_task(src: &str) -> ExecutionTask {
        task_with_vshard(RecordHomes::edge_owner(src))
    }

    /// A task on a home that holds `src`'s edges without owning them: the
    /// destination home of a dual-homed edge.
    fn peer_task(src: &str) -> ExecutionTask {
        let owner = RecordHomes::edge_owner(src).as_u32();
        task_with_vshard(VShardId::new((owner + 1) % VShardId::COUNT))
    }

    fn batch_edge(src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            src_id: src.to_string(),
            label: LABEL.to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
            properties: Vec::new(),
        }
    }

    fn put_plan(src: &str, dst: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            src_id: src.to_string(),
            label: LABEL.to_string(),
            dst_id: dst.to_string(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        })
    }

    #[test]
    fn a_live_put_and_delete_count_on_the_source_home_only() {
        let mut h = harness();

        assert_eq!(h.put(&owner_task("a"), "a", "b").status, Status::Ok);
        assert_eq!(h.written(), 1);
        // The destination home applies the same edge and counts nothing.
        assert_eq!(h.put(&peer_task("a"), "a", "b").status, Status::Ok);
        assert_eq!(h.written(), 1);

        assert_eq!(h.delete(&owner_task("a"), "a", "b").status, Status::Ok);
        assert_eq!(h.deleted(), 1);
        // A delete of an edge that is already gone removes nothing.
        assert_eq!(h.delete(&owner_task("a"), "a", "b").status, Status::Ok);
        assert_eq!(h.deleted(), 1);
        assert_eq!(h.written(), 1);
    }

    #[test]
    fn a_batch_counts_each_edge_written_and_each_live_edge_removed() {
        let mut h = harness();
        let task = owner_task("a");

        let put = h.core.execute_edge_put_batch(
            &task,
            TID,
            &[batch_edge("a", "b"), batch_edge("a", "c")],
        );
        assert_eq!(put.status, Status::Ok);
        assert_eq!(h.written(), 2);

        let delete = h.core.execute_edge_delete_batch(
            &task,
            TID,
            &[batch_edge("a", "b"), batch_edge("a", "x")],
        );
        assert_eq!(delete.status, Status::Ok);
        assert_eq!(h.deleted(), 1, "only the live edge counts as removed");
        assert_eq!(h.written(), 2);
    }

    #[test]
    fn a_committed_transaction_counts_its_edges_once_the_install_settles() {
        let mut h = harness();
        let task = owner_task("a");

        let response =
            h.core
                .commit_plans_for_test(&task, TID, &[put_plan("a", "b"), put_plan("a", "c")], 40);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(h.out_edges("a"), 2);
        assert_eq!(h.written(), 2);
    }

    #[test]
    fn a_rolled_back_install_counts_nothing() {
        let mut h = harness();
        let task = owner_task("a");

        let response =
            h.core
                .commit_plans_then_refuse_for_test(&task, TID, &[put_plan("a", "b")], 41);
        assert_eq!(response.status, Status::Error);
        assert_eq!(h.out_edges("a"), 0, "the install rolled the edge back");
        assert_eq!(h.written(), 0);
    }

    #[test]
    fn restart_replay_counts_nothing() {
        let mut h = harness();
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: COLLECTION.to_string(),
            src_id: "a".to_string(),
            label: LABEL.to_string(),
            dst_id: "b".to_string(),
            properties: Vec::new(),
            src_surrogate: 1,
            dst_surrogate: 2,
            system_from: Some(nodedb_types::ms_to_ordinal_upper(100)),
            applied: None,
            valid_from_ms: None,
            valid_until_ms: None,
        })
        .expect("encode edge put");
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload,
            }],
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        };
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: RecordHomes::edge_owner("a").as_u32(),
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        h.core
            .replay_all_wal(std::slice::from_ref(&record), 1, &TombstoneSet::new())
            .expect("replay");

        assert_eq!(h.out_edges("a"), 1, "replay applied the edge");
        assert_eq!(h.written(), 0, "replay re-applies a write counted before");
    }
}

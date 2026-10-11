// SPDX-License-Identifier: BUSL-1.1

//! `NodeEdgeGuard` and `NodePresenceGuard`: the graph ops a document delete
//! uses to tombstone the edges of its collection on both homes.
//!
//! A CRDT document delete tombstones the edges of a node only when its
//! document is stored. The planner reads which documents are, and a
//! presence guard on the collection's vShard refuses the transaction when
//! one appeared or vanished since.
//!
//! A node delete's planner reads the node's incident edges and deletes each
//! one in the delete's transaction. The guard runs on the node's key home,
//! which holds every incident edge, and refuses the transaction when the
//! edges it finds differ from the ones the planner read.
//!
//! TRUNCATE of an edge-bearing collection is one Calvin transaction with a
//! `TruncateEdges` on every vShard. Each copy's resolve records a cut of the
//! collection at the transaction's ordinal (see `wal_replay_redo_graph_cut`).

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{BatchEdge, GraphOp, PhysicalPlan};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Whether the live edges of `collection` incident on `node_id` are
    /// exactly `expected`.
    pub(in crate::data::executor) fn node_edge_guard_holds(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        node_id: &str,
        expected: &[BatchEdge],
    ) -> crate::Result<bool> {
        let live: BTreeSet<(String, String, String)> = self
            .edge_store
            .live_edges_of_node(database_id, TenantId::new(tid), collection, node_id)?
            .into_iter()
            .collect();
        let planned: BTreeSet<(String, String, String)> = expected
            .iter()
            .filter(|edge| edge.collection.as_str() == collection)
            .map(|edge| (edge.src_id.clone(), edge.label.clone(), edge.dst_id.clone()))
            .collect();
        Ok(live == planned && planned.len() == expected.len())
    }

    /// Whether every node delete guard among `plans` holds on this core.
    /// A session COMMIT checks them against the state it commits on.
    pub(in crate::data::executor) fn node_edge_guards_hold(
        &self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) -> crate::Result<bool> {
        let database_id = task.request.database_id.as_u64();
        for plan in plans {
            if let PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
                collection,
                node_id,
                expected,
            }) = plan
                && !self.node_edge_guard_holds(
                    database_id,
                    tid,
                    collection.as_str(),
                    node_id,
                    expected,
                )?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Answer `Ok` when the guard holds, and `OllpRetryRequired` when the
    /// node's edges changed since the planner read them. Writes nothing.
    pub(in crate::data::executor) fn execute_node_edge_guard(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        node_id: &str,
        expected: &[BatchEdge],
    ) -> Response {
        match self.node_edge_guard_holds(
            task.request.database_id.as_u64(),
            tid,
            collection,
            node_id,
            expected,
        ) {
            Ok(true) => self.response_ok(task),
            Ok(false) => self.response_error(task, ErrorCode::OllpRetryRequired),
            Err(error) => self.response_error(task, error),
        }
    }

    /// The ids among `ids` that are stored CRDT documents of `collection`.
    fn stored_crdt_documents<'a>(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        ids: impl Iterator<Item = &'a String>,
    ) -> crate::Result<BTreeSet<String>> {
        let engine = self.get_crdt_engine(task.request.database_id, task.request.tenant_id)?;
        Ok(ids
            .filter(|id| engine.row_exists(collection, id))
            .cloned()
            .collect())
    }

    /// Whether every CRDT presence guard among `plans` holds on this core:
    /// exactly its `present` ids are stored. A session COMMIT checks them
    /// against the state it commits on.
    pub(in crate::data::executor) fn node_presence_guards_hold(
        &mut self,
        task: &ExecutionTask,
        plans: &[PhysicalPlan],
    ) -> crate::Result<bool> {
        for plan in plans {
            if let PhysicalPlan::Graph(GraphOp::NodePresenceGuard {
                collection,
                present,
                absent,
                ..
            }) = plan
                && !self.presence_holds(task, collection.as_str(), present, absent)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn presence_holds(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        present: &[String],
        absent: &[String],
    ) -> crate::Result<bool> {
        let stored =
            self.stored_crdt_documents(task, collection, present.iter().chain(absent.iter()))?;
        let claimed: BTreeSet<String> = present.iter().cloned().collect();
        Ok(stored == claimed)
    }

    /// Answer `Ok` when the presence guard holds, and `OllpRetryRequired`
    /// when a document appeared or vanished since the planner read them.
    /// Writes nothing.
    pub(in crate::data::executor) fn execute_node_presence_guard(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        present: &[String],
        absent: &[String],
    ) -> Response {
        match self.presence_holds(task, collection, present, absent) {
            Ok(true) => self.response_ok(task),
            Ok(false) => self.response_error(task, ErrorCode::OllpRetryRequired),
            Err(error) => self.response_error(task, error),
        }
    }

    /// The planner's read before a presence guard: the stored ids among
    /// `ids`, as a msgpack array of strings.
    pub(in crate::data::executor) fn execute_node_presence_read(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        ids: &[String],
    ) -> Response {
        let stored = match self.stored_crdt_documents(task, collection, ids.iter()) {
            Ok(stored) => stored,
            Err(error) => return self.response_error(task, error),
        };
        let stored: Vec<String> = stored.into_iter().collect();
        match zerompk::to_msgpack_vec(&stored) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("node presence read: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::super::graph::EdgeDeleteParams;
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::graph::edge_store::EdgeRef;
    use crate::types::RecordHomes;

    const TID: u64 = 1;

    /// A stable non-zero surrogate per node name, as a real edge write binds.
    fn bound(node: &str) -> Surrogate {
        Surrogate::new(
            1 + node.bytes().fold(0u32, |acc, b| {
                acc.wrapping_mul(31).wrapping_add(u32::from(b))
            }),
        )
    }

    fn seed(core: &mut CoreLoop, src: &str, dst: &str, ordinal: i64) {
        core.edge_store
            .put_edge_versioned(
                EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(TID), "g", src, "L", dst)
                    .with_surrogates(bound(src), bound(dst)),
                b"{}",
                ordinal,
                0,
                i64::MAX,
            )
            .expect("seed edge");
    }

    fn batch_edge(src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: src.into(),
            label: "L".into(),
            dst_id: dst.into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    /// The guard holds only for the exact set of live incident edges: an
    /// edge added or missing after the planner read refuses it.
    #[test]
    fn the_guard_holds_only_for_the_edges_the_planner_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core, "alice", "bob", 10);
        seed(&mut core, "carol", "alice", 11);
        seed(&mut core, "dave", "erin", 12);
        let db = DatabaseId::DEFAULT.as_u64();
        let read = [batch_edge("alice", "bob"), batch_edge("carol", "alice")];
        assert!(
            core.node_edge_guard_holds(db, TID, "g", "alice", &read)
                .expect("guard")
        );
        assert!(
            !core
                .node_edge_guard_holds(db, TID, "g", "alice", &read[..1])
                .expect("guard"),
            "an edge the planner did not read refuses the guard"
        );
        seed(&mut core, "alice", "frank", 13);
        assert!(
            !core
                .node_edge_guard_holds(db, TID, "g", "alice", &read)
                .expect("guard"),
            "an edge added after the read refuses the guard"
        );
    }

    fn task_on(vshard: crate::types::VShardId) -> ExecutionTask {
        use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
        use crate::types::{ReadConsistency, RequestId, TraceId};
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: vshard,
            plan: PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::Compact),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// Delete `src -> dst` of `g` at the transaction ordinal `ordinal`, as a
    /// Calvin resolve applies a node delete's `EdgeDelete`. Returns the
    /// tombstone's ordinal.
    fn delete_at(
        core: &mut CoreLoop,
        home: crate::types::VShardId,
        src: &str,
        dst: &str,
        ordinal: i64,
    ) -> Option<i64> {
        core.apply_scope.graph_system_from = Some(ordinal);
        let check = nodedb_types::RlsWriteCheck::decided_earlier_in_request();
        let response = core.execute_edge_delete(
            &task_on(home),
            EdgeDeleteParams {
                tid: TID,
                collection: "g",
                src_id: src,
                label: "L",
                dst_id: dst,
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
                rls_write_check: &check,
            },
        );
        core.apply_scope.graph_system_from = None;
        assert_eq!(response.status, Status::Ok, "edge delete: {response:?}");
        response
            .write_set
            .iter()
            .find_map(|entry| match &entry.effect {
                crate::bridge::envelope::RowEffect::Edge(
                    crate::bridge::envelope::EdgeImage::Delete(delete),
                ) => delete.system_from,
                _ => None,
            })
    }

    /// A node delete whose incident edge's other endpoint homes on another
    /// vShard and core: each home checks the node's guard against the same
    /// read, then applies the edge's delete at the transaction's ordinal.
    /// Both homes stamp one version key, and the node has no live edge left
    /// on either.
    #[test]
    fn a_node_delete_tombstones_a_cross_shard_edge_on_both_homes_at_one_key() {
        const TXN_ORDINAL: i64 = 7_000;
        let peer = (0..)
            .map(|i| format!("peer{i}"))
            .find(|peer| {
                crate::types::VShardId::from_key(peer.as_bytes())
                    != crate::types::VShardId::from_key(b"alice")
            })
            .unwrap_or_default();
        let homes = RecordHomes::edge("alice", &peer);
        let db = DatabaseId::DEFAULT.as_u64();
        let read = [batch_edge("alice", &peer)];
        let mut stamped = Vec::new();
        for home in [homes.owner(), homes.second()] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            seed(&mut core, "alice", &peer, 10);
            assert!(
                core.node_edge_guard_holds(db, TID, "g", "alice", &read)
                    .expect("guard"),
                "the guard holds against the read"
            );
            stamped.push(delete_at(&mut core, home, "alice", &peer, TXN_ORDINAL));
            assert!(
                core.edge_store
                    .live_edges_of_node(db, TenantId::new(TID), "g", "alice")
                    .expect("live edges")
                    .is_empty(),
                "the edge is gone on this home"
            );
        }
        assert_eq!(stamped[0], Some(TXN_ORDINAL));
        assert_eq!(stamped[0], stamped[1], "both homes stamp one version key");
    }

    /// One core holds both homes of an edge. The peer home's participant of
    /// a node delete flushes the edge's delete before the node's home
    /// resolves. The Calvin resolve checks no guard, so it resolves. A
    /// session COMMIT checks its guards, and there the same state refuses.
    #[test]
    fn a_calvin_resolve_ignores_its_own_delete_from_the_peer_home() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let peer = (0..)
            .map(|i| format!("peer{i}"))
            .find(|peer| {
                crate::types::VShardId::from_key(peer.as_bytes())
                    != crate::types::VShardId::from_key(b"alice")
            })
            .unwrap_or_default();
        seed(&mut core, "alice", &peer, 10);
        let guard = PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            node_id: "alice".into(),
            expected: vec![batch_edge("alice", &peer)],
        });
        let peer_home = crate::types::VShardId::from_key(peer.as_bytes());
        delete_at(&mut core, peer_home, "alice", &peer, 7_000);

        let home = task_on(crate::types::VShardId::from_key(b"alice"));
        let txn = crate::types::TxnId::new(71);
        let calvin = core.execute_resolve_txn(&home, TID, txn, std::slice::from_ref(&guard));
        assert_eq!(calvin.status, Status::Ok, "{calvin:?}");
        let session = core.execute_resolve_session_txn(&home, TID, txn, &[guard]);
        assert_eq!(
            session.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
    }

    /// An edge insert that commits between the planner's read and the node
    /// delete refuses the delete's guard with a retry. The retry reads the
    /// node's edges again, the guard holds, and the delete covers the inserted
    /// edge too.
    #[test]
    fn an_edge_inserted_after_the_read_refuses_the_guard_until_read_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let db = DatabaseId::DEFAULT.as_u64();
        let home = crate::types::VShardId::from_key(b"alice");
        seed(&mut core, "alice", "bob", 10);
        let first_read = [batch_edge("alice", "bob")];

        // The racing insert commits before the delete's transaction runs.
        seed(&mut core, "alice", "carol", 20);
        let refused = core.execute_node_edge_guard(&task_on(home), TID, "g", "alice", &first_read);
        assert_eq!(refused.status, Status::Error);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "a drifted read asks for a retry"
        );

        // The retry's read names the inserted edge, and its delete covers it.
        let second_read: Vec<BatchEdge> = core
            .edge_store
            .live_edges_of_node(db, TenantId::new(TID), "g", "alice")
            .expect("live edges")
            .into_iter()
            .map(|(src, _, dst)| batch_edge(&src, &dst))
            .collect();
        assert_eq!(second_read.len(), 2);
        let held = core.execute_node_edge_guard(&task_on(home), TID, "g", "alice", &second_read);
        assert_eq!(held.status, Status::Ok, "{held:?}");
        for edge in &second_read {
            delete_at(&mut core, home, &edge.src_id, &edge.dst_id, 30);
        }
        assert!(
            core.node_edge_guard_holds(db, TID, "g", "alice", &[])
                .expect("guard"),
            "no edge of the node is left live"
        );
    }
}

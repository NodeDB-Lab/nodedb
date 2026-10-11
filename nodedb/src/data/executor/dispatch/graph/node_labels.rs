// SPDX-License-Identifier: BUSL-1.1

//! Node-label set and removal on a graph partition.
//!
//! The CSR keeps the labels of a node as one `u64` bitset. Each distinct
//! node-label name in a partition owns one bit, so a partition holds at most
//! [`NODE_LABEL_LIMIT`] distinct node labels. A set that needs a label past
//! that limit is refused whole, and nothing of it stays applied.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// The most distinct node labels one graph partition holds: one bit each in
/// the `u64` label bitset of the CSR.
pub const NODE_LABEL_LIMIT: usize = u64::BITS as usize;

impl CoreLoop {
    /// Run a `SetNodeLabels` op and emit its CDC event.
    pub(in crate::data::executor::dispatch) fn dispatch_set_node_labels(
        &mut self,
        task: &ExecutionTask,
        database_id: u64,
        tid: u64,
        node_id: &str,
        labels: &[String],
    ) -> Response {
        if let Err(code) = self.set_node_labels(database_id, tid, node_id, labels) {
            return self.response_error(task, code);
        }
        // CDC: a node-label set is an Insert on the nameable node-label
        // stream, with the labels as `new_value`.
        self.emit_graph_label_event(task, node_id, labels, crate::event::WriteOp::Insert);
        // A set interns the node when it is new, so it touches exactly one node.
        self.response_affected(task, 1)
    }

    /// Run a `RemoveNodeLabels` op and emit its CDC event.
    pub(in crate::data::executor::dispatch) fn dispatch_remove_node_labels(
        &mut self,
        task: &ExecutionTask,
        database_id: u64,
        tid: u64,
        node_id: &str,
        labels: &[String],
    ) -> Response {
        let partition = self.csr_partition_mut(database_id, tid);
        // Read before the removals: `remove_node_label` does not report
        // whether the node exists.
        let existed = partition.contains_node(node_id);
        for label in labels {
            partition.remove_node_label(node_id, label);
        }
        // CDC: a node-label removal is a Delete on the nameable node-label
        // stream, with the removed labels as `old_value`.
        self.emit_graph_label_event(task, node_id, labels, crate::event::WriteOp::Delete);
        self.response_affected(task, u64::from(existed))
    }

    /// Set every label of `labels` on `node_id`, or none of them.
    ///
    /// A label the node already carries is a no-op. A label that needs a bit
    /// past [`NODE_LABEL_LIMIT`] refuses the whole set with
    /// `ErrorCode::NodeLabelLimit`. The labels already set, the label names
    /// interned and the node interned by this call are then rolled back. A
    /// rollback that fails answers `ErrorCode::RollbackFailed`.
    pub(in crate::data::executor) fn set_node_labels(
        &mut self,
        database_id: u64,
        tid: u64,
        node_id: &str,
        labels: &[String],
    ) -> Result<(), ErrorCode> {
        // The partition reports the limit only from `add_node_label`, and its
        // distinct-label count is not public. So the prior state is captured
        // first, and a refused set restores it.
        let undo = self.capture_node_labels_undo(database_id, tid, node_id, labels);
        for label in labels {
            let refusal = match self
                .csr_partition_mut(database_id, tid)
                .add_node_label(node_id, label)
            {
                // `true` also covers a label the node already carries.
                Ok(true) => continue,
                // `false` means only one thing: the label is not interned and
                // every bit of the bitset is taken.
                Ok(false) => ErrorCode::NodeLabelLimit {
                    node: node_id.to_string(),
                    label: label.clone(),
                    limit: NODE_LABEL_LIMIT,
                },
                Err(e) => ErrorCode::from(crate::Error::from(e)),
            };
            self.rollback_undo_log(database_id, tid, vec![undo])
                .map_err(ErrorCode::from)?;
            return Err(refusal);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::event::WriteOp;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::GraphOp;
    use std::time::{Duration, Instant};

    const TID: u64 = 1;

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

    fn make_task_with_lsn(op: GraphOp, lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Graph(op),
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
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    fn db() -> u64 {
        DatabaseId::DEFAULT.as_u64()
    }

    fn set_labels(node: &str, labels: &[&str]) -> GraphOp {
        GraphOp::SetNodeLabels {
            node_id: node.to_string(),
            labels: labels.iter().map(|l| l.to_string()).collect(),
        }
    }

    /// Intern `count` distinct labels `L0..` on the node `filler`.
    fn fill_labels(core: &mut CoreLoop, count: usize) {
        let partition = core.csr_partition_mut(db(), TID);
        for i in 0..count {
            let added = partition
                .add_node_label("filler", &format!("L{i}"))
                .expect("label filler");
            assert!(added, "label L{i} fits under the limit");
        }
    }

    /// The labels `node` carries, sorted.
    fn labels_of(core: &CoreLoop, node: &str) -> Vec<String> {
        let partition = core.csr_partition(db(), TID).expect("partition");
        let id = partition.node_id_raw(node).expect("node exists");
        let mut labels: Vec<String> = partition
            .node_labels(id)
            .into_iter()
            .map(str::to_string)
            .collect();
        labels.sort();
        labels
    }

    fn affected(resp: &Response) -> u64 {
        crate::control::server::shared::sql::staging_predicates::require_affected_count(
            resp.payload.as_bytes(),
        )
        .expect("a node-label op reports an affected count")
    }

    #[test]
    fn set_node_labels_emits_cdc_on_nameable_label_stream() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let op = set_labels("alice", &["Person"]);
        let task = make_task_with_lsn(op.clone(), 88);
        let resp = h.core.dispatch_graph(&task, &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected(&resp), 1, "a set touches exactly one node");

        let event = consumers[0]
            .try_recv()
            .expect("SetNodeLabels must emit a CDC WriteEvent");
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM,
            "node-label CDC uses the nameable stream, not the NUL sentinel"
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(88));
    }

    #[test]
    fn remove_node_labels_emits_cdc_delete() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));

        let op = GraphOp::RemoveNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        };
        let task = make_task_with_lsn(op.clone(), 89);
        let resp = h.core.dispatch_graph(&task, &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            affected(&resp),
            0,
            "'alice' was never labeled, so there is no node to remove from"
        );

        let event = consumers[0]
            .try_recv()
            .expect("RemoveNodeLabels must emit a CDC WriteEvent");
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Delete);
        assert!(event.old_value.is_some(), "removed labels ride old_value");
    }

    #[test]
    fn label_past_the_limit_refuses_the_set_and_applies_nothing() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 64);
        let mut h = make_core();
        h.core
            .set_event_producer(producers.pop().expect("producer"));
        // One bit stays free after `L0..L61` and the node's own `Seed`.
        fill_labels(&mut h.core, NODE_LABEL_LIMIT - 2);
        h.core
            .csr_partition_mut(db(), TID)
            .add_node_label("alice", "Seed")
            .expect("label alice");

        // `L1` is interned, `Fresh` takes the last bit, `Over` has none.
        let op = set_labels("alice", &["L1", "Fresh", "Over"]);
        let task = make_task_with_lsn(op.clone(), 90);
        let resp = h.core.dispatch_graph(&task, &op);

        assert_eq!(resp.status, Status::Error);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::NodeLabelLimit {
                node: "alice".into(),
                label: "Over".into(),
                limit: NODE_LABEL_LIMIT,
            })
        );
        assert_eq!(labels_of(&h.core, "alice"), vec!["Seed".to_string()]);
        let partition = h.core.csr_partition(db(), TID).expect("partition");
        assert!(
            !partition.has_node_label_name("Fresh"),
            "the bit the refused set took is free again"
        );
        assert!(
            consumers[0].try_recv().is_none(),
            "a refused set emits no CDC"
        );

        // The freed bit takes a label again.
        let op = set_labels("alice", &["Fresh"]);
        let resp = h
            .core
            .dispatch_graph(&make_task_with_lsn(op.clone(), 91), &op);
        assert_eq!(resp.status, Status::Ok);
    }

    #[test]
    fn label_past_the_limit_on_a_new_node_leaves_no_node() {
        let mut h = make_core();
        fill_labels(&mut h.core, NODE_LABEL_LIMIT - 1);

        let op = set_labels("bob", &["Fresh", "Over"]);
        let resp = h
            .core
            .dispatch_graph(&make_task_with_lsn(op.clone(), 92), &op);

        assert_eq!(resp.status, Status::Error);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::NodeLabelLimit { label, .. }) if label == "Over"
        ));
        let partition = h.core.csr_partition(db(), TID).expect("partition");
        assert!(
            !partition.contains_node("bob"),
            "the refused set interned no node"
        );
        assert!(!partition.has_node_label_name("Fresh"));
    }

    #[test]
    fn repeated_label_is_a_noop_success() {
        let mut h = make_core();
        let op = set_labels("alice", &["Person"]);
        let resp = h
            .core
            .dispatch_graph(&make_task_with_lsn(op.clone(), 93), &op);
        assert_eq!(resp.status, Status::Ok);

        let op = set_labels("alice", &["Person", "Person"]);
        let resp = h
            .core
            .dispatch_graph(&make_task_with_lsn(op.clone(), 94), &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected(&resp), 1);
        assert_eq!(labels_of(&h.core, "alice"), vec!["Person".to_string()]);
    }

    #[test]
    fn repeated_label_at_the_limit_is_a_noop_success() {
        let mut h = make_core();
        fill_labels(&mut h.core, NODE_LABEL_LIMIT);

        let op = set_labels("filler", &["L0", "L63", "L0"]);
        let resp = h
            .core
            .dispatch_graph(&make_task_with_lsn(op.clone(), 95), &op);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(labels_of(&h.core, "filler").len(), NODE_LABEL_LIMIT);
    }
}

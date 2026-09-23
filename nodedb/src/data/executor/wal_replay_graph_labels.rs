// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for graph node-label mutations (`GraphNodeLabelSet` /
//! `GraphNodeLabelRemove`, autocommit path).
//!
//! Unlike graph edges — which are durable via redb's synchronous commit at
//! apply time and rebuilt into the CSR from the `EdgeStore` at
//! `CoreLoop::open()` (`engine::graph::csr::rebuild::rebuild_sharded_from_store`,
//! called BEFORE any WAL replay runs) — the node-label bitset
//! (`CsrIndex::node_label_bits`) has no redb-backed durability at all. A WAL
//! record is its only durable backing, so it needs its own standalone replay
//! pass; graph otherwise has none (see `wal/redo/replay.rs`'s comment on why
//! edges only replay via `TransactionRedo` reconstitution).
//!
//! ## Ordering
//!
//! Because the CSR is fully rebuilt from the durable `EdgeStore` before this
//! pass runs, every node that has ever had an edge already exists in the CSR
//! by the time this function is called — no ordering dependency on an
//! earlier WAL record.
//!
//! ## Replay reproduces live, it does not narrow it
//!
//! A `GraphNodeLabelSet` record can only exist in the WAL because a live
//! `SetNodeLabels` (`data::executor::dispatch::graph`) already ran and already
//! called `CsrIndex::add_node_label`, which vivifies its node argument via
//! `ensure_node` — labeling a node with no edges yet is a supported, successful
//! live operation. Replay therefore calls the exact same `add_node_label` /
//! `remove_node_label` unconditionally, with no `contains_node` precondition:
//! reproducing that vivification on restart is not "interning a phantom node",
//! it is recreating the state that legitimately existed before the crash.
//! `add_node_label`'s `Err` (node-id space exhausted) is still logged and
//! skipped rather than propagated as a panic; its `Ok(false)` (64-distinct-label
//! bitset limit) is treated the same as the live handler treats it — ignored,
//! since the live handler discards the returned bool on `Ok` and only reacts to
//! `Err`. `remove_node_label` never vivifies (it no-ops on an unknown node or
//! unknown label), so it was already identical to live and needs no change.

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use crate::types::DatabaseId;

impl CoreLoop {
    /// Try to decode+replay one record as a graph node-label mutation.
    ///
    /// Returns `None` when `record` is not a `GraphNodeLabelSet` /
    /// `GraphNodeLabelRemove` record (caller skips), `Some(0)` when it is one
    /// of ours but was not applied (malformed payload, or a typed error from
    /// the live handler such as node-id space exhaustion — always logged,
    /// never a panic), or `Some(1)` on successful replay.
    pub(crate) fn try_replay_graph_node_label(
        &mut self,
        record: &WalRecord,
        database_id: DatabaseId,
    ) -> Option<usize> {
        let record_type = RecordType::from_raw(record.logical_record_type())?;
        let is_set = record_type == RecordType::GraphNodeLabelSet;
        let is_remove = record_type == RecordType::GraphNodeLabelRemove;
        if !is_set && !is_remove {
            return None;
        }

        let tenant_id = record.header.tenant_id;
        let record_lsn = record.header.lsn;

        let Ok((node_id, labels)) = zerompk::from_msgpack::<(String, Vec<String>)>(&record.payload)
        else {
            self.replay_record_rejected(
                "graph",
                record_lsn,
                None,
                "malformed graph node-label WAL record",
            );
            return Some(0);
        };
        if self.claim_for_validation() {
            return Some(0);
        }
        if self.recording_redo_undo() {
            let undo =
                self.capture_node_labels_undo(database_id.as_u64(), tenant_id, &node_id, &labels);
            self.record_redo_undo([undo]);
        }

        if is_set {
            for label in &labels {
                // `add_node_label` vivifies `node_id` via `ensure_node` exactly
                // as the live `SetNodeLabels` handler does. `Ok(false)` (the
                // 64-distinct-label bitset limit) is discarded here, mirroring
                // the live handler, which also never inspects the returned
                // bool on `Ok`.
                let added = self
                    .csr_partition_mut(database_id.as_u64(), tenant_id)
                    .add_node_label(&node_id, label);
                if let Err(e) = added {
                    self.replay_record_rejected(
                        "graph",
                        record_lsn,
                        None,
                        &format!("setting label '{label}' on node '{node_id}' failed: {e}"),
                    );
                    return Some(0);
                }
            }
        } else {
            let partition = self.csr_partition_mut(database_id.as_u64(), tenant_id);
            // `remove_node_label` no-ops on an unknown node or unknown label —
            // it never vivifies, so calling it unconditionally is already
            // identical to the live `RemoveNodeLabels` handler.
            for label in &labels {
                partition.remove_node_label(&node_id, label);
            }
        }
        Some(1)
    }

    /// Replay every `GraphNodeLabelSet` / `GraphNodeLabelRemove` record in
    /// `records`, routing each through [`CoreLoop::try_replay_graph_node_label`].
    ///
    /// Called once during startup, after the CSR has been rebuilt from the
    /// `EdgeStore` (in `CoreLoop::open`) and before the event loop starts.
    /// Node-label records carry no `collection` field (labels are
    /// tenant/database-scoped, not collection-scoped — see
    /// `GraphOp::SetNodeLabels`), so there is nothing to check against
    /// `CollectionTombstoned` tombstones here, unlike the other per-engine
    /// replay passes.
    pub(crate) fn replay_graph_node_label_wal(&mut self, records: &[WalRecord], num_cores: usize) {
        let mut replayed = 0usize;

        for record in records {
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let database_id = DatabaseId::new(record.header.database_id);
            if let Some(applied) = self.try_replay_graph_node_label(record, database_id) {
                replayed += applied;
            }
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL graph node-label replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_wal::WalRecord;
    use nodedb_wal::WalRecordArgs;
    use nodedb_wal::record::RecordType;

    use super::CoreLoop;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::GraphOp;

    const TID: u64 = 7;

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
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

    fn has_label(core: &CoreLoop, node_id: &str, label: &str) -> bool {
        let Some(partition) = core.csr_partition(DatabaseId::DEFAULT.as_u64(), TID) else {
            return false;
        };
        let Some(id) = partition.node_id(node_id) else {
            return false;
        };
        partition.node_has_label(id.raw(partition.partition_tag()), label)
    }

    #[test]
    fn autocommit_set_node_labels_produces_durable_lsn() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append set node labels");
        assert!(
            outcome.lsn.is_some(),
            "autocommit GraphOp::SetNodeLabels must be durably WAL-appended \
             (pre-fix: it fell through the catch-all and was never logged)"
        );
    }

    #[test]
    fn autocommit_remove_node_labels_produces_durable_lsn() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append remove node labels");
        assert!(
            outcome.lsn.is_some(),
            "autocommit GraphOp::RemoveNodeLabels must be durably WAL-appended \
             (pre-fix: it fell through the catch-all and was never logged)"
        );
    }

    #[test]
    fn replay_from_empty_sets_label_on_existing_node() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        });
        wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        // The node already exists in the CSR here (as it would after the
        // redb-backed edge rebuild for a node that also has edges); replay
        // still applies the label unconditionally regardless of prior
        // existence.
        h.core
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node("alice")
            .expect("seed node");

        h.core.replay_graph_node_label_wal(&records, 1);

        assert!(
            has_label(&h.core, "alice", "Person"),
            "label must be present after replay from empty \
             (pre-fix: SetNodeLabels was never WAL-logged, so nothing to replay)"
        );
    }

    #[test]
    fn remove_after_set_leaves_label_absent_after_replay() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let set_plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        });
        let remove_plan = PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
            node_id: "alice".to_string(),
            labels: vec!["Person".to_string()],
        });
        for plan in [&set_plan, &remove_plan] {
            wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        // Seeding is incidental here (this test is about Set-then-Remove
        // ordering, not vivification) — replay would create "alice" from the
        // Set record regardless of whether it is pre-seeded.
        h.core
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node("alice")
            .expect("seed node");

        h.core.replay_graph_node_label_wal(&records, 1);

        assert!(
            !has_label(&h.core, "alice", "Person"),
            "label must be absent after Set-then-Remove replay \
             (proves ordering and that Remove is logged too)"
        );
    }

    #[test]
    fn label_on_edgeless_node_survives_replay() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "ghost".to_string(),
            labels: vec!["Person".to_string()],
        });
        wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        // No node seeded — "ghost" never had an edge, so the redb-backed CSR
        // rebuild never created it. The live `SetNodeLabels` handler still
        // vivifies a never-edged node via `ensure_node` (this WAL record only
        // exists because that live call already succeeded), so replay must
        // reproduce the same vivification rather than drop the label.
        h.core.replay_graph_node_label_wal(&records, 1);

        let partition = h.core.csr_partition(DatabaseId::DEFAULT.as_u64(), TID);
        assert!(
            partition.is_some_and(|p| p.contains_node("ghost")),
            "replaying a label on a never-edged node must vivify the node, \
             matching the live handler"
        );
        assert!(
            has_label(&h.core, "ghost", "Person"),
            "the label itself must be set after replay, not just the node created"
        );
    }

    #[test]
    fn malformed_payload_does_not_panic() {
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::GraphNodeLabelSet as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: 0,
            database_id: 0,
            payload: vec![0xff, 0xff, 0xff],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let mut h = make_core();
        h.core
            .replay_graph_node_label_wal(std::slice::from_ref(&record), 1);
        // No panic is the assertion.
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for graph node-label mutations (`GraphNodeLabelSet` /
//! `GraphNodeLabelRemove`): standalone records and the sub-records of
//! committed `TransactionRedo` records.
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
//! ran `CoreLoop::set_node_labels`, which vivifies its node argument via
//! `ensure_node` — labeling a node with no edges yet is a supported, successful
//! live operation. Replay therefore runs the same `set_node_labels` /
//! `remove_node_label` unconditionally, with no `contains_node` precondition:
//! reproducing that vivification on restart is not "interning a phantom node",
//! it is recreating the state that legitimately existed before the crash.
//! `set_node_labels` is all-or-none: a set that exhausts the node-id space or
//! the distinct node-label limit applies nothing, and replay logs and skips
//! the record with the same error code the live handler answered.
//! `remove_node_label` never vivifies (it no-ops on an unknown node or unknown
//! label), so it is identical to live.
//!
//! ## One record, one version commit
//!
//! A committed transaction's redo record carries one sub-record per staged
//! node delta, all at the record's LSN. The versions of all of them commit in
//! one storage transaction with the record's LSN mark. A crash therefore
//! leaves the record versioned whole or not at all, and replay never skips a
//! version of a record whose mark landed.

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use super::graph_label_apply::LabelOpKind;
use crate::bridge::envelope::ErrorCode;
use crate::wal::NodeLabelRedo;

/// The label op kind of `record`, `None` when it is no label record.
fn label_kind(record: &WalRecord) -> Option<LabelOpKind> {
    match RecordType::from_raw(record.logical_record_type())? {
        RecordType::GraphNodeLabelSet => Some(LabelOpKind::Set),
        RecordType::GraphNodeLabelRemove => Some(LabelOpKind::Remove),
        _ => None,
    }
}

/// Whether two records belong to one WAL record: the sub-records of one
/// redo record share its LSN, tenant and database.
fn same_record(a: &WalRecord, b: &WalRecord) -> bool {
    a.header.lsn == b.header.lsn
        && a.header.tenant_id == b.header.tenant_id
        && a.header.database_id == b.header.database_id
}

impl CoreLoop {
    /// Replay the label records of one WAL record: every op applies to the
    /// CSR, then every version the record writes commits in one storage
    /// transaction with the record's LSN mark. Returns how many ops applied.
    ///
    /// A malformed payload, or an op the live handler refuses (node-id space
    /// exhaustion, the distinct node-label limit), is logged and skipped:
    /// never a panic. A version write that fails takes back every op of the
    /// record. In a committed-redo install each op's undo goes to the
    /// install's undo log, so a failed install takes the record back whole.
    pub(crate) fn replay_label_record(&mut self, records: &[&WalRecord]) -> usize {
        let Some(first) = records.first() else {
            return 0;
        };
        let lsn = first.header.lsn;
        let tenant_id = first.header.tenant_id;
        let database_id = first.header.database_id;
        let recording = self.recording_redo_undo();
        let mut versions = Vec::new();
        let mut local_undo = Vec::new();
        let mut applied = 0usize;
        for record in records {
            let Some(kind) = label_kind(record) else {
                continue;
            };
            let Ok(op) = NodeLabelRedo::decode(&record.payload) else {
                self.replay_record_rejected(
                    "graph",
                    lsn,
                    None,
                    "malformed graph node-label WAL record",
                );
                continue;
            };
            if self.claim_for_validation() {
                continue;
            }
            match self.apply_label_op(database_id, tenant_id, &op, kind, Some(lsn)) {
                Ok(done) => {
                    if recording {
                        self.record_redo_undo([done.undo]);
                    } else {
                        local_undo.push(done.undo);
                    }
                    versions.extend(done.version);
                    applied += 1;
                }
                Err(code) => {
                    let detail = format!(
                        "{kind:?} of labels {:?} on node '{}' failed",
                        op.labels, op.node_id
                    );
                    self.replay_record_rejected("graph", lsn, Some(Box::new(code)), &detail);
                }
            }
        }
        if versions.is_empty() {
            return applied;
        }
        match self
            .edge_store
            .put_node_label_versions(&versions, Some(lsn))
        {
            Ok(replaced) => {
                if recording {
                    let undo: Vec<_> = versions
                        .iter()
                        .zip(replaced)
                        .map(|(version, replaced)| Self::label_version_undo(version, replaced))
                        .collect();
                    self.record_redo_undo(undo);
                }
                applied
            }
            Err(error) => {
                // An install's own rollback takes back the recorded ops.
                let code = match self.rollback_undo_log(database_id, tenant_id, local_undo) {
                    Ok(()) => ErrorCode::from(error),
                    Err(undo_error) => ErrorCode::from(undo_error),
                };
                let detail = format!(
                    "writing the {} label versions of the record failed",
                    versions.len()
                );
                self.replay_record_rejected("graph", lsn, Some(Box::new(code)), &detail);
                0
            }
        }
    }

    /// Replay every label record in `records` that routes to this core,
    /// each sub-record group of one WAL record as one
    /// [`CoreLoop::replay_label_record`]. `records` is in LSN order.
    ///
    /// A record at or below the label checkpoint's durable LSN is skipped:
    /// the restored labels hold it. A snapshot install leaves labels no WAL
    /// record carries, and its checkpoint is durable through the install, so
    /// an older record of the installed nodes never lands on top of them.
    pub(crate) fn replay_label_records(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
    ) -> usize {
        let floor = self.floors.graph_label_durable_lsn.as_u64();
        let mut replayed = 0usize;
        let mut group: Vec<&WalRecord> = Vec::new();
        for record in records {
            if self.replay_halted() {
                group.clear();
                break;
            }
            if label_kind(record).is_none() || record.header.lsn <= floor {
                continue;
            }
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }
            if group
                .first()
                .is_some_and(|first| !same_record(first, record))
            {
                replayed += self.replay_label_record(&group);
                group.clear();
            }
            group.push(record);
        }
        if !group.is_empty() {
            replayed += self.replay_label_record(&group);
        }
        replayed
    }

    /// Replay every standalone `GraphNodeLabelSet` / `GraphNodeLabelRemove`
    /// record in `records`.
    ///
    /// Called once during startup, after the CSR has been rebuilt from the
    /// `EdgeStore` (in `CoreLoop::open`) and before the event loop starts.
    /// Node-label records carry no `collection` field (labels are
    /// tenant/database-scoped, not collection-scoped — see
    /// `GraphOp::SetNodeLabels`), so there is nothing to check against
    /// `CollectionTombstoned` tombstones here, unlike the other per-engine
    /// replay passes.
    pub(crate) fn replay_graph_node_label_wal(&mut self, records: &[WalRecord], num_cores: usize) {
        let replayed = self.replay_label_records(records, num_cores);
        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL graph node-label replay complete"
            );
        }
    }

    /// Replay the `GraphNodeLabelSet` / `GraphNodeLabelRemove` sub-records
    /// reconstituted from committed `TransactionRedo` records. The sub-records
    /// of one redo record share its LSN, so they replay as one record.
    pub(crate) fn replay_graph_node_labels_redo(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
    ) {
        let replayed = self.replay_label_records(records, num_cores);
        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL graph node-label redo replay complete"
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

    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core")
    }

    /// A committed redo record at `lsn` with one label set per `(node,
    /// label)`, each without an ordinal: each version takes the applying
    /// core's clock, so a version written twice shows as two versions.
    fn label_redo(lsn: u64, sets: &[(&str, &str)]) -> WalRecord {
        let ops = sets
            .iter()
            .map(|(node, label)| crate::wal::RedoSubRecord {
                record_type: RecordType::GraphNodeLabelSet as u32,
                payload: crate::wal::NodeLabelRedo::live(node, &[label.to_string()])
                    .encode()
                    .expect("encode label op"),
            })
            .collect();
        let redo = crate::wal::RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        };
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn version_count(core: &CoreLoop) -> usize {
        core.edge_store
            .scan_node_label_versions(DatabaseId::DEFAULT, TenantId::new(TID))
            .expect("scan label versions")
            .len()
    }

    fn replay(core: &mut CoreLoop, record: &WalRecord) {
        core.replay_transaction_redo_wal(
            std::slice::from_ref(record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay");
    }

    /// The store's mark is behind a redo record carrying two label ops. Replay
    /// writes both versions, once each. A restart replays the record again
    /// and writes neither a second time: both versions committed with the
    /// record's mark.
    #[test]
    fn every_version_of_a_redo_record_lands_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let core = open_core_at(dir.path());
            // An earlier record's version leaves the mark at 10.
            core.edge_store
                .put_node_label_version(
                    DatabaseId::DEFAULT,
                    TenantId::new(TID),
                    "carol",
                    1,
                    Some(10),
                    &["City".to_string()],
                )
                .expect("seed an earlier version");
        }
        let record = label_redo(50, &[("alice", "Person"), ("bob", "Robot")]);
        {
            let mut core = open_core_at(dir.path());
            replay(&mut core, &record);
            assert_eq!(version_count(&core), 3, "both versions of the record land");
            assert!(has_label(&core, "alice", "Person"));
            assert!(has_label(&core, "bob", "Robot"));
        }
        let mut core = open_core_at(dir.path());
        replay(&mut core, &record);
        assert_eq!(
            version_count(&core),
            3,
            "a record whose mark landed writes no version again"
        );
        assert!(has_label(&core, "alice", "Person"));
        assert!(has_label(&core, "bob", "Robot"));
    }

    /// A redo label op that carries its ordinal writes its version at that
    /// ordinal, whatever the applying core's clock reads.
    #[test]
    fn a_carried_ordinal_is_the_versions_ordinal() {
        let mut h = make_core();
        let op = crate::wal::NodeLabelRedo {
            node_id: "alice".into(),
            labels: vec!["Person".into()],
            system_from: Some(7_000),
            exact: false,
        };
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::GraphNodeLabelSet as u32,
            lsn: 3,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: op.encode().expect("encode"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        h.core
            .replay_graph_node_label_wal(std::slice::from_ref(&record), 1);
        assert_eq!(
            h.core
                .edge_store
                .scan_node_label_versions(DatabaseId::DEFAULT, TenantId::new(TID))
                .expect("scan"),
            vec![("alice".to_string(), 7_000, vec!["Person".to_string()])]
        );
    }

    /// An exact set replaces the node's labels and writes exactly its labels.
    /// A newer stored version keeps the CSR at that version's labels.
    #[test]
    fn an_exact_set_restores_a_version_and_respects_a_newer_one() {
        let mut h = make_core();
        h.core
            .csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node_label("alice", "Stale")
            .expect("seed label");
        let exact = |ordinal: i64, labels: &[&str]| crate::wal::NodeLabelRedo {
            node_id: "alice".into(),
            labels: labels.iter().map(|l| l.to_string()).collect(),
            system_from: Some(ordinal),
            exact: true,
        };
        let db = DatabaseId::DEFAULT.as_u64();
        let set = super::LabelOpKind::Set;
        let applied = h
            .core
            .apply_label_op(db, TID, &exact(500, &["Admin"][..]), set, None)
            .expect("exact set at 500");
        let version = applied.version.expect("a version");
        assert_eq!(version.labels, vec!["Admin".to_string()]);
        h.core
            .edge_store
            .put_node_label_versions(&[version], None)
            .expect("write version");
        assert!(has_label(&h.core, "alice", "Admin"));
        assert!(!has_label(&h.core, "alice", "Stale"));

        // An older restored version: history gains it, the CSR keeps the
        // newer version's labels.
        let applied = h
            .core
            .apply_label_op(db, TID, &exact(100, &["Person"][..]), set, None)
            .expect("exact set at 100");
        assert_eq!(
            applied.version.map(|v| v.labels),
            Some(vec!["Person".to_string()])
        );
        assert!(has_label(&h.core, "alice", "Admin"));
        assert!(!has_label(&h.core, "alice", "Person"));
    }
}

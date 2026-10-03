// SPDX-License-Identifier: BUSL-1.1

//! WAL replay arm for a TRUNCATE share's edge cut.
//!
//! A `GraphEdgeCut` sub-record names an edge collection and the ordinal of
//! the TRUNCATE's Calvin transaction. Its install records the cut, and every
//! read then hides the collection's versions applied below it. It reads no
//! stored edge, so the install gives the same state whatever this core
//! applied before or after it: a core that holds several vShards, or both
//! homes of an edge, reaches the state every other replica does.
//!
//! Every share of one TRUNCATE on this core records the same cut. The first
//! install records it, and the rest change nothing. Re-applying the record
//! on restart changes nothing either.
//!
//! The install brings the CSR along for each edge whose current state the
//! cut changed. It emits no per-edge change event, and replay rebuilds none.

use nodedb_wal::WalRecord;

use super::core_loop::CoreLoop;
use super::handlers::transaction::undo::UndoEntry;
use super::handlers::transaction::undo::edge_cut::EdgeCutUndo;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Install one journalled edge cut. Returns whether it applied. A cut
    /// that does not decode, or whose install fails, is a committed effect
    /// replay cannot apply.
    pub(super) fn replay_edge_cut(
        &mut self,
        record: &WalRecord,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let record_lsn = record.header.lsn;
        let cut = match zerompk::from_msgpack::<crate::wal::EdgeCutRedo>(&record.payload) {
            Ok(cut) => cut,
            Err(e) => {
                self.replay_record_unapplied(
                    "graph",
                    "edge_cut_decode",
                    record_lsn,
                    &format!("edge cut does not decode: {e}"),
                );
                return false;
            }
        };
        let database_id = record.header.database_id;
        let tid = record.header.tenant_id;
        if tombstones.is_tombstoned(database_id, tid, &cut.collection, record_lsn) {
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        let install = match self.edge_store.install_edge_cut(
            DatabaseId::new(database_id),
            TenantId::new(tid),
            &cut.collection,
            cut.cut,
        ) {
            Ok(install) => install,
            Err(e) => {
                self.replay_record_unapplied(
                    "graph",
                    "edge_cut_install",
                    record_lsn,
                    &format!(
                        "edge cut of '{}' at {} failed: {e}",
                        cut.collection, cut.cut
                    ),
                );
                return false;
            }
        };
        let flips = install.flips.clone();
        // The undo is recorded once the store holds the cut, and before the
        // CSR follows it, so a rollback removes the cut even when a CSR
        // update below fails.
        if self.recording_redo_undo() {
            self.record_redo_undo([UndoEntry::EdgeCut(Box::new(EdgeCutUndo {
                database_id,
                tid,
                install,
            }))]);
        }
        for flip in &flips {
            if let Err(e) = self.mirror_edge_csr(
                database_id,
                tid,
                (&flip.src, &flip.label, &flip.dst),
                &cut.collection,
                flip.after.as_deref(),
            ) {
                self.replay_record_unapplied(
                    "graph",
                    "edge_cut_csr",
                    record_lsn,
                    &format!(
                        "CSR edge {} {}-[{}]->{} after the cut at {}: {e}",
                        cut.collection, flip.src, flip.label, flip.dst, cut.cut
                    ),
                );
                return false;
            }
        }
        if !flips.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", flips.len());
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::Surrogate;
    use nodedb_wal::record::{RecordType, WalRecordArgs};

    use super::EdgeCutUndo;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::graph::csr::Direction;
    use crate::types::{DatabaseId, RecordHomes, TenantId, VShardId};
    use crate::wal::{EdgeCutRedo, EdgePutRedo, RedoRecord, RedoSubRecord};

    const TID: u64 = 1;
    const COLL: &str = "g";

    fn task_on(vshard: VShardId) -> ExecutionTask {
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
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// Put `src -> dst` on `home` as a committed record applies it: at
    /// `system_from`, applied at `applied` when the record carries one.
    fn put_at(
        core: &mut CoreLoop,
        home: VShardId,
        (src, dst): (&str, &str),
        system_from: i64,
        applied: Option<i64>,
    ) {
        core.apply_scope.graph_system_from = Some(system_from);
        core.apply_scope.graph_applied = applied;
        let response = core.execute_edge_put(
            &task_on(home),
            EdgePutParams {
                tid: TID,
                collection: COLL,
                src_id: src,
                label: "L",
                dst_id: dst,
                properties: b"",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        core.apply_scope.graph_system_from = None;
        core.apply_scope.graph_applied = None;
        assert_eq!(response.status, Status::Ok, "edge put: {response:?}");
    }

    /// One committed redo record on `home` carrying `ops`.
    fn redo_on(home: VShardId, lsn: u64, ops: Vec<RedoSubRecord>) -> nodedb_wal::WalRecord {
        let redo = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        nodedb_wal::WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: TID,
            vshard_id: home.as_u32(),
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn cut_sub(cut: i64) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::GraphEdgeCut as u32,
            payload: zerompk::to_msgpack_vec(&EdgeCutRedo {
                collection: COLL.to_string(),
                cut,
            })
            .expect("encode cut"),
        }
    }

    fn put_sub((src, dst): (&str, &str), system_from: i64, applied: Option<i64>) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&EdgePutRedo {
                collection: COLL.to_string(),
                src_id: src.to_string(),
                label: "L".to_string(),
                dst_id: dst.to_string(),
                properties: Vec::new(),
                src_surrogate: 1,
                dst_surrogate: 2,
                system_from: Some(system_from),
                applied,
            })
            .expect("encode put"),
        }
    }

    fn replay(core: &mut CoreLoop, records: &[nodedb_wal::WalRecord]) {
        core.replay_transaction_redo_wal(records, 1, &nodedb_wal::TombstoneSet::new())
            .expect("redo replay");
    }

    /// Live edges as `(src, dst)` pairs.
    type EdgePairs = Vec<(String, String)>;

    /// The live edges out of `nodes` on `core`, from the store and the CSR,
    /// each sorted.
    fn live(core: &mut CoreLoop, nodes: &[&str]) -> (EdgePairs, EdgePairs) {
        let mut store: EdgePairs = core
            .edge_store
            .scan_all_edges_decoded(None)
            .expect("scan edges")
            .into_iter()
            .map(|(_, _, _, src, _, dst, _)| (src, dst))
            .collect();
        store.sort();
        let partition = core.csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID);
        let mut csr: Vec<(String, String)> = nodes
            .iter()
            .flat_map(|node| {
                partition
                    .neighbors(node, &[], Direction::Out)
                    .into_iter()
                    .map(|(_, dst)| (node.to_string(), dst))
                    .collect::<Vec<_>>()
            })
            .collect();
        csr.sort();
        (store, csr)
    }

    /// An edge put sequenced just before a TRUNCATE, applied on a replica
    /// whose clock runs an hour past the sequencer's epoch clock. The
    /// version takes the transaction's Calvin ordinal, not the replica's
    /// clock, so the TRUNCATE's cut hides it. A version stamped from that
    /// clock sits above the cut, and the edge survives.
    #[test]
    fn a_calvin_edge_version_orders_against_the_cut_under_clock_skew() {
        const EPOCH_MS: i64 = 1_700_000_000_000;
        const HOUR_NS: i64 = 3_600_000_000_000;
        let put_ordinal = nodedb_types::calvin_txn_ordinal(EPOCH_MS, 3);
        let cut = nodedb_types::calvin_txn_ordinal(EPOCH_MS, 4);
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.hlc.update_from_remote(cut + HOUR_NS);
        let home = RecordHomes::edge("a", "b").owner();
        put_at(&mut core, home, ("a", "b"), put_ordinal, None);
        replay(&mut core, &[redo_on(home, 1, vec![cut_sub(cut)])]);
        assert_eq!(
            live(&mut core, &["a"]),
            (Vec::new(), Vec::new()),
            "an edge sequenced before the TRUNCATE is cut, whatever this replica's clock reads"
        );
    }

    /// One core holds both homes of a cross-shard edge, and a second edge
    /// sequenced after the TRUNCATE. The later edge's version lands on one
    /// home before the TRUNCATE's shares apply, and an earlier edge's version
    /// lands after them. Every order this core can apply them in gives the
    /// state a replica that applied them in sequence order holds.
    #[test]
    fn a_shared_core_reaches_the_sequence_order_state_in_any_apply_order() {
        const BEFORE: i64 = 1_000;
        const CUT: i64 = 2_000;
        const AFTER: i64 = 3_000;
        let peer = (0..)
            .map(|i| format!("peer{i}"))
            .find(|peer| !RecordHomes::edge("a", peer).is_single())
            .expect("a cross-shard edge");
        let homes = RecordHomes::edge("a", &peer);
        let shares = [homes.owner(), homes.second()];
        let before_edge = ("a", peer.as_str());
        let after_edge = ("a", "late");

        // Sequence order: the early put, both TRUNCATE shares, the late put.
        let in_order = |core: &mut CoreLoop| {
            for home in shares {
                put_at(core, home, before_edge, BEFORE, None);
            }
            for (lsn, home) in shares.iter().enumerate() {
                replay(core, &[redo_on(*home, lsn as u64 + 1, vec![cut_sub(CUT)])]);
            }
            put_at(core, homes.owner(), after_edge, AFTER, None);
        };
        // A shared core flushes the late put and one share first, and the
        // early put's second home last.
        let reordered = |core: &mut CoreLoop| {
            put_at(core, homes.owner(), after_edge, AFTER, None);
            put_at(core, homes.owner(), before_edge, BEFORE, None);
            replay(core, &[redo_on(shares[1], 1, vec![cut_sub(CUT)])]);
            put_at(core, homes.second(), before_edge, BEFORE, None);
            replay(core, &[redo_on(shares[0], 2, vec![cut_sub(CUT)])]);
        };

        let dir_a = tempfile::tempdir().expect("tempdir");
        let (mut ordered, _ta, _ra) = make_core_with_dir(dir_a.path());
        in_order(&mut ordered);
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (mut shared, _tb, _rb) = make_core_with_dir(dir_b.path());
        reordered(&mut shared);

        let expected = (
            vec![("a".to_string(), "late".to_string())],
            vec![("a".to_string(), "late".to_string())],
        );
        assert_eq!(live(&mut ordered, &["a"]), expected);
        assert_eq!(live(&mut shared, &["a"]), expected);
        assert_eq!(
            ordered
                .edge_store
                .scan_edges_for_tenant(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID))
                .expect("scan"),
            shared
                .edge_store
                .scan_edges_for_tenant(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID))
                .expect("scan"),
            "both cores store the same versions, cuts and applied ordinals"
        );
    }

    /// A restore's redo, sequenced after a TRUNCATE, restores an edge's
    /// history at its original system time. The TRUNCATE does not hide it,
    /// whether this core applied the cut first or the restore first. A
    /// second TRUNCATE sequenced after the restore hides it.
    #[test]
    fn a_restore_after_a_truncate_keeps_its_history_until_a_later_truncate() {
        const HISTORICAL: i64 = 100;
        const LIVE: i64 = 500;
        const FIRST_CUT: i64 = 1_000;
        const RESTORE: i64 = 2_000;
        const SECOND_CUT: i64 = 3_000;
        let home = RecordHomes::edge("a", "b").owner();
        let restore = redo_on(
            home,
            3,
            vec![put_sub(("a", "b"), HISTORICAL, Some(RESTORE))],
        );
        let first_cut = redo_on(home, 2, vec![cut_sub(FIRST_CUT)]);

        for restore_first in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            put_at(&mut core, home, ("a", "c"), LIVE, None);
            if restore_first {
                replay(&mut core, std::slice::from_ref(&restore));
                replay(&mut core, std::slice::from_ref(&first_cut));
            } else {
                replay(&mut core, std::slice::from_ref(&first_cut));
                replay(&mut core, std::slice::from_ref(&restore));
            }
            assert_eq!(
                live(&mut core, &["a"]),
                (
                    vec![("a".to_string(), "b".to_string())],
                    vec![("a".to_string(), "b".to_string())]
                ),
                "the restored edge is live and the truncated one is not \
                 (restore first: {restore_first})"
            );
            let edge = crate::engine::graph::edge_store::EdgeRef::new(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                COLL,
                "a",
                "L",
                "b",
            );
            assert!(
                core.edge_store
                    .ceiling_resolve_edge(edge, HISTORICAL, None)
                    .expect("history")
                    .is_some(),
                "the restored version keeps its original system time"
            );

            replay(&mut core, &[redo_on(home, 4, vec![cut_sub(SECOND_CUT)])]);
            assert_eq!(live(&mut core, &["a"]), (Vec::new(), Vec::new()));
        }
    }

    /// The undo of a cut install removes the cut and puts each edge it
    /// changed back into the CSR.
    #[test]
    fn a_cut_rolls_back_with_its_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let home = RecordHomes::edge("a", "b").owner();
        put_at(&mut core, home, ("a", "b"), 100, None);
        let install = core
            .edge_store
            .install_edge_cut(DatabaseId::DEFAULT, TenantId::new(TID), COLL, 200)
            .expect("cut");
        for flip in &install.flips {
            core.mirror_edge_csr(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                (&flip.src, &flip.label, &flip.dst),
                &install.collection,
                flip.after.as_deref(),
            )
            .expect("csr");
        }
        assert_eq!(live(&mut core, &["a"]), (Vec::new(), Vec::new()));
        core.apply_undo_edge_cut(
            0,
            EdgeCutUndo {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                install,
            },
        )
        .expect("undo cut");
        let expected = vec![("a".to_string(), "b".to_string())];
        assert_eq!(live(&mut core, &["a"]), (expected.clone(), expected));
    }
}

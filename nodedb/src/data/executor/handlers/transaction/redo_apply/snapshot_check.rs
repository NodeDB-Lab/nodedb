// SPDX-License-Identifier: BUSL-1.1

//! The serialization check of a single-shard commit's redo: its reads and
//! its keyed writes against the snapshot its reads observed.
//!
//! The check runs inside the apply, after the validate pass and before any
//! write, against the versions this replica stored at the record's log
//! position. Every replica stored the same versions there, so every one
//! reaches one verdict, and nothing commits between the check and the
//! install. Restart replay reinstalls the record through the replay arms
//! straight from the WAL and never reaches the check: a refused record left
//! no WAL record behind, since the funnel cancels the record of an apply
//! that answers with an error, and the durable Raft applied floor names
//! only records whose WAL record is synced.

use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::wal::RedoSubRecord;

use super::write_keys::redo_writes;

impl CoreLoop {
    /// Refuse a committed redo whose transaction read or replaces a row that
    /// holds a committed version above `snapshot`.
    ///
    /// `SerializationConflict` is final on every replica. A store that does
    /// not read is a `RetryableRefusal`: the failure is this node's, not the
    /// record's.
    pub(super) fn check_redo_snapshot(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        reads: &[VersionedReadEntry],
        ops: &[RedoSubRecord],
        snapshot: i64,
    ) -> Result<(), ErrorCode> {
        let vshard = task.request.vshard_id.as_u32();
        let unreadable = |error: crate::Error| ErrorCode::RetryableRefusal {
            reason: format!(
                "the serialization check of a committed redo on vShard {vshard} could not read \
                 a store: {error}"
            ),
        };
        // A snapshot more than the commit window behind the commit's ordinal
        // can predate a history purge on some replica. The window check reads
        // only values the commit carries, so every replica refuses it alike.
        let ordinal = self.apply_scope.replicated_ordinal;
        if !self.snapshot_within_window(snapshot, ordinal) {
            return Err(ErrorCode::SerializationConflict {
                detail: format!(
                    "the transaction's snapshot {snapshot} trails its commit ordinal {} by more \
                     than the snapshot window on vShard {vshard}; retry the transaction",
                    ordinal.unwrap_or_default()
                ),
            });
        }
        if !self
            .txn_reads_unchanged(task, tid, reads, snapshot)
            .map_err(unreadable)?
        {
            return Err(ErrorCode::SerializationConflict {
                detail: format!(
                    "a row the transaction read at snapshot {snapshot} holds a newer committed \
                     version on vShard {vshard}; retry the transaction"
                ),
            });
        }
        if !self
            .txn_writes_unchanged(task, tid, &redo_writes(ops), snapshot)
            .map_err(unreadable)?
        {
            return Err(ErrorCode::SerializationConflict {
                detail: format!(
                    "a row the transaction writes holds a committed version above its snapshot \
                     {snapshot} on vShard {vshard}; retry the transaction"
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::RedoOrigin;
    use nodedb_types::calvin::{EngineTag, ReadKeyIdent};
    use nodedb_types::{StorageKey, Surrogate};

    use super::super::entry::{CommittedRedo, RedoSnapshot};
    use super::super::test_commit::doc_put_sub_record;
    use super::*;
    use crate::bridge::envelope::{Response, Status};
    use crate::data::executor::core_loop::newest_ordinals::collection_vshard;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
    use crate::types::{DatabaseId, Lsn};
    use crate::wal::RedoRecord;

    const TID: u64 = 1;

    fn redo_of(ops: Vec<RedoSubRecord>) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
        }
        .to_bytes()
        .expect("encode redo")
    }

    fn task_at(lsn: u64) -> ExecutionTask {
        let mut task = task_with_vshard(collection_vshard(DatabaseId::DEFAULT, "orders").unwrap());
        task.wal_lsn = Some(Lsn::new(lsn));
        task
    }

    fn row_newest(core: &CoreLoop, surrogate: u32) -> Option<i64> {
        core.sparse
            .versioned_latest_sys_from(
                0,
                TID,
                "orders",
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read row")
    }

    /// Install a put of row `surrogate`, checked against `snapshot` when
    /// one is given.
    fn put_row(
        core: &mut CoreLoop,
        lsn: u64,
        surrogate: u32,
        snapshot: Option<RedoSnapshot<'_>>,
    ) -> Response {
        let redo = redo_of(vec![doc_put_sub_record(
            "orders",
            &format!("o{surrogate}"),
            &crate::data::executor::core_loop::tests::doc_value("a", "1"),
            surrogate,
        )]);
        let committed = CommittedRedo {
            redo: &redo,
            collections: &["orders".to_string()],
            sum_targets: &[],
        };
        match snapshot {
            Some(snapshot) => core.install_snapshot_redo(
                &task_at(lsn),
                TID,
                committed,
                RedoOrigin::Commit,
                snapshot,
            ),
            None => core.install_redo(&task_at(lsn), TID, committed, RedoOrigin::Commit),
        }
    }

    fn predicate() -> VersionedReadEntry {
        VersionedReadEntry {
            engine: EngineTag::Document,
            collection: "orders".into(),
            key: ReadKeyIdent::Predicate,
            home_vshard: None,
        }
    }

    fn is_conflict(response: &Response) -> bool {
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::SerializationConflict { .. })
        )
    }

    /// A transaction that replaces a row a later commit wrote is refused,
    /// and writes nothing. One whose snapshot covers that commit installs.
    #[test]
    fn a_write_over_a_row_committed_after_the_snapshot_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let seeded = put_row(&mut core, 10, 7, None);
        assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
        let newest = row_newest(&core, 7).expect("row 7 holds a version");

        let stale = RedoSnapshot {
            reads: &[],
            ordinal: newest - 1,
        };
        let refused = put_row(&mut core, 11, 7, Some(stale));
        assert!(is_conflict(&refused), "{:?}", refused.error_code);
        assert_eq!(row_newest(&core, 7), Some(newest), "nothing was written");

        let current = RedoSnapshot {
            reads: &[],
            ordinal: newest,
        };
        let installed = put_row(&mut core, 12, 7, Some(current));
        assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
        assert!(row_newest(&core, 7) > Some(newest));
    }

    /// A predicate read is refused once any row of its collection committed
    /// a version above the snapshot, and a fresh insert under a covering
    /// snapshot installs.
    #[test]
    fn a_predicate_read_is_refused_after_any_row_of_its_collection_changed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let seeded = put_row(&mut core, 10, 7, None);
        assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
        let newest = row_newest(&core, 7).expect("row 7 holds a version");
        let reads = [predicate()];

        let refused = put_row(
            &mut core,
            11,
            8,
            Some(RedoSnapshot {
                reads: &reads,
                ordinal: newest - 1,
            }),
        );
        assert!(is_conflict(&refused), "{:?}", refused.error_code);
        assert_eq!(row_newest(&core, 8), None, "nothing was written");

        let installed = put_row(
            &mut core,
            12,
            8,
            Some(RedoSnapshot {
                reads: &reads,
                ordinal: newest,
            }),
        );
        assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    }

    /// A put of row `surrogate` stamped at `sys_from`, as a session resolve
    /// stamps it.
    fn stamped_put(surrogate: u32, sys_from: i64) -> RedoSubRecord {
        RedoSubRecord {
            record_type: nodedb_wal::record::RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                "orders",
                format!("o{surrogate}"),
                crate::data::executor::core_loop::tests::doc_value("n", "1"),
                None::<nodedb_types::sync::wire::SyncProvenance>,
                surrogate,
                sys_from,
                i64::MIN,
                i64::MAX,
            ))
            .expect("encode put"),
        }
    }

    fn install_record(
        core: &mut CoreLoop,
        lsn: u64,
        record: &RedoRecord,
        snapshot: i64,
    ) -> Response {
        let redo = record.to_bytes().expect("encode redo");
        core.install_snapshot_redo(
            &task_at(lsn),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["orders".to_string()],
                sum_targets: &[],
            },
            RedoOrigin::Commit,
            RedoSnapshot {
                reads: &[],
                ordinal: snapshot,
            },
        )
    }

    /// The lost update of two read-modify-write transactions on one row.
    ///
    /// T1 resolves its put of row 7 at `resolved`. T2's snapshot closes at
    /// `snapshot` above it before T1's record enters the log, so no
    /// reservation holds the cut and T2 reads the row's older version. The
    /// leader reserves T1's record ordinal `record` after that cut, so above
    /// it. T2 then writes the row from its stale read.
    ///
    /// With the resolve stamp, T1's version lands at or below T2's
    /// snapshot, and T2's check passes: T1's update is lost. Raised to the
    /// record ordinal, T1's version is above the snapshot, and T2 is
    /// refused.
    #[test]
    fn a_commit_resolved_below_a_snapshot_installs_above_it() {
        for raised in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            let seeded = put_row(&mut core, 10, 7, None);
            assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
            let base = row_newest(&core, 7).expect("row 7 holds a version");
            let (resolved, snapshot, record, later) =
                (base + 1_000, base + 2_000, base + 3_000, base + 4_000);

            let mut t1 = RedoRecord::from_bytes(&redo_of(vec![stamped_put(7, resolved)]))
                .expect("decode redo");
            if raised {
                assert!(t1.raise_stamps_to(record).expect("raise"));
            }
            let installed = install_record(&mut core, 11, &t1, base);
            assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
            let t1_version = if raised { record } else { resolved };
            assert_eq!(row_newest(&core, 7), Some(t1_version));

            let t2 =
                RedoRecord::from_bytes(&redo_of(vec![stamped_put(7, later)])).expect("decode redo");
            let t2_commit = install_record(&mut core, 12, &t2, snapshot);
            if raised {
                assert!(is_conflict(&t2_commit), "{:?}", t2_commit.error_code);
                assert_eq!(row_newest(&core, 7), Some(record), "T2 wrote nothing");
            } else {
                assert_eq!(
                    t2_commit.status,
                    Status::Ok,
                    "the resolve stamp lets T2 overwrite T1 from a stale read"
                );
                assert_eq!(row_newest(&core, 7), Some(later));
            }
        }
    }

    /// Two replicas that applied the same records reach the same verdict,
    /// one with a warm newest-ordinal cache and one with a cold one.
    #[test]
    fn replicas_with_the_same_records_reach_one_verdict() {
        let (dir_a, dir_b) = (
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        );
        let (mut warm, _ra, _sa) = make_core_with_dir(dir_a.path());
        let (mut cold, _rb, _sb) = make_core_with_dir(dir_b.path());
        let redo = redo_of(vec![doc_put_sub_record(
            "orders",
            "o7",
            &crate::data::executor::core_loop::tests::doc_value("a", "1"),
            7,
        )]);
        let reads = [predicate()];
        let task = task_at(10);
        let home = collection_vshard(DatabaseId::DEFAULT, "orders").unwrap();
        // The warm replica caches the slice before the record lands, so the
        // install's keyed note raises the cached value.
        assert_eq!(
            warm.slice_newest(
                home,
                DatabaseId::DEFAULT,
                crate::types::TenantId::new(TID),
                "orders"
            )
            .expect("read stores"),
            None
        );
        for core in [&mut warm, &mut cold] {
            let installed = core.install_redo(
                &task,
                TID,
                CommittedRedo {
                    redo: &redo,
                    collections: &["orders".to_string()],
                    sum_targets: &[],
                },
                RedoOrigin::Commit,
            );
            assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
        }
        let newest = row_newest(&warm, 7).expect("row 7");
        assert_eq!(row_newest(&cold, 7), Some(newest), "one stamp on both");
        // The cold replica reads every value from its stores, as after a
        // restart.
        cold.forget_newest_ordinals();
        for snapshot in [newest - 1, newest] {
            let verdicts: Vec<bool> = [&mut warm, &mut cold]
                .into_iter()
                .map(|core| {
                    core.txn_reads_unchanged(&task, TID, &reads, snapshot)
                        .expect("read stores")
                })
                .collect();
            assert_eq!(verdicts[0], verdicts[1], "snapshot {snapshot}");
            assert_eq!(verdicts[0], snapshot >= newest);
        }
    }
}

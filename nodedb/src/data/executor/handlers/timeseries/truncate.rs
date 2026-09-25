// SPDX-License-Identifier: BUSL-1.1

//! `TimeseriesOp::Truncate`: remove every row of a timeseries collection.
//!
//! In-memory state (memtable, its reservation, partition registry,
//! last-value cache, series catalog) is moved out whole. The partition
//! directory is renamed to an aside name in one atomic step
//! (`truncating_dir_name`), so a scan can never observe half a directory.
//! Autocommit removes the aside directory at once; inside a transaction
//! batch it stays until the batch finalizes (`finalize_timeseries_truncates`)
//! or the undo renames it back (`apply_undo_timeseries_truncate`). A crash
//! between rename and removal leaves an aside directory that boot removes
//! (`remove_truncating_leftovers`).
//!
//! ## The stamp a truncate leaves
//!
//! After the rename the truncate creates a fresh collection directory holding
//! only the collection's replay stamp (`timeseries_checkpoint::stamp`). Its
//! rows name every record the truncate removed: the collection stamp plus
//! every record this core applied. Its truncates name this truncate. Restart
//! replay then never re-applies the truncate, and a record below its LSN that
//! applied after it replays and survives.
//!
//! The stamp is written after the rename. A crash between the two leaves no
//! stamp naming the truncate, so replay re-applies it; no later write applied
//! in between, because this core ran nothing else. A stamp that cannot be
//! written puts the directory back and fails the truncate.

use std::path::{Path, PathBuf};

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::{TimeseriesTruncateUndo, UndoEntry};
use crate::data::executor::task::ExecutionTask;
use crate::data::executor::timeseries_checkpoint::stamp::{
    TsCollectionKey, TsReplayStamp, write_ts_stamp,
};
use crate::types::replay_stamp::ReplayStamp;

/// Suffix marking a partition directory renamed aside by a truncate.
pub(in crate::data::executor) const TRUNCATING_MARKER: &str = ".truncating-";

/// Whether `dir_name` is an aside directory a truncate left behind.
pub(in crate::data::executor) fn is_truncating_leftover(dir_name: &str) -> bool {
    dir_name.contains(TRUNCATING_MARKER)
}

/// The aside name for `collection`'s directory at `lsn`. Unique per truncate
/// on a core: two truncates of one collection never share an LSN.
fn truncating_dir_name(collection: &str, lsn: u64) -> String {
    format!("{collection}{TRUNCATING_MARKER}{lsn}")
}

/// Remove `dir` and everything below it. A missing directory is already
/// removed.
pub(in crate::data::executor) fn remove_dir_tree(dir: &Path) -> crate::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "remove truncated partition directory {}: {e}",
                dir.display()
            ),
        }),
    }
}

impl CoreLoop {
    /// Handle `TimeseriesOp::Truncate`. Reports the number of rows that
    /// existed across the memtable and every partition.
    pub(in crate::data::executor) fn execute_timeseries_truncate(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "timeseries truncate");
        let db = task.request.database_id;
        let tid = task.request.tenant_id;
        let key = (db, tid, collection.to_string());

        // The registry is loaded lazily on first scan; load it here so the
        // count covers partitions written before this process started.
        if let Err(e) = self.ensure_ts_registry(tid, db, collection) {
            return self.response_error(task, e);
        }

        // The rename is the one step that can fail, so it runs before any
        // in-memory state moves: a failure leaves the collection untouched.
        let original =
            super::paths::ts_collection_dir(&self.data_dir, db.as_u64(), tid.as_u64(), collection);
        let moved_dir = if original.exists() {
            let lsn = task
                .wal_lsn()
                .map(|l| l.as_u64())
                .unwrap_or(self.watermark.as_u64());
            let moved = original.with_file_name(truncating_dir_name(collection, lsn));
            if let Err(e) = rename_aside(&original, &moved) {
                return self.response_error(task, e);
            }
            Some(moved)
        } else {
            None
        };
        let stamp = match self.write_truncate_stamp(&key, &original, task.wal_lsn()) {
            Ok(stamp) => stamp,
            Err(e) => {
                if let Err(restore) = restore_after_failed_stamp(&original, moved_dir.as_ref()) {
                    return self.response_error(task, restore);
                }
                return self.response_error(task, e);
            }
        };

        let memtable = self.columnar_memtables.remove(&key);
        let memtable_mem = self.columnar_memtable_mem.remove(&key);
        let registry = self.ts_registries.remove(&key);
        let last_value_cache = self.ts_last_value_caches.remove(&key);
        let series_catalog = self.ts_series_catalogs.remove(&key);
        let truncated = memtable.as_ref().map(|m| m.row_count()).unwrap_or(0)
            + registry.as_ref().map(|r| r.total_row_count()).unwrap_or(0);
        let replay_stamp = self.ts_replay_stamps.insert(key.clone(), stamp);

        self.continuous_agg_mgr
            .reset_for_source(db.as_u64(), collection);

        match undo_log {
            Some(log) => log.push(UndoEntry::TimeseriesTruncate(Box::new(
                TimeseriesTruncateUndo {
                    collection_key: key,
                    original_dir: original,
                    moved_dir,
                    memtable,
                    memtable_mem,
                    registry,
                    last_value_cache,
                    series_catalog,
                    replay_stamp,
                },
            ))),
            None => {
                if let Some(moved) = &moved_dir
                    && let Err(e) = remove_dir_tree(moved)
                {
                    return self.response_error(task, e);
                }
            }
        }

        self.checkpoint_coordinator
            .mark_dirty("timeseries", truncated as usize);
        self.note_collection_write_lsn(task, collection);
        self.invalidate_aggregate_cache_for_collection(db.as_u64(), tid.as_u64(), collection);

        debug!(core = self.core_id, %collection, truncated, "timeseries truncate complete");
        self.truncate_response(task, truncated as usize)
    }

    /// Remove the aside partition directories of every timeseries truncate
    /// in a committed batch. A directory that cannot be removed is queued
    /// for the maintenance tick (`retry_ts_truncate_backlog`); the truncate
    /// itself is committed and the rows are already unreachable.
    pub(in crate::data::executor) fn finalize_timeseries_truncates(
        &mut self,
        undo_log: &[UndoEntry],
    ) {
        for entry in undo_log {
            let UndoEntry::TimeseriesTruncate(undo) = entry else {
                continue;
            };
            let Some(moved) = &undo.moved_dir else {
                continue;
            };
            if let Err(e) = remove_dir_tree(moved) {
                tracing::error!(
                    core = self.core_id,
                    dir = %moved.display(),
                    error = %e,
                    "committed timeseries truncate: aside directory removal deferred to maintenance"
                );
                self.ts_truncate_backlog.push(moved.clone());
            }
        }
    }

    /// Create `original` afresh and write into it the replay stamp this
    /// truncate leaves: every record this core applied, and the truncate at
    /// `lsn`. Returns the stamp.
    fn write_truncate_stamp(
        &self,
        key: &TsCollectionKey,
        original: &Path,
        lsn: Option<crate::types::Lsn>,
    ) -> crate::Result<TsReplayStamp> {
        let mut stamp = self.ts_flush_stamp(key)?;
        if let Some(lsn) = lsn {
            stamp.truncates = stamp.truncates.union(&ReplayStamp::naming(lsn.as_u64()));
        }
        std::fs::create_dir_all(original).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create collection directory {}: {e}", original.display()),
        })?;
        write_ts_stamp(original, &stamp)?;
        Ok(stamp)
    }

    /// Retry every aside directory removal a batch finalize deferred.
    /// Returns how many are still pending.
    pub(in crate::data::executor) fn retry_ts_truncate_backlog(&mut self) -> usize {
        let pending = std::mem::take(&mut self.ts_truncate_backlog);
        for dir in pending {
            if let Err(e) = remove_dir_tree(&dir) {
                tracing::error!(
                    core = self.core_id,
                    dir = %dir.display(),
                    error = %e,
                    "timeseries truncate backlog: aside directory removal failed again"
                );
                self.ts_truncate_backlog.push(dir);
            }
        }
        self.ts_truncate_backlog.len()
    }
}

/// Put the collection back as it was before a truncate whose stamp could not
/// be written: remove the fresh directory, then rename the aside one back.
fn restore_after_failed_stamp(original: &Path, moved: Option<&PathBuf>) -> crate::Result<()> {
    remove_dir_tree(original)?;
    if let Some(moved) = moved {
        std::fs::rename(moved, original).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "rename partition directory {} back to {}: {e}",
                moved.display(),
                original.display()
            ),
        })?;
    }
    Ok(())
}

/// Rename `original` to `moved`. An aside directory already at `moved` is a
/// leftover of an earlier truncate at the same LSN (a replay) and is removed
/// first; the truncate is committed either way.
fn rename_aside(original: &Path, moved: &PathBuf) -> crate::Result<()> {
    if moved.exists() {
        remove_dir_tree(moved)?;
    }
    std::fs::rename(original, moved).map_err(|e| crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!(
            "rename partition directory {} aside to {}: {e}",
            original.display(),
            moved.display()
        ),
    })
}

/// Remove every aside directory under the timeseries root at boot. The
/// truncate that produced it is in the WAL ahead of the rename, so replay
/// re-applies the truncate whether or not the removal completed.
pub(in crate::data::executor) fn remove_truncating_leftovers(ts_root: &Path) -> crate::Result<()> {
    let Ok(db_dirs) = std::fs::read_dir(ts_root) else {
        return Ok(());
    };
    for db_dir in db_dirs.flatten() {
        let Ok(tenant_dirs) = std::fs::read_dir(db_dir.path()) else {
            continue;
        };
        for tenant_dir in tenant_dirs.flatten() {
            let Ok(coll_dirs) = std::fs::read_dir(tenant_dir.path()) else {
                continue;
            };
            for coll_dir in coll_dirs.flatten() {
                let name = coll_dir.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if is_truncating_leftover(name) && coll_dir.path().is_dir() {
                    remove_dir_tree(&coll_dir.path())?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::timeseries_checkpoint::stamp::TS_STAMP_FILE;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use nodedb_physical::physical_plan::TimeseriesOp;

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// A task stamped `lsn` on this collection. The plan is the truncate;
    /// the ingest helper below reads only the envelope.
    fn task_at(lsn: u64) -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
            }),
            Some(Lsn::new(lsn)),
        )
    }

    /// An autocommit ILP ingest stamped `lsn`, as the ILP planner builds it.
    fn ingest(core: &mut CoreLoop, lines: &str, lsn: u64) -> Status {
        use super::super::{TimeseriesApplyMode, TimeseriesIngestExec};
        let task = task_at(lsn);
        core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: TenantId::new(TENANT),
            collection: COLLECTION,
            payload: lines.as_bytes(),
            format: "ilp",
            wal_lsn: Some(lsn),
            provenance: None,
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        })
        .status
    }

    fn memtable_rows(core: &CoreLoop) -> u64 {
        core.columnar_memtables
            .get(&key())
            .map(|m| m.row_count())
            .unwrap_or(0)
    }

    fn partition_rows(core: &CoreLoop) -> u64 {
        core.ts_registries
            .get(&key())
            .map(|r| r.total_row_count())
            .unwrap_or(0)
    }

    /// The entries of `dir`, by name, sorted.
    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn decode_truncated(resp: &crate::bridge::envelope::Response) -> u64 {
        let v = nodedb_types::value_from_msgpack(resp.payload.as_bytes()).expect("payload");
        v.as_object()
            .and_then(|m| m.get("truncated"))
            .and_then(|n| n.as_i64())
            .expect("truncated count") as u64
    }

    /// Autocommit: memtable rows and a flushed partition both go, the
    /// directory is replaced by one holding only the truncate's stamp, and
    /// the count covers both.
    #[test]
    fn autocommit_truncate_removes_memtable_partitions_and_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(
                &mut core,
                "metrics,host=a value=1i\nmetrics,host=b value=2i\n",
                1
            ),
            Status::Ok
        );
        core.flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        assert_eq!(
            ingest(&mut core, "metrics,host=c value=3i\n", 2),
            Status::Ok
        );
        assert_eq!(partition_rows(&core), 2);
        assert_eq!(memtable_rows(&core), 1);
        let ts_dir = super::super::paths::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);
        assert!(ts_dir.exists());

        let resp = core.execute_timeseries_truncate(&task_at(3), COLLECTION, None);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(decode_truncated(&resp), 3);
        assert_eq!(memtable_rows(&core), 0);
        assert_eq!(partition_rows(&core), 0);
        assert_eq!(
            entries(&ts_dir),
            vec![TS_STAMP_FILE.to_string()],
            "the partitions are gone; the fresh directory holds the stamp"
        );
        let stamp = core.ts_replay_stamps.get(&key()).expect("stamp");
        assert!(stamp.rows.skips(1) && stamp.rows.skips(2), "{stamp:?}");
        assert!(stamp.truncates.skips(3) && !stamp.truncates.skips(2));
        assert_eq!(
            crate::data::executor::timeseries_checkpoint::stamp::read_ts_stamp(&ts_dir)
                .expect("read")
                .as_ref(),
            Some(stamp)
        );
        assert!(
            !core.columnar_memtable_mem.contains_key(&key()),
            "the memtable reservation is released"
        );
        assert!(!core.ts_series_catalogs.contains_key(&key()));
        assert!(!core.ts_last_value_caches.contains_key(&key()));

        assert_eq!(
            ingest(&mut core, "metrics,host=z value=9i\n", 4),
            Status::Ok
        );
        assert_eq!(memtable_rows(&core), 1);
    }

    /// A catch-up redelivery of a record the truncate removed is refused: the
    /// truncate's stamp names it. A record below the truncate's LSN that was
    /// still on its way when the truncate applied is not named, and applies.
    #[test]
    fn only_a_record_the_truncate_removed_is_refused_after_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1),
            Status::Ok
        );
        let resp = core.execute_timeseries_truncate(&task_at(5), COLLECTION, None);
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1),
            Status::Ok
        );
        assert_eq!(
            memtable_rows(&core),
            0,
            "the redelivered record the truncate removed writes nothing"
        );
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 4),
            Status::Ok
        );
        assert_eq!(
            memtable_rows(&core),
            1,
            "a record in flight at the truncate applies after it"
        );
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 6),
            Status::Ok
        );
        assert_eq!(memtable_rows(&core), 2, "a post-truncate record is stored");
    }

    /// Inside a batch the directory is renamed aside and every in-memory
    /// structure is held on the undo entry; a rollback puts all of it back.
    #[test]
    fn batch_truncate_undo_restores_state_and_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1),
            Status::Ok
        );
        core.flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        assert_eq!(
            ingest(&mut core, "metrics,host=b value=2i\n", 2),
            Status::Ok
        );
        let ts_dir = super::super::paths::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);
        let stamp_before = core.ts_replay_stamps.get(&key()).cloned();

        let mut undo = Vec::new();
        let resp = core.execute_timeseries_truncate(&task_at(3), COLLECTION, Some(&mut undo));
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(decode_truncated(&resp), 2);
        assert_eq!(
            entries(&ts_dir),
            vec![TS_STAMP_FILE.to_string()],
            "the live directory is renamed aside and replaced by the stamp"
        );
        let aside = ts_dir.with_file_name(truncating_dir_name(COLLECTION, 3));
        assert!(aside.exists());
        assert_eq!(memtable_rows(&core), 0);
        assert_eq!(partition_rows(&core), 0);

        core.rollback_undo_log(0, TENANT, undo).expect("rollback");
        assert!(
            entries(&ts_dir).iter().any(|name| name.starts_with("ts-")),
            "the directory is renamed back"
        );
        assert!(!aside.exists());
        assert_eq!(memtable_rows(&core), 1);
        assert_eq!(partition_rows(&core), 1);
        assert_eq!(
            core.ts_replay_stamps.get(&key()).cloned(),
            stamp_before,
            "the stamp is rewound"
        );
        assert!(core.columnar_memtable_mem.contains_key(&key()));
    }

    /// A committed batch removes the aside directory at finalize.
    #[test]
    fn batch_truncate_finalize_removes_the_aside_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1),
            Status::Ok
        );
        core.flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        let ts_dir = super::super::paths::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);

        let mut undo = Vec::new();
        let resp = core.execute_timeseries_truncate(&task_at(2), COLLECTION, Some(&mut undo));
        assert_eq!(resp.status, Status::Ok);
        let aside = ts_dir.with_file_name(truncating_dir_name(COLLECTION, 2));
        assert!(aside.exists());

        core.finalize_timeseries_truncates(&undo);
        assert!(!aside.exists(), "finalize removes the aside directory");
        assert!(core.ts_truncate_backlog.is_empty());
    }

    #[test]
    fn aside_names_are_recognized_and_removed_at_boot() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("ts");
        let live = super::super::paths::ts_collection_dir(tmp.path(), 0, 1, "metrics");
        let aside = live.with_file_name(truncating_dir_name("metrics", 7));
        std::fs::create_dir_all(live.join("ts-1")).expect("live");
        std::fs::create_dir_all(aside.join("ts-1")).expect("aside");
        std::fs::write(aside.join("ts-1").join("data.bin"), b"x").expect("write");

        assert!(is_truncating_leftover("metrics.truncating-7"));
        assert!(!is_truncating_leftover("metrics"));

        remove_truncating_leftovers(&root).expect("recover");
        assert!(live.exists(), "the live directory is untouched");
        assert!(!aside.exists(), "the aside directory is removed");
    }

    #[test]
    fn remove_dir_tree_treats_a_missing_directory_as_removed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        remove_dir_tree(&tmp.path().join("nope")).expect("missing is fine");
    }
}

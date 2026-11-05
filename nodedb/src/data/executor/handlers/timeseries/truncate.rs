// SPDX-License-Identifier: BUSL-1.1

//! `TimeseriesOp::Truncate`: end every row of a timeseries collection.
//!
//! A TRUNCATE at ordinal `C` is a versioned end, not an erase:
//!
//! 1. The memtable flushes, so every row written before the truncate sits in
//!    a partition.
//! 2. Every current partition ends at `C` (`truncate_end`). A read at
//!    `S < C` still sees its rows. A current read, and a read at `S >= C`,
//!    does not. The memtable then holds only rows written after the
//!    truncate, so no read filters it.
//! 3. The collection's replay stamp names the truncate.
//! 4. The continuous aggregates over the collection reset, and their image
//!    is rewritten empty. They hold current state only, and every partition
//!    they folded ended. The image keeps its retention watermark.
//!
//! Every partition keeps its rows on disk. A purge removes a partition once
//! its end is at or below the purge horizon (`purge_timeseries_truncated_below`).
//!
//! ## The stamp a truncate leaves
//!
//! The collection stamp names this truncate. Restart replay then never
//! re-applies it: a record below its LSN that applied after it, and so sits
//! in the memtable as a current row, would be ended by a second apply. The
//! stamp is written after the partitions end. A crash between the two leaves
//! no stamp naming the truncate, so replay re-applies it, and ends the same
//! partitions: no later write applied in between, because this core ran
//! nothing else.
//!
//! ## Inside a transaction batch
//!
//! The undo entry carries what the truncate changed: the partitions it
//! ended, the stamps and the aggregate image before it, the last-value cache
//! and the series catalog. A rollback reverses each one.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::{TimeseriesTruncateUndo, UndoEntry};
use crate::data::executor::task::ExecutionTask;
use crate::data::executor::timeseries_checkpoint::stamp::{
    TsCollectionKey, TsReplayStamp, read_ts_stamp, write_ts_stamp,
};
use crate::engine::timeseries::last_value_cache::LastValueCache;
use crate::types::replay_stamp::ReplayStamp;

use super::TimeseriesApplyMode;

/// Remove `dir` and everything below it. A missing directory is already
/// removed.
pub(in crate::data::executor) fn remove_dir_tree(dir: &std::path::Path) -> crate::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("remove partition directory {}: {e}", dir.display()),
        }),
    }
}

impl CoreLoop {
    /// Handle `TimeseriesOp::Truncate`. Reports the number of rows it ended.
    ///
    /// The truncate's ordinal is the one its write carries: the Calvin
    /// transaction's, else the replicated or replayed record's, else a fresh
    /// one (`ts_batch_ordinal`), as an ingest stamps its rows.
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

        // The registry is loaded lazily; load it here so the truncate ends
        // partitions written before this process started.
        if let Err(e) = self.ensure_ts_registry(tid, db, collection) {
            return self.response_error(task, e);
        }
        let now_ms = self.ingest_now_ms();
        let ordinal = self.ts_batch_ordinal(TimeseriesApplyMode::Immediate, now_ms);

        // 1. Every row written before the truncate lands in a partition.
        if let Err(e) = self.flush_ts_collection(tid, db, collection, now_ms) {
            return self.response_error(task, e);
        }
        if self
            .columnar_memtables
            .get(&key)
            .is_some_and(|memtable| !memtable.is_empty())
        {
            return self.response_error(
                task,
                crate::Error::Internal {
                    detail: format!(
                        "timeseries truncate of '{collection}': the memtable still holds rows \
                         after its flush"
                    ),
                },
            );
        }

        // 2. The aggregate image leaves, so the aggregates start empty.
        let cagg_image = match self.take_cagg_image(db.as_u64(), tid.as_u64(), collection) {
            Ok(image) => image,
            Err(e) => return self.response_error(task, e),
        };

        // 3. Every current partition ends at the truncate's ordinal.
        let (ended, truncated) = match self.end_ts_partitions(&key, ordinal) {
            Ok(ended) => ended,
            Err(e) => {
                let e = self.put_back_after_failed_truncate(&key, &[], cagg_image.as_deref(), e);
                return self.response_error(task, e);
            }
        };

        // 4. The collection stamp names the truncate.
        let stamps = match self.write_truncate_stamp(&key, task.wal_lsn()) {
            Ok(stamps) => stamps,
            Err(e) => {
                let e = self.put_back_after_failed_truncate(&key, &ended, cagg_image.as_deref(), e);
                return self.response_error(task, e);
            }
        };

        self.continuous_agg_mgr
            .reset_for_source(db.as_u64(), tid.as_u64(), collection);
        // The reset image lands at once: it keeps the retention watermark
        // the removed image held (`raw_dropped_through`).
        if let Err(e) = self.persist_reset_cagg_image(db.as_u64(), tid.as_u64(), collection) {
            let e = match self.restore_truncate_stamps(&key, stamps) {
                Ok(()) => {
                    self.put_back_after_failed_truncate(&key, &ended, cagg_image.as_deref(), e)
                }
                Err(stamp_error) => stamp_error,
            };
            self.continuous_agg_mgr
                .reset_for_source(db.as_u64(), tid.as_u64(), collection);
            return self.response_error(task, e);
        }
        // A read below the truncate can no longer use the aggregates.
        let newest_truncate = self.ts_newest_truncate.get(&key).copied();
        self.ts_newest_truncate.insert(
            key.clone(),
            newest_truncate.map_or(ordinal, |n| n.max(ordinal)),
        );
        // The cache held the ended rows' values. An enabled cache starts
        // empty: the rows ingested from here on fill it.
        let last_value_cache = self.ts_last_value_caches.remove(&key);
        if last_value_cache.is_some() || self.last_value_cache_enabled(&key) {
            self.ts_last_value_caches
                .insert(key.clone(), LastValueCache::new());
        }
        // The catalog stays: the ended rows keep the ids it gave them, and a
        // key ingested again takes the same id. The durable copy needs no
        // rewrite.
        let series_catalog = self.ts_series_catalogs.get(&key).cloned();

        if let Some(log) = undo_log {
            log.push(UndoEntry::TimeseriesTruncate(Box::new(
                TimeseriesTruncateUndo {
                    collection_key: key,
                    ended,
                    replay_stamp: stamps.memory,
                    stamp_file: stamps.file,
                    cagg_image,
                    newest_truncate,
                    last_value_cache,
                    series_catalog,
                },
            )));
        }

        self.checkpoint_coordinator.mark_dirty(
            "timeseries",
            usize::try_from(truncated).unwrap_or(usize::MAX),
        );
        // No wipe: timeseries reads record only `Predicate` keys, which the
        // collection version bounds.
        if let Err(e) = self.note_collection_write(task, collection) {
            return self.response_error(task, e);
        }
        self.invalidate_aggregate_cache_for_collection(db.as_u64(), tid.as_u64(), collection);

        debug!(core = self.core_id, %collection, truncated, ordinal, "timeseries truncate complete");
        self.truncate_response(task, usize::try_from(truncated).unwrap_or(usize::MAX))
    }

    /// Write the collection stamp naming the truncate at `lsn`: every record
    /// this core applied, all of whose rows a partition now holds, and the
    /// truncate. Returns the stamps it replaced.
    fn write_truncate_stamp(
        &mut self,
        key: &TsCollectionKey,
        lsn: Option<crate::types::Lsn>,
    ) -> crate::Result<PriorStamps> {
        let dir = self.ts_dir_of(key);
        let prior = PriorStamps {
            memory: self.ts_replay.stamps.get(key).cloned(),
            file: read_ts_stamp(&dir)?,
        };
        let mut stamp = self.ts_flush_stamp(key)?;
        if let Some(lsn) = lsn {
            stamp.truncates = stamp.truncates.union(&ReplayStamp::naming(lsn.as_u64()));
        }
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create collection directory {}: {e}", dir.display()),
        })?;
        write_ts_stamp(&dir, &stamp)?;
        self.ts_replay.stamps.insert(key.clone(), stamp);
        Ok(prior)
    }

    /// Put back the stamps a truncate replaced: the collection stamp file
    /// and the in-memory collection stamp.
    pub(in crate::data::executor) fn restore_truncate_stamps(
        &mut self,
        key: &TsCollectionKey,
        prior: PriorStamps,
    ) -> crate::Result<()> {
        let dir = self.ts_dir_of(key);
        match &prior.file {
            Some(stamp) => write_ts_stamp(&dir, stamp)?,
            None => match std::fs::remove_file(
                dir.join(crate::data::executor::timeseries_checkpoint::stamp::TS_STAMP_FILE),
            ) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(crate::Error::Storage {
                        engine: "timeseries".into(),
                        detail: format!("remove the truncate stamp in {}: {e}", dir.display()),
                    });
                }
            },
        }
        match prior.memory {
            Some(stamp) => {
                self.ts_replay.stamps.insert(key.clone(), stamp);
            }
            None => {
                self.ts_replay.stamps.remove(key);
            }
        }
        Ok(())
    }

    /// Put back what a failed truncate changed: the partitions it ended and
    /// the aggregate image it removed. Returns the error it reports.
    fn put_back_after_failed_truncate(
        &mut self,
        key: &TsCollectionKey,
        ended: &[super::truncate_end::EndedPartition],
        cagg_image: Option<&[u8]>,
        cause: crate::Error,
    ) -> crate::Error {
        let (database_id, tenant_id, collection) = key;
        let reopened = self.reopen_ts_partitions(key, ended);
        let image = match cagg_image {
            Some(bytes) => self.put_back_cagg_image(
                database_id.as_u64(),
                tenant_id.as_u64(),
                collection,
                bytes,
            ),
            None => Ok(()),
        };
        match (reopened, image) {
            (Ok(()), Ok(())) => cause,
            (reopened, image) => {
                let failures: Vec<String> = [reopened.err(), image.err()]
                    .into_iter()
                    .flatten()
                    .map(|e| e.to_string())
                    .collect();
                tracing::error!(
                    core = self.core_id,
                    %collection,
                    error = %cause,
                    put_back = ?failures,
                    "timeseries truncate failed and could not put back its changes; a restart \
                     replays the truncate"
                );
                crate::Error::Storage {
                    engine: "timeseries".into(),
                    detail: format!(
                        "truncate of '{collection}' failed ({cause}) and putting back its \
                         changes failed ({}); a restart replays the truncate",
                        failures.join("; ")
                    ),
                }
            }
        }
    }

    /// Write the image of `root` right after a truncate reset its
    /// aggregates. A quarantined root keeps no image write, as everywhere.
    fn persist_reset_cagg_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) -> crate::Result<()> {
        if self
            .continuous_agg_mgr
            .is_quarantined(database_id, tenant_id, root)
        {
            return Ok(());
        }
        self.persist_cagg_image(database_id, tenant_id, root)
    }

    /// The newest TRUNCATE ordinal of `key`: the one this process applied,
    /// or the newest end a partition carries. `None` for a collection never
    /// truncated.
    pub(in crate::data::executor) fn ts_newest_truncate_of(
        &self,
        key: &TsCollectionKey,
    ) -> Option<i64> {
        let ended = self.ts_registries.get(key).and_then(|registry| {
            registry
                .iter()
                .filter_map(|(_, entry)| entry.meta.ended_at)
                .max()
        });
        self.ts_newest_truncate.get(key).copied().max(ended)
    }

    /// The directory of the collection `key`.
    pub(in crate::data::executor) fn ts_dir_of(&self, key: &TsCollectionKey) -> std::path::PathBuf {
        super::paths::ts_collection_dir(&self.data_dir, key.0.as_u64(), key.1.as_u64(), &key.2)
    }
}

/// The stamps a truncate replaced, which its undo puts back.
#[derive(Debug, Clone, Default)]
pub(in crate::data::executor) struct PriorStamps {
    /// The in-memory collection stamp.
    pub memory: Option<TsReplayStamp>,
    /// The collection directory's own stamp file.
    pub file: Option<TsReplayStamp>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use nodedb_physical::physical_plan::TimeseriesOp;
    use nodedb_types::SystemTimeScope;

    const TENANT: u64 = 1;
    const COLLECTION: &str = "metrics";

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// The ordinal every write at millisecond `ms` stamps in these tests.
    fn ordinal(ms: i64) -> i64 {
        ms * nodedb_types::NANOS_PER_MS + 1
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

    /// An autocommit ILP ingest stamped `lsn`, its rows at the ordinal of
    /// millisecond `ms`.
    fn ingest(core: &mut CoreLoop, lines: &str, lsn: u64, ms: i64) -> Status {
        use super::super::{TimeseriesApplyMode, TimeseriesIngestExec};
        let task = task_at(lsn);
        core.apply_scope.replicated_ordinal = Some(ordinal(ms));
        let status = core
            .execute_timeseries_ingest(TimeseriesIngestExec {
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
            .status;
        core.apply_scope.replicated_ordinal = None;
        status
    }

    /// A truncate stamped `lsn` at the ordinal of millisecond `ms`.
    fn truncate(
        core: &mut CoreLoop,
        lsn: u64,
        ms: i64,
        undo: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        core.apply_scope.replicated_ordinal = Some(ordinal(ms));
        let resp = core.execute_timeseries_truncate(&task_at(lsn), COLLECTION, undo);
        core.apply_scope.replicated_ordinal = None;
        resp
    }

    fn flush(core: &mut CoreLoop) {
        core.flush_ts_collection(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
    }

    fn memtable_rows(core: &CoreLoop) -> u64 {
        core.columnar_memtables
            .get(&key())
            .map(|m| m.row_count())
            .unwrap_or(0)
    }

    /// The end of every partition, in registry order.
    fn ends(core: &CoreLoop) -> Vec<Option<i64>> {
        core.ts_registries
            .get(&key())
            .map(|r| r.iter().map(|(_, e)| e.meta.ended_at).collect())
            .unwrap_or_default()
    }

    fn decode_truncated(resp: &Response) -> u64 {
        let v = nodedb_types::value_from_msgpack(resp.payload.as_bytes()).expect("payload");
        v.as_object()
            .and_then(|m| m.get("truncated"))
            .and_then(|n| n.as_i64())
            .expect("truncated count") as u64
    }

    /// `a` at 100 and `b` at 200 (`a` flushed), a truncate at 300, and `z`
    /// at 400.
    fn truncated_core(dir: &std::path::Path) -> (CoreLoop, impl Sized, impl Sized) {
        let (mut core, tx, rx) = make_core_with_dir(dir);
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i 1000000000", 1, 100),
            Status::Ok
        );
        flush(&mut core);
        assert_eq!(
            ingest(&mut core, "metrics,host=b value=2i 2000000000", 2, 200),
            Status::Ok
        );
        let resp = truncate(&mut core, 3, 300, None);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(decode_truncated(&resp), 2);
        assert_eq!(
            ingest(&mut core, "metrics,host=z value=9i 9000000000", 4, 400),
            Status::Ok
        );
        (core, tx, rx)
    }

    /// The truncate flushes the memtable, ends every partition at its
    /// ordinal, keeps every partition on disk, names itself in the stamp,
    /// and drops the caches. A later row lands in the memtable.
    #[test]
    fn a_truncate_ends_every_partition_and_keeps_the_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = truncated_core(dir.path());
        assert_eq!(ends(&core), vec![Some(ordinal(300)); 2]);
        assert_eq!(memtable_rows(&core), 1, "only the post-truncate row");
        let ts_dir = super::super::paths::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);
        for (_, entry) in core.ts_registries.get(&key()).expect("registry").iter() {
            assert!(
                ts_dir.join(&entry.dir_name).exists(),
                "the rows stay on disk"
            );
        }
        let stamp = core.ts_replay.stamps.get(&key()).expect("stamp");
        assert!(stamp.truncates.skips(3) && !stamp.truncates.skips(2));
        assert_eq!(
            read_ts_stamp(&ts_dir).expect("read").as_ref(),
            Some(stamp),
            "the stamp is on disk"
        );
        assert!(
            core.ts_series_catalogs.contains_key(&key()),
            "the catalog keeps the ids the ended rows carry"
        );
    }

    /// The collection scan reads the rows before the truncate at a cut
    /// below it, and only the later row at the current cut.
    #[test]
    fn the_collection_scan_reads_ended_rows_only_before_the_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = truncated_core(dir.path());
        let rows = |at| {
            core.scan_timeseries_docs_at(0, TENANT, COLLECTION, usize::MAX, at)
                .expect("scan")
                .len()
        };
        assert_eq!(rows(None), 1, "a current read sees only the later row");
        assert_eq!(rows(Some(ordinal(250))), 2, "before the truncate");
        assert_eq!(rows(Some(ordinal(150))), 1, "before `b`");
        assert_eq!(
            rows(Some(ordinal(350))),
            0,
            "after the truncate, before `z`"
        );
    }

    /// The universal scan: a raw read, the COUNT(*) fast path, a grouped
    /// count at a cut, and an audit read.
    #[test]
    fn the_timeseries_scan_applies_the_end_on_every_branch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = truncated_core(dir.path());
        let count = vec![("count".to_string(), "*".to_string())];
        let mut scan = |system_time, aggregates: &[(String, String)]| {
            let task = task_at(10);
            let resp = core.execute_timeseries_scan(super::super::TimeseriesScanParams {
                task: &task,
                tid: TenantId::new(TENANT),
                collection: COLLECTION,
                time_range: (0, i64::MAX),
                limit: usize::MAX,
                filters: &[],
                rls_filters: &[],
                sort_keys: &[],
                bucket_interval_ms: 0,
                group_by: &[],
                aggregates,
                gap_fill: "",
                computed_columns: &[],
                system_time,
                valid_at_ms: None,
                having: &[],
                tiering: None,
                output_columns: &[],
            });
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            let value: rmpv::Value =
                rmpv::decode::read_value(&mut resp.payload.as_bytes()).expect("decode");
            value.as_array().cloned().unwrap_or_default()
        };
        let counted = |rows: Vec<rmpv::Value>| -> i64 {
            rows.first()
                .and_then(|row| row.as_map())
                .and_then(|map| map.first())
                .and_then(|(_, n)| n.as_i64())
                .expect("count")
        };
        assert_eq!(scan(SystemTimeScope::Current, &[]).len(), 1);
        assert_eq!(counted(scan(SystemTimeScope::Current, &count)), 1);
        let before = SystemTimeScope::Pinned {
            ordinal: ordinal(250),
            cut_marker: 0,
        };
        assert_eq!(scan(before, &[]).len(), 2);
        assert_eq!(counted(scan(before, &count)), 2);
        assert_eq!(scan(SystemTimeScope::AllVersions, &[]).len(), 3);
    }

    /// The clone read at a millisecond before the truncate reads the rows
    /// it ended; a current clone read does not.
    #[test]
    fn the_clone_read_reads_ended_rows_only_before_the_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = truncated_core(dir.path());
        let entries = |as_of_ms| {
            let resp =
                core.execute_ts_materialize_scan(&task_at(10), COLLECTION, &[], 1000, as_of_ms);
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            let value: rmpv::Value =
                rmpv::decode::read_value(&mut resp.payload.as_bytes()).expect("decode");
            value
                .as_array()
                .and_then(|parts| parts.get(1))
                .and_then(|entries| entries.as_array())
                .map_or(0, Vec::len)
        };
        assert_eq!(entries(None), 1);
        assert_eq!(entries(Some(250)), 2);
    }

    /// A catch-up redelivery of a record whose rows the truncate's flush
    /// stored is refused: the stamp names it. A record below the truncate's
    /// LSN that was still on its way applies after it.
    #[test]
    fn only_a_record_the_flush_stored_is_refused_after_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1, 100),
            Status::Ok
        );
        let resp = truncate(&mut core, 5, 300, None);
        assert_eq!(resp.status, Status::Ok);

        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1, 100),
            Status::Ok
        );
        assert_eq!(
            memtable_rows(&core),
            0,
            "the redelivered record writes nothing"
        );
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 4, 400),
            Status::Ok
        );
        assert_eq!(
            memtable_rows(&core),
            1,
            "a record in flight applies after it"
        );
    }

    /// Inside a batch the undo entry holds what the truncate changed; a
    /// rollback reopens every partition and rewinds the stamp. The
    /// truncate's flush is not undone: its partition keeps the rows, so the
    /// stamp keeps naming their record and drops only the truncate.
    #[test]
    fn batch_truncate_undo_reopens_the_partitions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i\n", 1, 100),
            Status::Ok
        );
        flush(&mut core);
        assert_eq!(
            ingest(&mut core, "metrics,host=b value=2i\n", 2, 200),
            Status::Ok
        );
        let ts_dir = super::super::paths::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);
        let file_before = read_ts_stamp(&ts_dir).expect("read");

        let mut undo = Vec::new();
        let resp = truncate(&mut core, 3, 300, Some(&mut undo));
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(decode_truncated(&resp), 2);
        assert!(ends(&core).iter().all(Option::is_some));

        core.rollback_undo_log(0, TENANT, undo).expect("rollback");
        assert!(
            ends(&core).iter().all(Option::is_none),
            "every partition reopens"
        );
        let stamp = core.ts_replay.stamps.get(&key()).expect("stamp");
        assert!(
            stamp.rows.skips(1) && stamp.rows.skips(2),
            "the flushed records stay named"
        );
        assert!(!stamp.truncates.skips(3), "the truncate is no longer named");
        assert_eq!(read_ts_stamp(&ts_dir).expect("read"), file_before);
        assert_eq!(
            core.scan_timeseries_docs_at(0, TENANT, COLLECTION, usize::MAX, None)
                .expect("scan")
                .len(),
            2,
            "the current read sees both rows again"
        );
    }

    #[test]
    fn remove_dir_tree_treats_a_missing_directory_as_removed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        remove_dir_tree(&tmp.path().join("nope")).expect("missing is fine");
    }
}

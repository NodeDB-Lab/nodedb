// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable flush to L1 partition segments.
//!
//! The boot-side counterpart — rebuilding `ts_registries` from the partitions
//! this writes — lives in `data::executor::timeseries_checkpoint`.

use std::path::Path;

use nodedb_types::timeseries::PartitionMeta;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::schema::write_ts_schema;
use crate::data::executor::timeseries_checkpoint::stamp::{TsReplayStamp, write_ts_stamp};
use crate::engine::timeseries::columnar_memtable::ColumnarFlushView;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
use crate::engine::timeseries::interval_split::split_by_interval;
use crate::engine::timeseries::partition_registry::PartitionRegistry;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Flush a timeseries collection's memtable to L1 segments.
    ///
    /// Writes one partition per partition interval the rows fall in, drains
    /// the columnar memtable, registers the new partitions in `ts_registries`,
    /// and folds each into the continuous aggregates rooted at the collection.
    /// The interval is the collection's declared `partition_by`, read when the
    /// flush runs, so an ALTER of it governs later flushes only. A row whose
    /// interval an earlier flush sealed lands in a new partition of that
    /// interval.
    ///
    /// Rows in one interval commit with that partition's `partition.meta`.
    /// Rows in several commit together through a flush manifest
    /// (`flush_commit`): every partition appears, or none does.
    ///
    /// Returns `Ok(())` on success (including when the memtable is empty or
    /// absent — both are no-ops). Returns `Err` if the segment write fails;
    /// the caller is responsible for surfacing or propagating the error.
    ///
    /// ## Why the segment is written BEFORE the memtable is drained
    ///
    /// These rows have no durable copy but the WAL, and the coordinated
    /// checkpoint calls this flush and then reports the LSN that authorises
    /// deleting it. An encode or write failure must not take the rows out of
    /// memory without putting them anywhere. The partition is therefore
    /// written from a BORROW (`ColumnarMemtable::flush_view`) and the drain
    /// happens only once `write_partition` has returned `Ok` — its
    /// `partition.meta` write being the commit point. Every failure path
    /// leaves the memtable exactly as it was, so a failed flush costs a retry
    /// while the caller's clamped checkpoint LSN keeps the WAL records behind
    /// it.
    ///
    /// ## What the partition's stamp names
    ///
    /// The partition carries the collection's replay stamp as of this flush
    /// (`timeseries_checkpoint::stamp`): the collection stamp plus every
    /// record this core applied. Every row in the view belongs to one of
    /// those records, and every one of those records has its rows here or in
    /// an earlier partition. Restart replay skips exactly the named records.
    ///
    /// The ingest path resolves everything that could stop it mid-record
    /// before the first row of a record goes in (its record-boundary admission
    /// gate), and notes a record applied only once its rows all landed. A
    /// flush from between two rows of a record would break the stamp in
    /// either direction, so no caller may introduce one.
    pub(in crate::data::executor) fn flush_ts_collection(
        &mut self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
        now_ms: i64,
    ) -> crate::Result<()> {
        let key = (database_id, tid, collection.to_string());
        let segment_dir = super::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        // A flush that committed and did not install still has its rows at
        // the front of this memtable. Install it first, so those rows leave
        // the memtable before a new flush reads it. While that install fails,
        // the collection takes no new flush: writing those rows again would
        // store them twice.
        if super::flush_commit::has_uninstalled_flush(&segment_dir)? {
            self.roll_forward_ts_flush(&key, now_ms)?;
        }
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return Ok(());
        };
        if mt.is_empty() {
            return Ok(());
        }

        let interval = self.ts_partition_interval(&key)?;
        let view = mt.flush_view();

        let stamp = self.ts_flush_stamp(&key)?;
        // Informational: `PartitionMeta` is shared with Lite, and replay reads
        // the stamp file, never this LSN.
        let flush_wal_lsn = stamp.rows.highest();
        std::fs::create_dir_all(&segment_dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create collection dir {}: {e}", segment_dir.display()),
        })?;
        // The schema outlives the drain below. It lands before the commit
        // point too, so a restart seeds the memtable with every column a
        // committed partition's records added.
        write_ts_schema(&segment_dir, mt.schema())?;
        let ts_kek = self.segment_keks.ts_segment_kek.as_ref();

        // One partition per interval the rows fall in. Several commit together
        // and install through the same roll-forward a later flush, the
        // maintenance tick and boot recovery run.
        let buckets = split_by_interval(&view, &interval);
        if buckets.len() > 1 {
            let committed =
                super::flush_buckets::stage_and_commit(super::flush_buckets::BucketFlush {
                    segment_dir: &segment_dir,
                    view: &view,
                    buckets: &buckets,
                    stamp: &stamp,
                    flush_wal_lsn,
                    kek: ts_kek,
                })?;
            // ── Commit point passed: the flush is durable ────────────────────
            return self.install_committed_flush(&key, &segment_dir, committed, now_ms);
        }
        let (partition_name, meta) = write_single_partition(SinglePartition {
            segment_dir: &segment_dir,
            view: &view,
            interval_ms: buckets.first().map_or(0, |bucket| bucket.interval_ms()),
            stamp: &stamp,
            flush_wal_lsn,
            kek: ts_kek,
            collection,
        })?;

        // ── Commit point passed: the rows are on disk and reachable ──────────
        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return Err(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!(
                    "timeseries memtable for collection {collection} vanished between the \
                     segment write and the drain"
                ),
            });
        };
        let drain = mt.drain();
        let replay_prefix = stamp.rows.prefix;
        let applied_ranges = stamp.rows.applied_above.len();
        self.ts_replay.stamps.insert(key.clone(), stamp);

        // The memtable is empty, so drop its memory reservation. The
        // reservation tracks the full resident footprint, kept current by
        // `recharge_ts_memtable_budget` after every ingest, so dropping the
        // token here releases exactly what was reserved.
        self.columnar_memtable_mem.remove(&key);

        tracing::info!(
            collection,
            rows = meta.row_count,
            replay_prefix,
            applied_ranges,
            "timeseries columnar flush complete"
        );

        let registry = self.ts_registries.entry(key).or_insert_with(|| {
            PartitionRegistry::new(
                nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
            )
        });
        let row_count = meta.row_count;
        let mut reg_meta = meta;
        reg_meta.min_ts = drain.min_ts;
        reg_meta.max_ts = drain.max_ts;
        reg_meta.state = nodedb_types::timeseries::PartitionState::Sealed;
        // `insert_partition`, not `import`: two flushes can share a min_ts, a
        // late row's partition among them, and filing both under it would drop
        // one from the registry.
        registry.insert_partition(
            crate::engine::timeseries::partition_registry::PartitionEntry {
                meta: reg_meta,
                dir_name: partition_name.clone(),
            },
        );
        // The ingest rate an `AUTO` interval adapts to. A declared interval
        // ignores it.
        registry.record_ingest(row_count, now_ms);
        registry.maybe_narrow_interval();

        // Fold the new partition into the continuous aggregates rooted here.
        // Its files are durable, so an aggregate image that names it never
        // names a partition a crash can lose.
        //
        // The derived rows this writes are deliberately NOT put through the
        // row-level-security write gate. An aggregate refresh is system work: it
        // is triggered by a flush rather than by a statement, so there is no
        // `PhysicalPlan` to carry a compiled predicate and no requesting
        // identity to resolve `$auth.*` against — the same reason those
        // references cannot resolve anywhere else on an internal path. Gating it
        // on a predicate that cannot be resolved would fail closed and silently
        // stop every continuous aggregate on a governed collection. The rows it
        // derives were already decided by the policy when they were ingested.
        let refreshed = self.continuous_agg_mgr.on_flush(
            database_id.as_u64(),
            tid.as_u64(),
            collection,
            &partition_name,
            &drain,
            now_ms,
        );
        if !refreshed.is_empty() {
            tracing::debug!(
                collection,
                aggregates = ?refreshed,
                "continuous aggregates refreshed on flush"
            );
        }

        Ok(())
    }

    /// Charge the memtable's current resident footprint, replacing the
    /// prior charge so the budget tracks `memory_bytes()`, not the sum of
    /// every recharge.
    ///
    /// Runs after every ingest and WAL replay. The bytes are already
    /// resident, so the charge cannot be denied: hiding them would let
    /// `worst_engine_pressure` under-report while the engine holds the most.
    /// `flush_ts_collection` drops the charge when it drains the memtable.
    pub(in crate::data::executor) fn recharge_ts_memtable_budget(
        &mut self,
        tid: TenantId,
        db_id: DatabaseId,
        collection: &str,
    ) {
        let gov = self.governor.clone();
        let key = (db_id, tid, collection.to_string());
        let bytes = match self.columnar_memtables.get(&key) {
            Some(mt) => mt.memory_bytes(),
            None => {
                self.columnar_memtable_mem.remove(&key);
                return;
            }
        };
        // Release the prior reservation first so a recharge of an
        // unchanged memtable nets to zero rather than double-counting.
        self.columnar_memtable_mem.remove(&key);
        if bytes == 0 {
            return;
        }
        let mem = nodedb_mem::ScopedMemory::new(gov, db_id, tid, nodedb_mem::EngineId::Timeseries);
        self.columnar_memtable_mem.insert(key, mem.charge(bytes));
    }
}

/// A flush whose rows all fall in one interval.
struct SinglePartition<'a> {
    segment_dir: &'a Path,
    view: &'a ColumnarFlushView<'a>,
    interval_ms: u64,
    stamp: &'a TsReplayStamp,
    flush_wal_lsn: u64,
    kek: Option<&'a nodedb_wal::crypto::WalEncryptionKey>,
    collection: &'a str,
}

/// Write every row of the view as one partition. Its `partition.meta` is the
/// commit point, and the stamp lands before it, so a committed partition
/// always carries the stamp naming its rows.
fn write_single_partition(p: SinglePartition<'_>) -> crate::Result<(String, PartitionMeta)> {
    let SinglePartition {
        segment_dir,
        view,
        interval_ms,
        stamp,
        flush_wal_lsn,
        kek,
        collection,
    } = p;
    let partition_name =
        unique_partition_name(segment_dir, view.min_ts, view.max_ts, flush_wal_lsn)?;
    let partition_dir = segment_dir.join(&partition_name);
    std::fs::create_dir_all(&partition_dir).map_err(|e| crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("create partition dir {}: {e}", partition_dir.display()),
    })?;
    write_ts_stamp(&partition_dir, stamp)?;
    let meta = ColumnarSegmentWriter::new(segment_dir)
        .write_partition(&partition_name, view, interval_ms, flush_wal_lsn, kek)
        .map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("columnar flush failed for collection {collection}: {e}"),
        })?;
    Ok((partition_name, meta))
}

/// Pick a partition directory name that no existing partition owns.
///
/// The timestamp span alone is NOT an identity: late or duplicate-timestamp
/// ingest makes two flushes span the same `(min_ts, max_ts)`, and
/// `write_partition` rewrites each file in place — so a name collision replaces
/// rows a checkpoint has already reported durable. The flush LSN separates the
/// ordinary case; the probe closes the remainder, since the LSN can repeat when
/// a flush drains rows that arrived under an already-stamped record.
///
/// The `ts-` prefix is load-bearing: the boot registry scan and the orphan
/// sweeper both key on it.
pub(super) fn unique_partition_name(
    segment_dir: &Path,
    min_ts: i64,
    max_ts: i64,
    flush_wal_lsn: u64,
) -> crate::Result<String> {
    let base = format!("ts-{min_ts}_{max_ts}-{flush_wal_lsn:016x}");
    if !name_taken(segment_dir, &base)? {
        return Ok(base);
    }
    for suffix in 1u32..=u32::MAX {
        let candidate = format!("{base}-{suffix}");
        if !name_taken(segment_dir, &candidate)? {
            return Ok(candidate);
        }
    }
    // Handing back a taken name would rewrite a partition a checkpoint has
    // already reported durable — the exact defect the probe exists to prevent —
    // so the flush fails and retries instead. Its rows stay in the memtable and
    // the WAL behind them stays.
    Err(crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("no free partition directory name remains for {base}"),
    })
}

/// Whether `name` already names an entry under `segment_dir`.
///
/// `try_exists`, not `exists`: the latter reports a permission or I/O failure as
/// "absent", and treating an unreadable directory as free would hand the flush a
/// name whose files `write_partition` then rewrites in place.
fn name_taken(segment_dir: &Path, name: &str) -> crate::Result<bool> {
    segment_dir
        .join(name)
        .try_exists()
        .map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("probe partition directory {name}: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::unique_partition_name;

    #[test]
    fn distinct_spans_get_distinct_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = unique_partition_name(dir.path(), 1, 2, 10).expect("name");
        let b = unique_partition_name(dir.path(), 3, 4, 11).expect("name");
        assert_ne!(a, b);
        assert!(a.starts_with("ts-"), "boot scan keys on the ts- prefix");
    }

    /// The defect this guards: a second flush spanning the same timestamps and
    /// carrying the same flush LSN must not target the first flush's directory,
    /// whose files `write_partition` would rewrite in place.
    #[test]
    fn identical_span_and_lsn_does_not_reuse_a_live_directory() {
        let dir = tempfile::tempdir().expect("tempdir");

        let first = unique_partition_name(dir.path(), 100, 100, 42).expect("name");
        std::fs::create_dir_all(dir.path().join(&first)).expect("mkdir");

        let second = unique_partition_name(dir.path(), 100, 100, 42).expect("name");
        assert_ne!(first, second);
        std::fs::create_dir_all(dir.path().join(&second)).expect("mkdir");

        let third = unique_partition_name(dir.path(), 100, 100, 42).expect("name");
        assert_ne!(third, first);
        assert_ne!(third, second);
    }

    #[test]
    fn a_different_lsn_alone_separates_the_span() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = unique_partition_name(dir.path(), 100, 100, 42).expect("name");
        std::fs::create_dir_all(dir.path().join(&first)).expect("mkdir");
        let second = unique_partition_name(dir.path(), 100, 100, 43).expect("name");
        assert_ne!(first, second);
        // The LSN alone separated them, so no probe suffix was appended.
        assert_eq!(second, format!("ts-100_100-{:016x}", 43u64));
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The rows of a continuous aggregate read at a system-time cut `S`.
//!
//! A flush-driven aggregate (`OnFlush`, `OnSeal`) is a view over its root's
//! rows, so a read at `S` returns the aggregate of every row visible at `S`.
//! The read first folds every partition the aggregate lacks, so its buckets
//! hold every flushed row. Then:
//!
//! - A bucket with no row newer than `S` (`max_ordinal <= S`) holds exactly
//!   the flushed rows of its range visible at `S`, and is used as is.
//! - A bucket that holds a newer row is rebuilt from the raw partitions
//!   visible at `S`, filtered to rows written at or below `S`.
//! - A TRUNCATE after `S` reset every bucket, so every range is rebuilt.
//! - The memtable rows written at or below `S` fold into every range: no
//!   bucket holds them yet.
//!
//! Raw partitions answer a range only while retention removed none of it:
//! the image records the newest time-key value retention removed. When a
//! rebuilt range reaches it, the read is refused as a serialization
//! conflict, and the transaction retries at a later snapshot.
//!
//! A cut-driven aggregate (`Periodic`, `Manual`) serves its last refresh
//! cut, as a materialized view serves its last refresh. Its buckets equal
//! its state at `S` while no cut and no TRUNCATE landed after `S`.
//!
//! A refresh cut or a bucket retention pass after `S` changed buckets in a
//! way the rows cannot replay, so a read below either is refused.

use std::collections::BTreeSet;

use nodedb_columnar::VersionCut;
use nodedb_types::timeseries::TimeRange;

use super::cagg_fold::CaggRoot;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::continuous_agg::refresh::{Buckets, merge_delta};
use crate::engine::timeseries::continuous_agg::{ColumnLayout, PartialAggregate, RowCut};
use crate::engine::timeseries::time_bucket::time_bucket;
use crate::types::{DatabaseId, TenantId};

/// The aggregate a read at a cut targets.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct CaggReadAt<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    /// Root raw source of the aggregate.
    pub root: &'a str,
    pub aggregate: &'a str,
    /// The system-time ordinal the read sees.
    pub at: i64,
}

/// The bucket ranges a read rebuilds from raw rows.
enum Rebuild {
    /// Every range: a TRUNCATE after the cut reset the buckets.
    All,
    /// The ranges starting at these bucket timestamps.
    Buckets(BTreeSet<i64>),
}

impl Rebuild {
    fn covers(&self, bucket_ts: i64) -> bool {
        match self {
            Self::All => true,
            Self::Buckets(set) => set.contains(&bucket_ts),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::All => false,
            Self::Buckets(set) => set.is_empty(),
        }
    }

    /// The time-key range the rebuilt buckets span, `interval` wide each.
    fn range(&self, interval: i64) -> TimeRange {
        match self {
            Self::All => TimeRange::new(i64::MIN, i64::MAX),
            Self::Buckets(set) => {
                let first = set.first().copied().unwrap_or(i64::MAX);
                let last = set.last().copied().unwrap_or(i64::MIN);
                TimeRange::new(first, last.saturating_add(interval).saturating_sub(1))
            }
        }
    }

    /// The oldest time-key value a rebuilt bucket reads.
    fn start(&self) -> i64 {
        match self {
            Self::All => i64::MIN,
            Self::Buckets(set) => set.first().copied().unwrap_or(i64::MAX),
        }
    }
}

/// The refusal of a read no source answers exactly at its cut.
fn conflict(read: &CaggReadAt<'_>, why: String) -> crate::Error {
    crate::Error::ConflictRetry {
        collection: read.aggregate.to_string(),
        document_id: format!(
            "continuous aggregate '{}' has no exact state at snapshot {}: {why}; \
             retry the transaction",
            read.aggregate, read.at
        ),
    }
}

impl CoreLoop {
    /// The buckets of `read.aggregate` a read at `read.at` sees. The caller
    /// has folded every partition the aggregate owes.
    pub(in crate::data::executor) fn cagg_buckets_at(
        &self,
        read: CaggReadAt<'_>,
    ) -> crate::Result<Buckets> {
        let (db, tenant) = (read.database_id.as_u64(), read.tid.as_u64());
        let mgr = &self.continuous_agg_mgr;
        let def = mgr
            .get_definition(db, tenant, read.aggregate)
            .ok_or_else(|| crate::Error::UndefinedObject {
                kind: "continuous aggregate",
                name: read.aggregate.to_string(),
            })?;
        if let Some(rewritten) = mgr
            .rewritten_at(db, tenant, read.root, read.aggregate)
            .filter(|ordinal| *ordinal > read.at)
        {
            return Err(conflict(
                &read,
                format!("a refresh cut or bucket retention rewrote it at {rewritten}"),
            ));
        }
        let truncated_after = self
            .ts_newest_truncate_of(&(read.database_id, read.tid, read.root.to_string()))
            .is_some_and(|ordinal| ordinal > read.at);
        let layout = ColumnLayout::of(def);
        let current: Vec<&PartialAggregate> = mgr
            .get_materialized(db, tenant, read.aggregate)
            .unwrap_or_default();

        if mgr.folds_at_cuts(db, tenant, read.aggregate) == Some(true) {
            if truncated_after {
                return Err(conflict(
                    &read,
                    "a TRUNCATE after the snapshot reset its buckets".into(),
                ));
            }
            let mut out = Buckets::new();
            for bucket in current {
                add_bucket(&mut out, bucket, &layout);
            }
            return Ok(out);
        }

        let rebuild = if truncated_after {
            Rebuild::All
        } else {
            Rebuild::Buckets(
                current
                    .iter()
                    .filter(|bucket| bucket.max_ordinal > read.at)
                    .map(|bucket| bucket.bucket_ts)
                    .collect(),
            )
        };
        let mut out = Buckets::new();
        for bucket in current {
            if !rebuild.covers(bucket.bucket_ts) {
                add_bucket(&mut out, bucket, &layout);
            }
        }
        if !rebuild.is_empty() {
            self.rebuild_from_partitions(&read, &rebuild, def.bucket_interval_ms, &mut out)?;
        }
        self.fold_memtable_at(&read, &mut out, &layout);
        Ok(out)
    }

    /// Fold into `out` the partition rows visible at `read.at` whose
    /// buckets `rebuild` covers.
    fn rebuild_from_partitions(
        &self,
        read: &CaggReadAt<'_>,
        rebuild: &Rebuild,
        interval: i64,
        out: &mut Buckets,
    ) -> crate::Result<()> {
        let (db, tenant) = (read.database_id.as_u64(), read.tid.as_u64());
        let mgr = &self.continuous_agg_mgr;
        if let Some(through) = mgr
            .raw_dropped_through(db, tenant, read.root)
            .filter(|through| *through >= rebuild.start())
        {
            return Err(conflict(
                read,
                format!(
                    "rows newer than the snapshot changed its buckets, and retention removed \
                     the raw rows through {through}"
                ),
            ));
        }
        let key = (read.database_id, read.tid, read.root.to_string());
        let dirs: Vec<String> = self
            .ts_registries
            .get(&key)
            .map(|registry| {
                registry
                    .query_partitions(&rebuild.range(interval), VersionCut::At(read.at))
                    .into_iter()
                    .map(|entry| entry.dir_name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let layout = mgr
            .get_definition(db, tenant, read.aggregate)
            .map(ColumnLayout::of);
        let Some(layout) = layout else {
            return Ok(());
        };
        let keep_ts = |ts: i64| rebuild.covers(time_bucket(interval, ts));
        let cut = RowCut {
            at: read.at,
            keep_ts: &keep_ts,
        };
        let target = CaggRoot {
            database_id: db,
            tenant_id: tenant,
            root: read.root,
        };
        for dir in dirs {
            let rows = self.read_cagg_source(target, &dir)?;
            let delta = mgr.delta_at(db, tenant, read.aggregate, &rows.source(), &cut);
            merge_delta(out, &delta, &layout);
        }
        Ok(())
    }

    /// Fold into `out` the memtable rows written at or below `read.at`.
    fn fold_memtable_at(&self, read: &CaggReadAt<'_>, out: &mut Buckets, layout: &ColumnLayout) {
        let key = (read.database_id, read.tid, read.root.to_string());
        let Some(memtable) = self.columnar_memtables.get(&key) else {
            return;
        };
        let view = memtable.flush_view();
        let src = crate::engine::timeseries::continuous_agg::RefreshSource {
            schema: view.schema,
            columns: view.columns,
            symbol_dicts: view.symbol_dicts,
            row_count: usize::try_from(view.row_count).unwrap_or(usize::MAX),
        };
        let keep_all = |_: i64| true;
        let cut = RowCut {
            at: read.at,
            keep_ts: &keep_all,
        };
        let delta = self.continuous_agg_mgr.delta_at(
            read.database_id.as_u64(),
            read.tid.as_u64(),
            read.aggregate,
            &src,
            &cut,
        );
        merge_delta(out, &delta, layout);
    }
}

/// Add a copy of `bucket` to `out`.
fn add_bucket(out: &mut Buckets, bucket: &PartialAggregate, layout: &ColumnLayout) {
    out.entry((bucket.bucket_ts, bucket.group_key.clone()))
        .or_insert_with(|| {
            PartialAggregate::new(bucket.bucket_ts, bucket.group_key.clone(), layout)
        })
        .merge(bucket);
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, ingest, register,
    };
    use super::{CaggReadAt, ColumnLayout};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::{AggregateExpr, RefreshPolicy};
    use crate::types::{DatabaseId, TenantId};

    /// `ROWS_A` is written at this ordinal, `ROWS_B` one millisecond later.
    const WRITTEN_A: i64 = 7_000 * nodedb_types::NANOS_PER_MS;
    const WRITTEN_B: i64 = 8_000 * nodedb_types::NANOS_PER_MS;

    /// Ingest `rows` at `ordinal`.
    fn ingest_at(core: &mut CoreLoop, rows: &str, lsn: u64, ordinal: i64) {
        core.apply_scope.replicated_ordinal = Some(ordinal);
        assert_eq!(ingest(core, rows, lsn), Status::Ok);
        core.apply_scope.replicated_ordinal = None;
    }

    fn read_at(at: i64) -> CaggReadAt<'static> {
        CaggReadAt {
            database_id: DatabaseId::DEFAULT,
            tid: TenantId::new(TENANT),
            root: COLLECTION,
            aggregate: "by_host",
            at,
        }
    }

    /// Total `(COUNT, SUM)` of the buckets a read at `at` sees.
    fn totals_at(core: &mut CoreLoop, at: i64) -> crate::Result<(i64, i64)> {
        core.complete_cagg_root(0, TENANT, COLLECTION, 0)?;
        let buckets = core.cagg_buckets_at(read_at(at))?;
        let def = core
            .continuous_agg_mgr
            .get_definition(0, TENANT, "by_host")
            .expect("registered");
        let layout = ColumnLayout::of(def);
        let int = |bucket: &crate::engine::timeseries::continuous_agg::PartialAggregate,
                   expr: &AggregateExpr| match bucket.finalize(expr, &layout) {
            Ok(nodedb_types::Value::Integer(i)) => i,
            Ok(nodedb_types::Value::Null) => 0,
            other => panic!("unexpected {other:?}"),
        };
        Ok(buckets.values().fold((0, 0), |(c, s), bucket| {
            (
                c + int(bucket, &def.aggregates[0]),
                s + int(bucket, &def.aggregates[1]),
            )
        }))
    }

    /// The bucket holding a row newer than the snapshot is rebuilt from
    /// the raw rows visible at the snapshot. Memtable rows at or below the
    /// snapshot count, newer ones do not.
    #[test]
    fn a_read_at_a_cut_sees_only_the_rows_written_by_then() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        ingest_at(&mut core, ROWS_A, 1, WRITTEN_A);
        flush(&mut core);
        ingest_at(&mut core, ROWS_B, 2, WRITTEN_B);

        // `ROWS_B` sits in the memtable.
        assert_eq!(totals_at(&mut core, WRITTEN_A).expect("read"), (3, 111));
        assert_eq!(totals_at(&mut core, WRITTEN_B).expect("read"), (5, 11_111));

        // `ROWS_B` folds into the bucket `ROWS_A` sits in.
        flush(&mut core);
        assert_eq!(totals_at(&mut core, WRITTEN_A).expect("read"), (3, 111));
        assert_eq!(totals_at(&mut core, WRITTEN_B).expect("read"), (5, 11_111));
        assert_eq!(totals_at(&mut core, WRITTEN_A - 1).expect("read"), (0, 0));
    }

    /// A read below a rewrite, or one whose rebuild needs raw rows
    /// retention removed, is refused.
    #[test]
    fn a_read_no_source_answers_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        ingest_at(&mut core, ROWS_A, 1, WRITTEN_A);
        flush(&mut core);
        ingest_at(&mut core, ROWS_B, 2, WRITTEN_B);
        flush(&mut core);

        core.continuous_agg_mgr
            .note_raw_dropped(0, TENANT, COLLECTION, 1_700_000_000_000);
        assert!(
            totals_at(&mut core, WRITTEN_A).is_err(),
            "the bucket needs rebuilding and its raw rows are gone"
        );
        assert_eq!(
            totals_at(&mut core, WRITTEN_B).expect("the buckets are exact"),
            (5, 11_111)
        );

        core.continuous_agg_mgr
            .note_rewritten(0, TENANT, COLLECTION, "by_host", WRITTEN_B + 1);
        assert!(totals_at(&mut core, WRITTEN_B).is_err());
        assert!(totals_at(&mut core, WRITTEN_B + 1).is_ok());
    }
}

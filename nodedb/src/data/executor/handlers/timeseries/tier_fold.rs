// SPDX-License-Identifier: BUSL-1.1

//! Fold AUTO_TIER downsample buckets into a grouped timeseries aggregate.
//!
//! A tier is a continuous aggregate with no GROUP BY columns. Each of its
//! buckets holds the row count and, per source column, the exact total and
//! the extremes. Those merge exactly into COUNT, SUM, AVG, MIN and MAX of a
//! query with no GROUP BY columns whose time bucket, if any, is a multiple
//! of the tier's resolution. `tier_route::refuse_unmergeable` refuses every
//! other shape before this runs.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_agg::AggAccum;
use crate::engine::timeseries::continuous_agg::{ColumnLayout, PartialAggregate};
use crate::engine::timeseries::grouped_scan::{GroupKey, GroupedAggResult};
use crate::engine::timeseries::time_bucket::time_bucket;
use crate::types::{DatabaseId, TenantId};

use super::tier_route::{TierSpan, TierSplit};

/// Inputs to [`CoreLoop::fold_tier_buckets`].
pub(super) struct TierFold<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    /// The scanned collection: the root every tier of its policy hangs from.
    pub collection: &'a str,
    pub spans: &'a [TierSpan],
    pub aggregates: &'a [(String, String)],
    pub bucket_interval_ms: i64,
    pub now_ms: i64,
}

impl CoreLoop {
    /// The grouped state of every tier bucket inside `spans`.
    ///
    /// The tiers first fold every flushed partition they lack, so each
    /// bucket counts every flushed row of its range. Every tier, tier 2 and
    /// up included, is registered on every core and rolls down from the
    /// root collection on the core that owns it, which is the core that
    /// runs this scan. An unregistered tier, a stale tier, and a column a
    /// tier does not keep are errors.
    pub(super) fn fold_tier_buckets(&mut self, p: TierFold<'_>) -> crate::Result<GroupedAggResult> {
        let db = p.database_id.as_u64();
        let tenant = p.tid.as_u64();
        self.complete_cagg_backfill(db, tenant, p.collection, p.now_ms)?;

        let mut result = GroupedAggResult::new(p.aggregates.len());
        for span in p.spans {
            let name = span.tier.aggregate.as_str();
            let def = self
                .continuous_agg_mgr
                .get_definition(db, tenant, name)
                .ok_or(crate::Error::BadRequest {
                    detail: format!(
                        "AUTO_TIER on '{}': tier {} aggregate '{name}' is not registered; the \
                         retention policy changed while the query ran",
                        p.collection, span.tier.tier_index
                    ),
                })?;
            if def.stale {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "AUTO_TIER on '{}': tier {} aggregate '{name}' is stale; refresh it \
                         before reading its range",
                        p.collection, span.tier.tier_index
                    ),
                });
            }
            let layout = ColumnLayout::of(def);
            // `[start, end)` holds whole buckets: the range read is inclusive.
            let buckets = self
                .continuous_agg_mgr
                .get_materialized_range(db, tenant, name, span.start, span.end.saturating_sub(1))
                .unwrap_or_default();
            for bucket in buckets {
                let key = GroupKey {
                    bucket: (p.bucket_interval_ms > 0)
                        .then(|| time_bucket(p.bucket_interval_ms, bucket.bucket_ts)),
                    cells: Vec::new(),
                };
                let accums = p
                    .aggregates
                    .iter()
                    .map(|(op, field)| tier_accum(bucket, &layout, op, field, name))
                    .collect::<crate::Result<Vec<AggAccum>>>()?;
                let mut one = GroupedAggResult::new(p.aggregates.len());
                one.groups.insert(key, accums);
                result.merge(&one);
            }
        }
        Ok(result)
    }
}

impl CoreLoop {
    /// `split` as a read at the transaction snapshot `at` takes it: every
    /// tier span exact at `at` stays a tier span, and the rest move to raw
    /// rows.
    ///
    /// The tiers first fold every flushed partition they lack, as
    /// [`Self::fold_tier_buckets`] does. Spans run newest first, so the
    /// oldest span not exact at `at` and every newer span read raw rows,
    /// and the raw range grows down to that span's start.
    ///
    /// Raw rows answer that range exactly only while retention removed no
    /// partition reaching into it: the image records the newest time-key
    /// value retention removed (`raw_dropped_through`). When it reaches the
    /// range, neither source holds the rows visible at `at`, and the read is
    /// refused as a conflict: the transaction retries at a later snapshot,
    /// where the tier is exact again.
    pub(super) fn exact_tier_split_at(
        &mut self,
        root: TierRoot<'_>,
        mut split: TierSplit,
        at: i64,
        now_ms: i64,
    ) -> crate::Result<TierSplit> {
        if split.tiers.is_empty() {
            return Ok(split);
        }
        let db = root.database_id.as_u64();
        let tenant = root.tid.as_u64();
        self.complete_cagg_backfill(db, tenant, root.collection, now_ms)?;
        let newest_truncate =
            self.ts_newest_truncate_of(&(root.database_id, root.tid, root.collection.to_string()));
        let mut oldest_inexact: Option<(usize, String)> = None;
        for (index, span) in split.tiers.iter().enumerate() {
            let buckets = self
                .continuous_agg_mgr
                .get_materialized_range(
                    db,
                    tenant,
                    &span.tier.aggregate,
                    span.start,
                    span.end.saturating_sub(1),
                )
                .unwrap_or_default();
            if let Some(reason) = tier_inexact_at(&buckets, newest_truncate, at) {
                oldest_inexact = Some((index, reason));
            }
        }
        let Some((index, reason)) = oldest_inexact else {
            return Ok(split);
        };
        let Some(from) = split.tiers.get(index).map(|span| span.start) else {
            return Ok(split);
        };
        let dropped = self
            .continuous_agg_mgr
            .raw_dropped_through(db, tenant, root.collection);
        if let Some(through) = dropped.filter(|through| *through >= from) {
            tracing::debug!(
                collection = root.collection,
                at,
                reason,
                raw_dropped_through = through,
                "AUTO_TIER range has no exact source at the transaction snapshot"
            );
            return Err(crate::Error::ConflictRetry {
                collection: root.collection.to_string(),
                document_id: format!(
                    "AUTO_TIER range from {from}: the tier is not exact at snapshot {at} \
                     ({reason}) and retention removed raw rows through {through}"
                ),
            });
        }
        let raw_end = match split.raw_range {
            Some((_, end)) => end,
            None => split
                .tiers
                .first()
                .map_or(from, |newest| newest.end.saturating_sub(1)),
        };
        split.tiers.drain(..=index);
        split.raw_range = Some((from, raw_end));
        Ok(split)
    }
}

/// The collection whose AUTO_TIER tiers a read uses.
#[derive(Clone, Copy)]
pub(super) struct TierRoot<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
}

/// Why the tier buckets of one span are not exact at ordinal `at`, or
/// `None` when they are.
///
/// A tier bucket is the fold of every flushed row of its range, and every
/// flushed row of the range reaches it before a read (the backfill). Rows
/// still in the memtable are read from the memtable. So the bucket equals
/// the fold of the flushed rows visible at `at` exactly when:
///
/// - no row it holds is newer than `at` (`max_ordinal <= at`), and
/// - no TRUNCATE above `at` reset it: a reset drops rows a read at `at`
///   still sees, and no later fold brings them back.
///
/// A raw fallback is not exact in general: retention removes the raw rows
/// of a tiered range once the tier folded them, so the tier holds their
/// only copy.
fn tier_inexact_at(
    buckets: &[&PartialAggregate],
    newest_truncate: Option<i64>,
    at: i64,
) -> Option<String> {
    if let Some(end) = newest_truncate.filter(|end| *end > at) {
        return Some(format!(
            "a TRUNCATE at {end} reset the tier after the snapshot"
        ));
    }
    buckets
        .iter()
        .find(|bucket| bucket.max_ordinal > at)
        .map(|bucket| {
            format!(
                "bucket {} holds a row written at {}, after the snapshot",
                bucket.bucket_ts, bucket.max_ordinal
            )
        })
}

/// One query aggregate's state over one tier bucket.
fn tier_accum(
    bucket: &PartialAggregate,
    layout: &ColumnLayout,
    op: &str,
    field: &str,
    tier: &str,
) -> crate::Result<AggAccum> {
    if op == "count" && field == "*" {
        return AggAccum::from_totals(bucket.count, Default::default(), None, None)
            .map_err(crate::Error::from);
    }
    let column = layout
        .slot(field)
        .and_then(|slot| bucket.columns.get(slot))
        .ok_or(crate::Error::BadRequest {
            detail: format!(
                "AUTO_TIER tier aggregate '{tier}' keeps no state for column '{field}', so \
                 {op}({field}) cannot be read from it"
            ),
        })?;
    AggAccum::from_totals(
        column.sum.count(),
        column.sum.clone(),
        column.min.clone(),
        column.max.clone(),
    )
    .map_err(crate::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::test_support::exact_def;

    fn bucket(bucket_ts: i64, max_ordinal: i64) -> PartialAggregate {
        let mut bucket = PartialAggregate::new(
            bucket_ts,
            Vec::new(),
            &ColumnLayout::of(&exact_def("t", "s", "1m")),
        );
        bucket.max_ordinal = max_ordinal;
        bucket
    }

    /// A tier answers a read at `S` only while no bucket holds a row newer
    /// than `S` and no truncate above `S` reset it.
    #[test]
    fn a_tier_is_exact_only_up_to_its_newest_row_and_truncate() {
        let old = bucket(0, 10);
        let late = bucket(60_000, 50);
        assert_eq!(tier_inexact_at(&[&old], None, 10), None);
        assert!(tier_inexact_at(&[&old, &late], None, 40).is_some());
        assert_eq!(tier_inexact_at(&[&old, &late], None, 50), None);
        assert!(
            tier_inexact_at(&[&old], Some(30), 20).is_some(),
            "a truncate after the snapshot reset the tier"
        );
        assert_eq!(tier_inexact_at(&[&old], Some(30), 30), None);
    }

    /// A tier whose buckets hold a row newer than the snapshot moves to raw
    /// rows, while retention removed none of them. Once retention removed
    /// raw rows of that range, the read is refused.
    #[test]
    fn a_snapshot_reads_raw_rows_where_the_tier_is_newer() {
        use super::super::cagg_test_support::{
            COLLECTION, ROWS_A, TENANT, flush, host_def, ingest, register,
        };
        use crate::bridge::envelope::Status;
        use crate::data::executor::core_loop::tests::make_core_with_dir;
        use crate::engine::timeseries::continuous_agg::RefreshPolicy;
        use nodedb_physical::physical_plan::TierRead;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        let written = 7_000 * nodedb_types::NANOS_PER_MS;
        core.apply_scope.replicated_ordinal = Some(written);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        core.apply_scope.replicated_ordinal = None;
        flush(&mut core);

        // `ROWS_A` lands in the hour bucket starting at `HOUR`.
        const HOUR: i64 = 1_699_999_200_000;
        const NEXT: i64 = HOUR + 3_600_000;
        let split = || TierSplit {
            raw_range: Some((NEXT, NEXT + 10_000)),
            tiers: vec![TierSpan {
                tier: TierRead {
                    tier_index: 1,
                    aggregate: "by_host".into(),
                    resolution_ms: 3_600_000,
                    retain_ms: 0,
                },
                start: HOUR,
                end: NEXT,
            }],
        };
        let root = TierRoot {
            database_id: DatabaseId::DEFAULT,
            tid: TenantId::new(TENANT),
            collection: COLLECTION,
        };

        let exact = core
            .exact_tier_split_at(root, split(), written, 0)
            .expect("exact at the write");
        assert_eq!(exact.tiers.len(), 1, "the tier is exact at its newest row");

        let before = core
            .exact_tier_split_at(root, split(), written - 1, 0)
            .expect("raw rows serve the range");
        assert!(before.tiers.is_empty());
        assert_eq!(before.raw_range, Some((HOUR, NEXT + 10_000)));

        core.continuous_agg_mgr
            .note_raw_dropped(0, TENANT, COLLECTION, HOUR + 1_000);
        assert!(
            core.exact_tier_split_at(root, split(), written - 1, 0)
                .is_err(),
            "neither source holds the rows the snapshot sees"
        );
    }
}

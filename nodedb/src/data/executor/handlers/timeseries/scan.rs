// SPDX-License-Identifier: BUSL-1.1

//! Data Plane timeseries scan parameters and execution.

use crate::bridge::envelope::{Payload, Response, Status};
use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::filter::decode_rls_filters;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use super::{aggregate, raw_scan};

/// Parameters for a timeseries scan operation.
pub(in crate::data::executor) struct TimeseriesScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filters: &'a [u8],
    /// The caller's read policy as a MessagePack `Vec<ScanFilter>`, injected
    /// by the planner. Empty when no policy governs the caller. The raw scan
    /// evaluates it per row after `filters`, before computed columns, sort,
    /// and limit, on memtable, partition, and in-transaction overlay rows
    /// alike. The aggregate branch pushes it into the grouped scan so it
    /// excludes rows before aggregation, and fails when the grouped scan
    /// cannot lower it.
    pub rls_filters: &'a [u8],
    /// `ORDER BY` keys as `(column, ascending)`. Applied to the materialized
    /// result before `limit`, on both the raw and the aggregate branch.
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    /// Gap-fill strategy. Empty = no gap-fill.
    pub gap_fill: &'a str,
    /// Serialized computed columns for scalar projection expressions.
    pub computed_columns: &'a [u8],
    /// System-time selection. `_ts_system` holds the system-time ordinal of
    /// the batch that wrote the row. `Pinned { ordinal }` hides rows above
    /// `ordinal`, `AsOf(ms)` hides rows above the last ordinal of `ms`,
    /// `AllVersions` emits every row ordered ascending by system time (audit
    /// log), and `Current` is unconstrained.
    pub system_time: nodedb_types::SystemTimeScope,
    /// Bitemporal valid-time point. `None` skips valid-time filtering.
    pub valid_at_ms: Option<i64>,
    /// HAVING predicates (MessagePack `Vec<ScanFilter>`) over the finalized
    /// group rows of the aggregate branch. Empty on a raw scan.
    pub having: &'a [u8],
    /// AUTO_TIER layout of the collection's retention policy, if any.
    pub tiering: Option<&'a nodedb_physical::physical_plan::TieringPlan>,
    /// `(encoded key, output name)` of each output column of the aggregate
    /// branch, in SELECT-list order. Empty keeps the encoded keys.
    pub output_columns: &'a [(String, String)],
}

impl CoreLoop {
    /// Execute a timeseries scan — the universal timeseries query path.
    ///
    /// Three modes:
    /// 1. Raw scan: `aggregates.is_empty()` — emit rows.
    /// 2. Time-bucket agg: `bucket_interval_ms > 0` — bucket + aggregate.
    /// 3. Generic GROUP BY: `!aggregates.is_empty()` — group + aggregate.
    pub(in crate::data::executor) fn execute_timeseries_scan(
        &mut self,
        params: TimeseriesScanParams<'_>,
    ) -> Response {
        let TimeseriesScanParams {
            task,
            tid,
            collection,
            time_range,
            limit,
            filters,
            rls_filters,
            sort_keys,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            system_time,
            valid_at_ms,
            having,
            tiering,
            output_columns,
        } = params;

        let all_versions = system_time.is_all_versions();
        let system_cutoff = system_time.as_of_ordinal();
        // The partitions a TRUNCATE ended at or below the cut hold no row the
        // read sees. The memtable holds only rows written after the newest
        // TRUNCATE, which flushed every earlier row into a partition it ended.
        let cut = nodedb_columnar::VersionCut::of_scope(system_time);

        // The collection's declared TIME_KEY drives partition pruning and
        // projection pushdown. Resolved once here and threaded through both
        // branches; nothing downstream guesses it from a column name.
        let time_key = self.ts_time_column(task.request.database_id, tid, collection);
        let time_key_kind = self.ts_time_key_kind(task.request.database_id, tid, collection);

        // Lazy-load partition registry from disk if not yet loaded.
        if let Err(e) = self.ensure_ts_registry(tid, task.request.database_id, collection) {
            return self.response_error(task, crate::bridge::envelope::ErrorCode::from(e));
        }

        // Both predicate sets are decoded before any row is read. A payload
        // the Data Plane cannot decode fails the statement: for the policy
        // that is the difference between hiding governed rows and returning
        // them, and for the WHERE clause it is the difference between the
        // asked-for rows and every row.
        let mut filter_predicates: Vec<ScanFilter> =
            match decode_scan_filters(filters, "timeseries filter") {
                Ok(p) => p,
                Err(e) => return self.response_error(task, e),
            };
        let rls_predicates: Vec<ScanFilter> = match decode_rls_filters(rls_filters) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        let having_predicates: Vec<ScanFilter> =
            match decode_scan_filters(having, "timeseries HAVING") {
                Ok(p) => p,
                Err(e) => return self.response_error(task, e),
            };
        // Temporal cutoffs: translate to column-level predicates on
        // `_ts_system` / `_ts_valid_from` / `_ts_valid_until`. The
        // segment reader's block-skip infrastructure applies these
        // against per-block min/max automatically. Every timeseries row
        // carries `_ts_system`, so the system cut applies to every
        // collection.
        if let Some(point) = valid_at_ms {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_VALID_FROM.into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_VALID_UNTIL.into(),
                op: crate::bridge::scan_filter::FilterOp::Gt,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
        }
        // A staged row carries no `_ts_system` yet: it is newer than every
        // committed version. A transaction snapshot read applies the system
        // cut to stored rows only, so the staged rows keep every other
        // predicate.
        let staged_filter_predicates = filter_predicates.clone();
        let staged_has_filters = !staged_filter_predicates.is_empty();
        if let Some(cutoff) = system_cutoff {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_SYSTEM.into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(cutoff),
                clauses: Vec::new(),
                expr: None,
            });
        }
        // Narrow the plan's envelope with the query's own bounds on the
        // declared time column. The Control Plane sends an unbounded range
        // precisely because only this core knows which column that is.
        let time_range = super::time_range::narrow_time_range(
            time_range,
            &filter_predicates,
            Some((time_key.as_str(), time_key_kind)),
        );

        let has_filters = !filter_predicates.is_empty();
        let is_aggregate = !aggregates.is_empty() || bucket_interval_ms > 0;
        let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;

        // In-transaction read-your-own-writes covers a current read and a
        // transaction snapshot read: audit-log (`all_versions`) and
        // `AS OF SYSTEM TIME` reads are committed-only.
        let overlay_txn = if system_time.merges_txn_overlay() {
            task.request.txn_id
        } else {
            None
        };

        // AUTO_TIER: the part of the range raw rows serve, and the parts
        // downsample tiers serve.
        let now_ms = i64::try_from(
            task.request
                .resolved_now_ms
                .unwrap_or_else(crate::engine::kv::current_ms),
        )
        .unwrap_or(i64::MAX);
        let split = match super::tier_route::split_tiers(tiering, time_range, now_ms) {
            Ok(split) => split,
            Err(e) => return self.response_error(task, e),
        };
        if !is_aggregate
            && let Err(e) = super::tier_route::refuse_raw_read_of_tiers(collection, &split)
        {
            return self.response_error(task, e);
        }
        // A transaction snapshot reads a tier only where the tier is exact
        // at the snapshot. Every other span reads raw rows at the snapshot.
        let split = match system_cutoff {
            Some(at) if is_aggregate && !system_time.is_historical() => {
                let root = super::tier_fold::TierRoot {
                    database_id: task.request.database_id,
                    tid,
                    collection,
                };
                match self.exact_tier_split_at(root, split, at, now_ms) {
                    Ok(split) => split,
                    Err(e) => return self.response_error(task, e),
                }
            }
            Some(_) | None => split,
        };
        // A tiered aggregate reads each tier's buckets for the tier's part
        // of the range, after checking the buckets merge exactly.
        let tier_partials = if is_aggregate && !split.tiers.is_empty() {
            // The query's own predicates, without the system cut pushed above:
            // every span left is exact at the cut (`exact_tier_split_at`), so
            // the cut holds for its buckets already.
            let row_predicates: Vec<ScanFilter> = staged_filter_predicates
                .iter()
                .chain(&rls_predicates)
                .cloned()
                .collect();
            for span in &split.tiers {
                let shape = super::tier_route::TierMergeShape {
                    group_by,
                    aggregates,
                    bucket_interval_ms,
                    filter_predicates: &row_predicates,
                    time_key: &time_key,
                    // A transaction snapshot keeps only the spans exact at
                    // the snapshot (`exact_tier_split_at`).
                    temporal: system_time.is_historical() || valid_at_ms.is_some(),
                };
                if let Err(e) = super::tier_route::refuse_unmergeable(collection, span, shape) {
                    return self.response_error(task, e);
                }
            }
            match self.fold_tier_buckets(super::tier_fold::TierFold {
                database_id: task.request.database_id,
                tid,
                collection,
                spans: &split.tiers,
                aggregates,
                bucket_interval_ms,
                now_ms,
            }) {
                Ok(partials) => Some(partials),
                Err(e) => return self.response_error(task, e),
            }
        } else {
            None
        };
        // Flushed partitions serve only the raw part of the range. The
        // memtable and the transaction's staged rows serve the whole range:
        // a tier folds rows only when their partition is flushed.
        let raw_range = split.raw_range;
        let time_range = if is_aggregate {
            time_range
        } else {
            raw_range.unwrap_or(time_range)
        };

        // The transaction's staged rows, for the grouped scan. The raw scan
        // folds them itself.
        let staged = match overlay_txn {
            Some(txn_id) if is_aggregate => {
                let coll_key = (task.request.database_id, tid, collection.to_string());
                match self.staged_timeseries_rows(
                    crate::data::executor::handlers::transaction::overlay::TimeseriesOverlayMergeParams {
                        txn_id,
                        coll_key: &coll_key,
                        time_range,
                        filter_predicates: &staged_filter_predicates,
                        has_filters: staged_has_filters,
                        rls_predicates: &rls_predicates,
                        limit: usize::MAX,
                    },
                ) {
                    Ok(staged) => Some(staged),
                    Err(e) => return self.response_error(task, e),
                }
            }
            Some(_) | None => None,
        };
        let has_staged = staged
            .as_ref()
            .is_some_and(|s| s.truncated || !s.rows.is_empty());

        // Fast path: COUNT(*) with no GROUP BY, no filters, no read policy,
        // no HAVING, and no staged rows. The metadata count covers every
        // stored row, so a governed read takes the grouped scan, where the
        // policy excludes rows first.
        if is_aggregate
            && bucket_interval_ms == 0
            && group_by.is_empty()
            && !has_filters
            && rls_predicates.is_empty()
            && having_predicates.is_empty()
            && !has_staged
            && tier_partials.is_none()
            && !has_time_range
            && aggregates.len() == 1
            && aggregates[0].0 == "count"
            && aggregates[0].1 == "*"
        {
            return self.execute_ts_count_star(
                task,
                tid,
                collection,
                time_range,
                cut,
                output_columns,
            );
        }

        // Determine needed columns (projection pushdown).
        let needed_columns: Vec<String> = if is_aggregate {
            // The aggregate pipeline always needs the time column: it is the
            // bucketing key and the ordering key.
            let mut needed: Vec<String> = vec![time_key.clone()];
            for g in group_by {
                if !needed.contains(g) {
                    needed.push(g.clone());
                }
            }
            for (_, field) in aggregates {
                if field != "*" && !needed.contains(field) {
                    needed.push(field.clone());
                }
            }
            for fp in filter_predicates.iter().chain(&rls_predicates) {
                if !needed.contains(&fp.field) {
                    needed.push(fp.field.clone());
                }
            }
            needed
        } else {
            Vec::new() // empty = read all columns
        };

        // Mode dispatch.
        if is_aggregate {
            // The policy joins the WHERE predicates so the grouped scan
            // excludes governed rows before any accumulator sees them. The
            // grouped scan fails on a predicate it cannot lower, so a policy
            // shape it does not support fails the statement rather than
            // aggregating ungoverned rows.
            let governed_predicates: Vec<ScanFilter> = filter_predicates
                .iter()
                .chain(&rls_predicates)
                .cloned()
                .collect();
            self.execute_ts_aggregate(aggregate::TsAggregateParams {
                task,
                tid,
                collection,
                time_range,
                limit,
                filter_predicates: &governed_predicates,
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                needed_columns: &needed_columns,
                sort_keys,
                having: &having_predicates,
                staged,
                raw_range,
                tier_partials,
                cut,
                output_columns,
            })
        } else {
            self.execute_ts_raw_scan(raw_scan::RawScanParams {
                task,
                tid,
                collection,
                time_range,
                limit,
                filter_predicates: &filter_predicates,
                staged_filter_predicates: &staged_filter_predicates,
                rls_predicates: &rls_predicates,
                computed_columns,
                all_versions,
                txn_id: overlay_txn,
                sort_keys,
                cut,
            })
        }
    }

    /// COUNT(*) metadata fast path — zero I/O. A partition a TRUNCATE
    /// ended at or below `cut` counts no row.
    ///
    /// The one row carries the count under each name `output_columns` gives
    /// it, as the grouped scan's rows do.
    fn execute_ts_count_star(
        &self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        time_range: (i64, i64),
        cut: nodedb_columnar::VersionCut,
        output_columns: &[(String, String)],
    ) -> Response {
        let key = (task.request.database_id, tid, collection.to_string());
        let mut total: u64 = 0;
        if let Some(mt) = self.columnar_memtables.get(&key) {
            total += mt.row_count();
        }
        if let Some(registry) = self.ts_registries.get(&key) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            for entry in registry.query_partitions(&query_range, cut) {
                total += entry.meta.row_count;
            }
        }
        let count_key = canonical_agg_key("count", "*");
        let row = rmpv::Value::Map(vec![(
            rmpv::Value::String(count_key.into()),
            rmpv::Value::Integer((total as i64).into()),
        )]);
        let row = super::encode::select_output_columns(row, output_columns);
        let array = rmpv::Value::Array(vec![row]);
        let mut buf = Vec::new();
        if let Err(e) = rmpv::encode::write_value(&mut buf, &array) {
            return self.response_error(
                task,
                crate::Error::Codec {
                    detail: format!("timeseries COUNT(*) result encode: {e}"),
                },
            );
        }
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(buf),
            watermark_lsn: self.watermark,
            error_code: None,
            stage_vote: None,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }
}

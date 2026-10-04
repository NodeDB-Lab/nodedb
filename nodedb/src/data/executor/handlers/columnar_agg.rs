// SPDX-License-Identifier: BUSL-1.1

//! Native columnar aggregation for GROUP BY queries on columnar memtables.
//!
//! Bypasses the generic document-style aggregation path that converts
//! columnar data → serde_json::Map → msgpack → JSON string keys.
//! Instead, filters and groups directly on column vectors using integer
//! symbol IDs as group keys. Resolves symbol names only for the final
//! response.
//!
//! Performance: ~36x faster than the generic path for high-cardinality
//! GROUP BY (e.g., 10K+ unique qnames) because it eliminates:
//! - Per-row serde_json::Map construction
//! - Per-row msgpack encode/decode roundtrip
//! - JSON-serialized string HashMap keys
//! - Per-row symbol dictionary string allocation

use std::collections::HashMap;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use nodedb_query::agg_key::canonical_agg_key;

use super::columnar_agg_support::{
    AggAccum, DenseSymbolParams, GroupKey, GroupKeyPart, aggregate_dense_symbol,
    extract_group_key_part, for_each_set_bit, resolve_key_part,
};
use super::columnar_filter;
use super::spill::columnar::ColumnarGroupBySpiller;

/// Result of native columnar aggregation.
pub(super) struct ColumnarAggResult {
    pub rows: Vec<serde_json::Value>,
}

/// Parameters for [`try_columnar_aggregate`].
pub(super) struct ColumnarAggParams<'a> {
    pub mt: &'a ColumnarMemtable,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub filters: &'a [crate::bridge::scan_filter::ScanFilter],
    pub limit: usize,
    pub scan_limit: usize,
    pub spill_dir: &'a std::path::Path,
    pub spill_cap: usize,
    pub governor: std::sync::Arc<nodedb_mem::MemoryGovernor>,
    /// Database the query executes against, for memory governor scoping.
    pub db: crate::types::DatabaseId,
    /// Tenant the query executes on behalf of.
    pub tenant: crate::types::TenantId,
}

/// Try to execute an aggregate query natively on a columnar memtable.
///
/// Returns `None` if the query can't be handled natively (complex filters,
/// string comparison filters, etc.), in which case the caller should fall
/// back to the generic document-style path.
///
/// `p.spill_dir` and `p.spill_cap` control GROUP BY spill-to-disk: if the
/// number of distinct groups exceeds `spill_cap`, partial accumulators are
/// spilled to `spill_dir` and k-way merged at finalize time.
///
/// Fails with `EvalError::NumericOverflow` when an exact integer SUM total
/// lies outside the `Decimal` range.
pub(super) fn try_columnar_aggregate(
    p: &ColumnarAggParams<'_>,
) -> Result<Option<ColumnarAggResult>, nodedb_query::EvalError> {
    let Some(acc) = accumulate_groups(p) else {
        return Ok(None);
    };
    build_results_from_groups(
        &acc.groups,
        p.group_by,
        &acc.group_col_info,
        p.aggregates,
        p.mt,
        p.limit,
    )
    .map(Some)
}

/// Per-group accumulators and the resolved GROUP BY column info.
struct AccumulatedGroups {
    groups: HashMap<GroupKey, Vec<AggAccum>>,
    group_col_info: Vec<(usize, ColumnType)>,
}

/// Filter, group, and accumulate the memtable rows. `None` when the query
/// cannot run natively.
fn accumulate_groups(p: &ColumnarAggParams<'_>) -> Option<AccumulatedGroups> {
    let (mt, group_by, aggregates, filters) = (p.mt, p.group_by, p.aggregates, p.filters);
    let (scan_limit, spill_dir, spill_cap) = (p.scan_limit, p.spill_dir, p.spill_cap);
    let (db, tenant) = (p.db, p.tenant);
    let schema = mt.schema();
    let row_count = (mt.row_count() as usize).min(scan_limit);

    if row_count == 0 {
        return Some(AccumulatedGroups {
            groups: HashMap::new(),
            group_col_info: Vec::new(),
        });
    }

    // --- Phase 1: Resolve column indices for group-by and aggregate fields ---

    let group_col_info: Vec<(usize, ColumnType)> = group_by
        .iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _))| n == name)
                .map(|(i, (_, ty))| (i, *ty))
        })
        .collect::<Option<Vec<_>>>()?;

    // For each aggregate, find the column index (None for count(*)).
    let agg_col_info: Vec<(usize, ColumnType)> = aggregates
        .iter()
        .filter(|(_, field)| field != "*")
        .map(|(_, field)| {
            schema
                .columns
                .iter()
                .enumerate()
                .find(|(_, (n, _))| n == field)
                .map(|(i, (_, ty))| (i, *ty))
        })
        .collect::<Option<Vec<_>>>()?;

    // Only handle numeric aggregation columns (Float64, Int64, Timestamp).
    for (_, ty) in &agg_col_info {
        if *ty == ColumnType::Symbol {
            return None; // Can't SUM/AVG a symbol column
        }
    }

    // --- Phase 2: Build filter mask ---

    // Try SIMD bitmask path first; fall back to dense bool mask; fail on complex filters.
    enum FilterResult {
        Bitmask(Vec<u64>),
        BoolMask(Vec<bool>),
        None,
    }

    let filter_result = if filters.is_empty() {
        FilterResult::None
    } else {
        match columnar_filter::eval_filters_bitmask(mt, filters, row_count) {
            Some(bm) => FilterResult::Bitmask(bm),
            None => {
                let mask = columnar_filter::eval_filters_dense(mt, filters, row_count)?;
                FilterResult::BoolMask(mask)
            }
        }
    };

    // Pre-fetch aggregate column data. For count(*), we don't need column data.
    let agg_col_data: Vec<Option<(usize, &ColumnData)>> = aggregates
        .iter()
        .map(|(_, field)| {
            if field == "*" {
                None
            } else {
                schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, (n, _))| n == field)
                    .map(|(i, _)| (i, mt.column(i)))
            }
        })
        .collect();

    // --- Phase 3: Group and accumulate ---

    // Fast path: single Symbol GROUP BY column with cardinality ≤ 65536.
    // Replaces HashMap with a dense array indexed by symbol ID.
    let dense_result: Option<Vec<(u32, Vec<AggAccum>)>> =
        if group_col_info.len() == 1 && group_col_info[0].1 == ColumnType::Symbol {
            let col_idx = group_col_info[0].0;
            if let Some(dict) = mt.symbol_dict(col_idx) {
                let cardinality = dict.len();
                if cardinality <= 65_536 {
                    let (bm, boolm) = match &filter_result {
                        FilterResult::Bitmask(bm) => (Some(bm.as_slice()), None),
                        FilterResult::BoolMask(m) => (None, Some(m.as_slice())),
                        FilterResult::None => (None, None),
                    };
                    Some(aggregate_dense_symbol(&DenseSymbolParams {
                        mt,
                        group_col_idx: col_idx,
                        agg_col_data: &agg_col_data,
                        aggregates,
                        bitmask: bm,
                        bool_mask: boolm,
                        row_count,
                        cardinality,
                    }))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

    let groups: HashMap<GroupKey, Vec<AggAccum>> = if let Some(dense) = dense_result {
        dense
            .into_iter()
            .map(|(sym_id, accums)| (vec![GroupKeyPart::SymbolId(sym_id)], accums))
            .collect()
    } else {
        // HashMap path with spill-to-disk: multi-column GROUP BY, numeric keys,
        // or high-cardinality symbols.
        let num_aggs = aggregates.len();
        let group_col_data: Vec<_> = group_col_info
            .iter()
            .map(|&(idx, ty)| (idx, ty, mt.column(idx)))
            .collect();

        let mut spiller = match ColumnarGroupBySpiller::new(
            spill_dir.to_path_buf(),
            spill_cap,
            p.governor.clone(),
            db,
            tenant,
        ) {
            Ok(s) => s,
            Err(_) => {
                // If we can't create the spill dir, fall back to no-spill path.
                let mut groups: HashMap<GroupKey, Vec<AggAccum>> = HashMap::with_capacity(1024);
                let mut process_row = |row_idx: usize| {
                    let key: GroupKey = if group_by.is_empty() {
                        Vec::new()
                    } else {
                        group_col_data
                            .iter()
                            .map(|(_, col_type, col_data)| {
                                extract_group_key_part(col_type, col_data, row_idx)
                            })
                            .collect()
                    };
                    let accums = groups
                        .entry(key)
                        .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::new()).collect());
                    for (agg_idx, (op, _)) in aggregates.iter().enumerate() {
                        match &agg_col_data[agg_idx] {
                            None => accums[agg_idx].feed_count_only(),
                            Some((_, col_data)) => {
                                if !accums[agg_idx].feed_op(op, col_data, row_idx) {
                                    return;
                                }
                            }
                        }
                    }
                };
                match &filter_result {
                    FilterResult::Bitmask(bm) => {
                        for_each_set_bit(bm, row_count, &mut process_row);
                    }
                    FilterResult::BoolMask(mask) => {
                        for (row_idx, &passes) in mask.iter().enumerate().take(row_count) {
                            if passes {
                                process_row(row_idx);
                            }
                        }
                    }
                    FilterResult::None => {
                        for row_idx in 0..row_count {
                            process_row(row_idx);
                        }
                    }
                }
                return Some(AccumulatedGroups {
                    groups,
                    group_col_info,
                });
            }
        };

        // Spill-aware accumulation loop.
        let mut spill_err: Option<crate::Error> = None;

        let mut process_row = |row_idx: usize| -> bool {
            let key: GroupKey = if group_by.is_empty() {
                Vec::new()
            } else {
                group_col_data
                    .iter()
                    .map(|(_, col_type, col_data)| {
                        extract_group_key_part(col_type, col_data, row_idx)
                    })
                    .collect()
            };

            let accums = match spiller.get_or_insert_with(key, num_aggs) {
                Ok(a) => a,
                Err(e) => {
                    spill_err = Some(e);
                    return false; // signal early exit
                }
            };

            for (agg_idx, (op, _)) in aggregates.iter().enumerate() {
                match &agg_col_data[agg_idx] {
                    None => accums[agg_idx].feed_count_only(),
                    Some((_, col_data)) => {
                        if !accums[agg_idx].feed_op(op, col_data, row_idx) {
                            return true;
                        }
                    }
                }
            }
            true // continue
        };

        match &filter_result {
            FilterResult::Bitmask(bm) => {
                for_each_set_bit(bm, row_count, &mut |row_idx| {
                    if !process_row(row_idx) {
                        // Can't break out of for_each_set_bit; spill_err is set.
                    }
                });
            }
            FilterResult::BoolMask(mask) => {
                for (row_idx, &passes) in mask.iter().enumerate().take(row_count) {
                    if passes && !process_row(row_idx) {
                        break;
                    }
                }
            }
            FilterResult::None => {
                for row_idx in 0..row_count {
                    if !process_row(row_idx) {
                        break;
                    }
                }
            }
        }

        if spill_err.is_some() {
            // Spill error: fall through to the generic path by returning None.
            return None;
        }

        match spiller.finalize() {
            Ok(groups) => groups,
            Err(_) => return None,
        }
    };

    Some(AccumulatedGroups {
        groups,
        group_col_info,
    })
}

/// Build `serde_json` result rows from a finalized group accumulator map
/// (resolving symbols only here).
///
/// SUM is exact per `ExactSum`; AVG divides the exact total. MIN / MAX
/// return the original cell: an integer column stays an integer.
fn build_results_from_groups(
    groups: &HashMap<GroupKey, Vec<AggAccum>>,
    group_by: &[String],
    group_col_info: &[(usize, ColumnType)],
    aggregates: &[(String, String)],
    mt: &ColumnarMemtable,
    limit: usize,
) -> Result<ColumnarAggResult, nodedb_query::EvalError> {
    let mut results: Vec<serde_json::Value> = Vec::with_capacity(groups.len().min(limit));

    for (group_key, accums) in groups {
        let mut row = serde_json::Map::new();

        for (i, field) in group_by.iter().enumerate() {
            let (col_idx, _) = group_col_info[i];
            let val = if i < group_key.len() {
                resolve_key_part(mt, col_idx, &group_key[i])
            } else {
                serde_json::Value::Null
            };
            row.insert(field.clone(), val);
        }

        for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
            let agg_key = canonical_agg_key(op, field);
            let accum = &accums[agg_idx];
            let val = match op.as_str() {
                "count" => serde_json::json!(accum.count),
                "sum" => serde_json::Value::from(accum.sum.sum()?),
                "avg" => serde_json::Value::from(accum.sum.avg()?),
                "min" => accum
                    .min
                    .clone()
                    .map_or(serde_json::Value::Null, Into::into),
                "max" => accum
                    .max
                    .clone()
                    .map_or(serde_json::Value::Null, Into::into),
                _ => serde_json::Value::Null,
            };
            row.insert(agg_key, val);
        }

        results.push(serde_json::Value::Object(row));
        if results.len() >= limit {
            break;
        }
    }

    Ok(ColumnarAggResult { rows: results })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use nodedb_types::timeseries::SeriesId;

    fn make_test_memtable() -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("qname".into(), ColumnType::Symbol),
                ("qtype".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let config = ColumnarMemtableConfig::default();
        let mut mt = ColumnarMemtable::new(schema, config);

        use crate::engine::timeseries::columnar_memtable::ColumnValue;

        // Insert test rows with varying qnames (high cardinality) and qtypes (low cardinality)
        let qnames = [
            "example.com",
            "google.com",
            "github.com",
            "reddit.com",
            "rust-lang.org",
        ];
        let qtypes = ["A", "AAAA"];

        for i in 0..100 {
            let series_id: SeriesId = i as u64;
            let values = [
                ColumnValue::Timestamp(i as i64 * 1000),
                ColumnValue::Float64(i as f64 * 100.0),
                ColumnValue::Symbol(qnames[i % qnames.len()].to_string()),
                ColumnValue::Symbol(qtypes[i % qtypes.len()].to_string()),
            ];
            mt.ingest_row(series_id, &values).unwrap();
        }

        mt
    }

    fn test_spill_dir(suffix: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nodedb_columnar_agg_test_{suffix}_{}",
            std::process::id()
        ));
        p
    }

    #[test]
    fn group_by_symbol_column() {
        let mt = make_test_memtable();
        let sd = test_spill_dir("sym");
        let result = try_columnar_aggregate(&ColumnarAggParams {
            mt: &mt,
            group_by: &["qtype".into()],
            aggregates: &[("count".into(), "*".into()), ("avg".into(), "value".into())],
            filters: &[],
            limit: 100,
            scan_limit: 100_000,
            spill_dir: &sd,
            spill_cap: 1_000_000,
            governor: crate::data::executor::core_loop::test_governor(),
            db: crate::types::DatabaseId::DEFAULT,
            tenant: crate::types::TenantId::new(1),
        })
        .unwrap()
        .unwrap();

        assert_eq!(result.rows.len(), 2); // A and AAAA
        for row in &result.rows {
            let count = row.get("count(*)").and_then(|v| v.as_u64()).unwrap();
            assert_eq!(count, 50); // 100 rows / 2 types
        }
    }

    #[test]
    fn group_by_high_cardinality() {
        let mt = make_test_memtable();
        let sd = test_spill_dir("hc");
        let result = try_columnar_aggregate(&ColumnarAggParams {
            mt: &mt,
            group_by: &["qname".into()],
            aggregates: &[("count".into(), "*".into()), ("sum".into(), "value".into())],
            filters: &[],
            limit: 100,
            scan_limit: 100_000,
            spill_dir: &sd,
            spill_cap: 1_000_000,
            governor: crate::data::executor::core_loop::test_governor(),
            db: crate::types::DatabaseId::DEFAULT,
            tenant: crate::types::TenantId::new(1),
        })
        .unwrap()
        .unwrap();

        assert_eq!(result.rows.len(), 5); // 5 unique qnames
    }

    #[test]
    fn filter_and_group() {
        let mt = make_test_memtable();
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "value".into(),
            op: crate::bridge::scan_filter::FilterOp::Gt,
            value: nodedb_types::Value::Float(5000.0),
            clauses: vec![],
            expr: None,
        };
        let sd = test_spill_dir("filt");
        let result = try_columnar_aggregate(&ColumnarAggParams {
            mt: &mt,
            group_by: &["qname".into()],
            aggregates: &[("count".into(), "*".into()), ("avg".into(), "value".into())],
            filters: &[filter],
            limit: 100,
            scan_limit: 100_000,
            spill_dir: &sd,
            spill_cap: 1_000_000,
            governor: crate::data::executor::core_loop::test_governor(),
            db: crate::types::DatabaseId::DEFAULT,
            tenant: crate::types::TenantId::new(1),
        })
        .unwrap()
        .unwrap();

        // Only rows with value > 5000 (i >= 51, value >= 5100)
        assert!(!result.rows.is_empty());
        for row in &result.rows {
            let avg = row.get("avg(value)").and_then(|v| v.as_f64()).unwrap();
            assert!(avg > 5000.0);
        }
    }

    #[test]
    fn no_group_by_aggregate_all() {
        let mt = make_test_memtable();
        let sd = test_spill_dir("nogrp");
        let result = try_columnar_aggregate(&ColumnarAggParams {
            mt: &mt,
            group_by: &[],
            aggregates: &[("count".into(), "*".into()), ("sum".into(), "value".into())],
            filters: &[],
            limit: 100,
            scan_limit: 100_000,
            spill_dir: &sd,
            spill_cap: 1_000_000,
            governor: crate::data::executor::core_loop::test_governor(),
            db: crate::types::DatabaseId::DEFAULT,
            tenant: crate::types::TenantId::new(1),
        })
        .unwrap()
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0]
            .get("count(*)")
            .and_then(|v| v.as_u64())
            .unwrap();
        assert_eq!(count, 100);
    }

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    /// A memtable with a nanosecond `Int64` column `n`, a `Float64` column
    /// `f`, and a symbol `g`, one row per `(ts, n, f, g)`.
    fn int_memtable(rows: &[(i64, i64, f64, &str)]) -> ColumnarMemtable {
        use crate::engine::timeseries::columnar_memtable::ColumnValue;
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("n".into(), ColumnType::Int64),
                ("f".into(), ColumnType::Float64),
                ("g".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        for (i, &(ts, n, f, g)) in rows.iter().enumerate() {
            let values = [
                ColumnValue::Timestamp(ts),
                ColumnValue::Int64(n),
                ColumnValue::Float64(f),
                ColumnValue::Symbol(g.to_string()),
            ];
            mt.ingest_row(i as SeriesId, &values).unwrap();
        }
        mt
    }

    /// Aggregate `aggregates` over `mt`, grouped by `group_by`, with a spill
    /// cap of `spill_cap` groups.
    fn run(
        mt: &ColumnarMemtable,
        group_by: &[String],
        aggregates: &[(String, String)],
        spill_cap: usize,
        suffix: &str,
    ) -> Result<Vec<serde_json::Value>, nodedb_query::EvalError> {
        let sd = test_spill_dir(suffix);
        let result = try_columnar_aggregate(&ColumnarAggParams {
            mt,
            group_by,
            aggregates,
            filters: &[],
            limit: 100,
            scan_limit: 100_000,
            spill_dir: &sd,
            spill_cap,
            governor: crate::data::executor::core_loop::test_governor(),
            db: crate::types::DatabaseId::DEFAULT,
            tenant: crate::types::TenantId::new(1),
        })?;
        Ok(result.unwrap().rows)
    }

    fn aggs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(op, f)| (op.to_string(), f.to_string()))
            .collect()
    }

    #[test]
    fn int64_min_max_sum_stay_exact_above_2_pow_53() {
        let mt = int_memtable(&[(1, ABOVE, 0.0, "a"), (2, AT, 0.0, "a")]);
        let a = aggs(&[("min", "n"), ("max", "n"), ("sum", "n"), ("avg", "n")]);
        // Dense-symbol path and the no-GROUP-BY hash path.
        for group_by in [vec!["g".to_string()], vec![]] {
            let rows = run(&mt, &group_by, &a, 1_000_000, "exact").unwrap();
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row["min(n)"], serde_json::json!(AT));
            assert_eq!(row["max(n)"], serde_json::json!(ABOVE));
            assert_eq!(row["sum(n)"], serde_json::json!(ABOVE + AT));
            assert_eq!(row["avg(n)"], serde_json::json!(AT as f64));
        }
    }

    #[test]
    fn nanosecond_timestamps_min_max_exact() {
        let t = [
            1_700_000_000_000_000_002_i64,
            1_700_000_000_000_000_001,
            1_700_000_000_000_000_003,
        ];
        let mt = int_memtable(&[
            (t[0], 0, 0.0, "a"),
            (t[1], 0, 0.0, "a"),
            (t[2], 0, 0.0, "a"),
        ]);
        let a = aggs(&[("min", "timestamp"), ("max", "timestamp")]);
        let rows = run(&mt, &[], &a, 1_000_000, "nanos").unwrap();
        assert_eq!(rows[0]["min(timestamp)"], serde_json::json!(t[1]));
        assert_eq!(rows[0]["max(timestamp)"], serde_json::json!(t[2]));
    }

    #[test]
    fn int64_sum_past_i64_is_decimal_exact() {
        let mt = int_memtable(&[(1, i64::MAX, 0.0, "a"), (2, i64::MAX, 0.0, "a")]);
        let rows = run(&mt, &[], &aggs(&[("sum", "n")]), 1_000_000, "dec").unwrap();
        let want = 2 * i128::from(i64::MAX);
        assert_eq!(rows[0]["sum(n)"], serde_json::json!(want as u64));
    }

    #[test]
    fn float_min_max_keep_float_and_nan_yields() {
        let mt = int_memtable(&[(1, 0, f64::NAN, "a"), (2, 0, 1.5, "a"), (3, 0, -2.5, "a")]);
        let a = aggs(&[("min", "f"), ("max", "f"), ("sum", "f")]);
        let rows = run(&mt, &[], &a, 1_000_000, "nan").unwrap();
        assert_eq!(rows[0]["min(f)"], serde_json::json!(-2.5));
        assert_eq!(rows[0]["max(f)"], serde_json::json!(1.5));
        // NaN has no JSON number form.
        assert_eq!(rows[0]["sum(f)"], serde_json::Value::Null);
    }

    /// A spill cap of one group forces every group through spill runs, so
    /// the partial merge must keep the integer totals exact.
    #[test]
    fn spill_merge_keeps_integers_exact() {
        let mt = int_memtable(&[
            (1, ABOVE, 0.0, "a"),
            (2, AT, 0.0, "b"),
            (3, AT, 0.0, "a"),
            (4, ABOVE, 0.0, "b"),
        ]);
        let a = aggs(&[("min", "n"), ("max", "n"), ("sum", "n")]);
        let rows = run(&mt, &["timestamp".to_string()], &a, 1, "spill").unwrap();
        assert_eq!(rows.len(), 4);
        let rows = run(&mt, &["n".to_string()], &a, 1, "spill_n").unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            let n = row["n"].as_i64().unwrap();
            assert_eq!(row["min(n)"], serde_json::json!(n));
            assert_eq!(row["max(n)"], serde_json::json!(n));
            assert_eq!(row["sum(n)"], serde_json::json!(2 * n));
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures for the continuous-aggregate unit tests. Every fixture
//! works in database 0, tenant 1.

use nodedb_types::Value;

use super::definition::{AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy};
use super::manager::ContinuousAggregateManager;
use super::partial::ColumnLayout;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnValue, ColumnarDrainResult, ColumnarMemtable, ColumnarMemtableConfig,
    ColumnarSchema, TimeKind,
};
use crate::engine::timeseries::time_bucket;

pub const ABOVE: i64 = 9_007_199_254_740_993;
pub const AT: i64 = 9_007_199_254_740_992;
pub const T0: i64 = 1_700_000_040_000;

/// `(timestamp_ms, v)` rows: integers one apart above 2^53, and
/// `i64::MAX` twice so a bucket total leaves `i64`.
pub fn exact_rows() -> Vec<(i64, i64)> {
    vec![
        (T0, ABOVE),
        (T0 + 1_000, AT),
        (T0 + 2_000, 1_700_000_000_000_000_002),
        (T0 + 61_000, 1_700_000_000_000_000_001),
        (T0 + 62_000, i64::MAX),
        (T0 + 63_000, i64::MAX),
        (T0 + 3_600_000, 1_700_000_000_000_000_003),
    ]
}

/// COUNT, SUM, MIN, MAX, AVG, FIRST, LAST over `v`.
pub fn exact_def(name: &str, source: &str, bucket: &str) -> ContinuousAggregateDef {
    let expr = |function, column: &str| AggregateExpr {
        function,
        source_column: column.into(),
        output_column: String::new(),
    };
    ContinuousAggregateDef {
        database_id: 0,
        name: name.into(),
        source: source.into(),
        bucket_interval: bucket.into(),
        bucket_interval_ms: time_bucket::parse_interval_ms(bucket).unwrap_or(60_000),
        group_by: Vec::new(),
        aggregates: vec![
            expr(AggFunction::Count, "*"),
            expr(AggFunction::Sum, "v"),
            expr(AggFunction::Min, "v"),
            expr(AggFunction::Max, "v"),
            expr(AggFunction::Avg, "v"),
            expr(AggFunction::First, "v"),
            expr(AggFunction::Last, "v"),
        ],
        refresh_policy: RefreshPolicy::OnFlush,
        retention_period_ms: 0,
        stale: false,
        incarnation: nodedb_types::Hlc::new(1, 0),
    }
}

/// One drain of `rows` over a `(timestamp, v BIGINT)` schema.
pub fn int_drain(rows: &[(i64, i64)]) -> ColumnarDrainResult {
    let schema = ColumnarSchema {
        columns: vec![
            ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
            ("v".into(), ColumnType::Int64),
        ],
        timestamp_idx: 0,
        codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
    };
    let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
    for &(ts, v) in rows {
        mt.ingest_row(1, &[ColumnValue::Timestamp(ts), ColumnValue::Int64(v)])
            .expect("ingest test row");
    }
    mt.drain()
}

/// The ad-hoc timeseries aggregate of `rows` per bucket of `bucket_ms`, in
/// `exact_def` order.
pub fn ad_hoc(rows: &[(i64, i64)], bucket_ms: i64) -> Vec<(i64, Vec<Value>)> {
    use crate::engine::timeseries::columnar_agg::AggAccum;
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|&(ts, _)| ts);
    let mut buckets: std::collections::BTreeMap<i64, AggAccum> = Default::default();
    for (ts, v) in sorted {
        buckets
            .entry(time_bucket::time_bucket(bucket_ms, ts))
            .or_default()
            .feed_int(v);
    }
    buckets
        .into_iter()
        .map(|(bucket, a)| {
            let cell = |v: Option<&Value>| v.cloned().unwrap_or(Value::Null);
            let values = vec![
                Value::Integer(a.count as i64),
                a.sum_value().expect("ad-hoc sum"),
                cell(a.min()),
                cell(a.max()),
                a.avg_f64()
                    .expect("ad-hoc avg")
                    .map_or(Value::Null, Value::Float),
                cell(a.first()),
                cell(a.last()),
            ];
            (bucket, values)
        })
        .collect()
}

/// Every materialized bucket of `name`, finalized.
pub fn materialized(mgr: &ContinuousAggregateManager, name: &str) -> Vec<(i64, Vec<Value>)> {
    let def = mgr.get_definition(0, 1, name).expect("registered");
    let layout = ColumnLayout::of(def);
    mgr.get_materialized(0, 1, name)
        .expect("materialized")
        .into_iter()
        .map(|p| {
            let values = def
                .aggregates
                .iter()
                .map(|e| p.finalize(e, &layout).expect("finalize"))
                .collect();
            (p.bucket_ts, values)
        })
        .collect()
}

/// A manager holding `defs` for tenant 1, every one loaded.
pub fn loaded_manager(defs: Vec<ContinuousAggregateDef>) -> ContinuousAggregateManager {
    let mut mgr = ContinuousAggregateManager::new();
    for def in defs {
        let key = (def.database_id, 1, def.name.clone());
        mgr.register(1, def);
        mgr.loaded.insert(key);
    }
    mgr
}

// SPDX-License-Identifier: BUSL-1.1

//! MessagePack encoding for grouped timeseries aggregate results.

use nodedb_query::agg_key::canonical_agg_key;

use crate::data::executor::core_loop::TsGroupKeyKind;
use crate::data::executor::handlers::columnar_read::rmpv_time_cell;
use crate::engine::timeseries::columnar_memtable::TimeKind;
use crate::engine::timeseries::grouped_scan::GroupCell;
use crate::util::rmpv_value::value_to_rmpv;

/// The wire types of a grouped result's key columns.
pub(in crate::data::executor) struct GroupedKeyTypes<'a> {
    /// One kind per GROUP BY column, in `group_by` order.
    pub group_key_kinds: &'a [TsGroupKeyKind],
    /// One kind per aggregate, in `aggregates` order: the kind of the column
    /// the aggregate reads. `MIN` / `MAX` / `FIRST` / `LAST` over an instant
    /// column render as a typed instant.
    pub aggregate_kinds: &'a [TsGroupKeyKind],
    /// The kind of the collection's time key, which the `bucket` column
    /// derives from.
    pub bucket_kind: TimeKind,
}

/// Render one `MIN` / `MAX` / `FIRST` / `LAST` cell with its column's type.
///
/// The accumulator holds an instant column's cell as the integer storage
/// holds. That integer renders as a typed instant, as the column renders
/// ungrouped. Every other cell keeps its own type.
fn extreme_value(
    value: Option<&nodedb_types::Value>,
    kind: TsGroupKeyKind,
) -> crate::Result<rmpv::Value> {
    match (value, kind) {
        (Some(nodedb_types::Value::Integer(n)), TsGroupKeyKind::Instant(k)) => {
            rmpv_time_cell(TimeKind::Instant(k), *n)
        }
        (Some(v), _) => Ok(value_to_rmpv(v)),
        (None, _) => Ok(rmpv::Value::Nil),
    }
}

/// How a grouped result is bucketed, filtered, ordered, and cut.
pub(in crate::data::executor) struct GroupedOutputShape<'a> {
    /// Maximum number of rows returned.
    pub limit: usize,
    /// Time bucket width in milliseconds. Zero means no `bucket` column.
    pub bucket_interval_ms: i64,
    /// ORDER BY keys applied after HAVING.
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    /// HAVING filters applied to the finalized group rows.
    pub having: &'a [crate::bridge::scan_filter::ScanFilter],
    /// `(encoded key, output name)` of each output column, in SELECT-list
    /// order, applied after sort and limit. Empty keeps the encoded keys.
    pub output_columns: &'a [(String, String)],
}

/// `row` as exactly the columns `output_columns` names, in that order.
///
/// Each output column takes the value the row holds under its encoded key,
/// or NULL when the row holds none. Two output columns can read one key:
/// `AVG(v) AS a, AVG(v) AS b` both read `avg(v)`. An empty `output_columns`
/// returns `row` unchanged.
pub(super) fn select_output_columns(
    row: rmpv::Value,
    output_columns: &[(String, String)],
) -> rmpv::Value {
    if output_columns.is_empty() {
        return row;
    }
    let rmpv::Value::Map(fields) = row else {
        return row;
    };
    let selected = output_columns
        .iter()
        .map(|(key, name)| {
            let value = fields
                .iter()
                .find(|(k, _)| k.as_str() == Some(key.as_str()))
                .map_or(rmpv::Value::Nil, |(_, v)| v.clone());
            (rmpv::Value::String(name.as_str().into()), value)
        })
        .collect();
    rmpv::Value::Map(selected)
}

/// Render one GROUP BY key cell with the type its column carries ungrouped.
///
/// A NULL cell renders NULL; an empty-string symbol renders `''`. An integer
/// cell of a declared instant column is its millisecond count and renders as
/// a typed instant, exactly as row emission renders it, so the two routes to
/// one stored instant render it identically.
fn group_key_value(cell: Option<&GroupCell>, kind: TsGroupKeyKind) -> crate::Result<rmpv::Value> {
    Ok(match cell {
        None | Some(GroupCell::Null) => rmpv::Value::Nil,
        Some(GroupCell::Text(text)) => rmpv::Value::String(text.as_str().into()),
        Some(GroupCell::Float(bits)) => rmpv::Value::F64(f64::from_bits(*bits)),
        Some(GroupCell::Int(n)) => match kind {
            TsGroupKeyKind::Instant(k) => rmpv_time_cell(TimeKind::Instant(k), *n)?,
            TsGroupKeyKind::Integer | TsGroupKeyKind::Float | TsGroupKeyKind::Text => {
                rmpv::Value::Integer((*n).into())
            }
        },
    })
}

/// Serialize GroupedAggResult directly to MessagePack bytes.
///
/// Avoids building `Vec<serde_json::Value>` (2M allocations for 2M groups).
/// Writes an array of maps directly to the MessagePack buffer.
///
/// Each group's typed key carries its time bucket when the query buckets,
/// then one cell per GROUP BY column. The `bucket` column is derived from
/// the collection's time key and renders with that column's own kind,
/// `key_types.bucket_kind`.
pub(in crate::data::executor) fn encode_grouped_results(
    result: &crate::engine::timeseries::grouped_scan::GroupedAggResult,
    group_by: &[String],
    aggregates: &[(String, String)],
    shape: GroupedOutputShape<'_>,
    key_types: GroupedKeyTypes<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let GroupedOutputShape {
        limit,
        bucket_interval_ms,
        sort_keys,
        having,
        output_columns,
    } = shape;
    let GroupedKeyTypes {
        group_key_kinds,
        aggregate_kinds,
        bucket_kind,
    } = key_types;
    let has_bucket = bucket_interval_ms > 0;
    // An ordered or HAVING-filtered query has to see every group before
    // cutting to `limit`: groups arrive in hash-map order, so the first
    // `limit` of them are an arbitrary subset. Other queries keep the cheap
    // early cut.
    let gather = if sort_keys.is_empty() && having.is_empty() {
        limit
    } else {
        usize::MAX
    };
    let num_groups = result.groups.len().min(gather);

    // Pre-compute aggregate key names once (not per group).
    let agg_keys: Vec<String> = aggregates
        .iter()
        .map(|(op, field)| canonical_agg_key(op, field))
        .collect();

    // Fields per row: group_by columns + aggregates + optional bucket.
    let fields_per_row = group_by.len() + aggregates.len() + if has_bucket { 1 } else { 0 };

    let mut rows: Vec<rmpv::Value> = Vec::with_capacity(num_groups);

    for (count, (key, accums)) in result.groups.iter().enumerate() {
        if count >= gather {
            break;
        }

        let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(fields_per_row);

        if has_bucket {
            let bucket_ts = key.bucket.ok_or(crate::Error::Internal {
                detail: "a bucketed timeseries group has no bucket in its key".into(),
            })?;
            fields.push((
                rmpv::Value::String("bucket".into()),
                rmpv_time_cell(bucket_kind, bucket_ts)?,
            ));
        }
        for (i, field) in group_by.iter().enumerate() {
            let kind = group_key_kinds
                .get(i)
                .copied()
                .unwrap_or(TsGroupKeyKind::Text);
            let val = group_key_value(key.cells.get(i), kind)?;
            fields.push((rmpv::Value::String(field.as_str().into()), val));
        }

        for (agg_idx, agg_key) in agg_keys.iter().enumerate() {
            let accum = &accums[agg_idx];
            let op = &aggregates[agg_idx].0;
            // SUM is exact; MIN / MAX / FIRST / LAST keep the column's own
            // type, so an integer column stays an integer and an instant
            // column stays an instant.
            let kind = aggregate_kinds
                .get(agg_idx)
                .copied()
                .unwrap_or(TsGroupKeyKind::Text);
            let val = match op.as_str() {
                "count" => rmpv::Value::Integer((accum.count as i64).into()),
                "sum" => value_to_rmpv(&accum.sum_value()?),
                "avg" => accum.avg_f64()?.map_or(rmpv::Value::Nil, rmpv::Value::F64),
                "min" => extreme_value(accum.min(), kind)?,
                "max" => extreme_value(accum.max(), kind)?,
                "first" => extreme_value(accum.first(), kind)?,
                "last" => extreme_value(accum.last(), kind)?,
                // The planner sends only `ts_stddev` here: the population
                // form, NULL for fewer than 2 values.
                "ts_stddev" if accum.count >= 2 => rmpv::Value::F64(accum.stddev_population()),
                "ts_stddev" => rmpv::Value::Nil,
                other => {
                    return Err(crate::Error::Internal {
                        detail: format!("timeseries grouped aggregate '{other}' has no encoding"),
                    });
                }
            };
            fields.push((rmpv::Value::String(agg_key.as_str().into()), val));
        }

        rows.push(rmpv::Value::Map(fields));
    }

    // HAVING filters the finalized group rows before sort and limit.
    if !having.is_empty() {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            let mut bytes = Vec::new();
            rmpv::encode::write_value(&mut bytes, &row).map_err(|e| crate::Error::Codec {
                detail: format!("HAVING row encode: {e}"),
            })?;
            if crate::bridge::scan_filter::ScanFilter::all_match_binary(having, &bytes, crs)? {
                kept.push(row);
            }
        }
        rows = kept;
    }

    super::sort::sort_rows(&mut rows, sort_keys, crs)?;
    rows.truncate(limit);
    // HAVING and the sort keys read the encoded keys. The output names are
    // applied last.
    let rows: Vec<rmpv::Value> = rows
        .into_iter()
        .map(|row| select_output_columns(row, output_columns))
        .collect();

    let array = rmpv::Value::Array(rows);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &array).map_err(|e| crate::Error::Codec {
        detail: format!("grouped timeseries result encode: {e}"),
    })?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::grouped_scan::{GroupKey, GroupedAggResult};
    use nodedb_types::InstantKind;

    const BUCKET_MS: i64 = 1_583_402_400_000;

    fn one_bucket() -> GroupedAggResult {
        let mut result = GroupedAggResult::new(1);
        result.groups.insert(
            GroupKey {
                bucket: Some(BUCKET_MS),
                cells: Vec::new(),
            },
            vec![Default::default()],
        );
        result
    }

    fn bucket_cell(bucket_kind: TimeKind) -> rmpv::Value {
        let bytes = encode_grouped_results(
            &one_bucket(),
            &[],
            &[("count".to_string(), "v".to_string())],
            GroupedOutputShape {
                limit: usize::MAX,
                bucket_interval_ms: 60_000,
                sort_keys: &[],
                having: &[],
                output_columns: &[],
            },
            GroupedKeyTypes {
                group_key_kinds: &[],
                aggregate_kinds: &[],
                bucket_kind,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("encode");
        let rmpv::Value::Array(rows) =
            crate::util::bounded_msgpack::read_value(&bytes).expect("decode")
        else {
            panic!("not an array");
        };
        let rmpv::Value::Map(fields) = &rows[0] else {
            panic!("not a map");
        };
        fields
            .iter()
            .find(|(k, _)| k.as_str() == Some("bucket"))
            .map(|(_, v)| v.clone())
            .expect("bucket column")
    }

    #[test]
    fn bucket_over_an_instant_time_key_is_a_typed_instant() {
        assert_eq!(
            bucket_cell(TimeKind::Instant(InstantKind::Naive)),
            rmpv::Value::Ext(
                InstantKind::Naive.ext_type(),
                (BUCKET_MS * 1000).to_be_bytes().to_vec()
            )
        );
    }

    #[test]
    fn bucket_over_a_millis_time_key_is_the_integer_stored() {
        assert_eq!(
            bucket_cell(TimeKind::Millis),
            rmpv::Value::Integer(BUCKET_MS.into())
        );
    }

    /// The output columns rename, reorder, repeat, and drop the encoded keys.
    #[test]
    fn output_columns_follow_the_select_list() {
        let row = rmpv::Value::Map(vec![
            (rmpv::Value::from("bucket"), rmpv::Value::from(1)),
            (rmpv::Value::from("host"), rmpv::Value::from("a")),
            (rmpv::Value::from("avg(v)"), rmpv::Value::F64(2.5)),
        ]);
        let output = [
            ("avg(v)".to_string(), "m".to_string()),
            ("bucket".to_string(), "b".to_string()),
            ("avg(v)".to_string(), "again".to_string()),
        ];
        assert_eq!(
            select_output_columns(row.clone(), &output),
            rmpv::Value::Map(vec![
                (rmpv::Value::from("m"), rmpv::Value::F64(2.5)),
                (rmpv::Value::from("b"), rmpv::Value::from(1)),
                (rmpv::Value::from("again"), rmpv::Value::F64(2.5)),
            ])
        );
        assert_eq!(select_output_columns(row.clone(), &[]), row);
    }

    /// `MIN` over an instant column renders the typed instant the column
    /// renders ungrouped. Over an integer column it stays the integer.
    #[test]
    fn extreme_over_an_instant_column_is_a_typed_instant() {
        let stored = nodedb_types::Value::Integer(BUCKET_MS);
        assert_eq!(
            extreme_value(Some(&stored), TsGroupKeyKind::Instant(InstantKind::Utc))
                .expect("instant"),
            rmpv::Value::Ext(
                InstantKind::Utc.ext_type(),
                (BUCKET_MS * 1000).to_be_bytes().to_vec()
            )
        );
        assert_eq!(
            extreme_value(Some(&stored), TsGroupKeyKind::Integer).expect("integer"),
            rmpv::Value::Integer(BUCKET_MS.into())
        );
        assert_eq!(
            extreme_value(None, TsGroupKeyKind::Instant(InstantKind::Utc)).expect("null"),
            rmpv::Value::Nil
        );
    }

    /// Encode one ungrouped row whose accumulators were fed `feed`, and
    /// return the aggregate cells by key.
    fn aggregate_cells(
        ops: &[&str],
        feed: impl Fn(&mut crate::engine::timeseries::columnar_agg::AggAccum),
    ) -> Vec<(String, rmpv::Value)> {
        let mut result = GroupedAggResult::new(ops.len());
        let accums = ops
            .iter()
            .map(|_| {
                let mut a = crate::engine::timeseries::columnar_agg::AggAccum::default();
                feed(&mut a);
                a
            })
            .collect();
        result.groups.insert(GroupKey::default(), accums);
        let aggregates: Vec<(String, String)> = ops
            .iter()
            .map(|op| (op.to_string(), "v".to_string()))
            .collect();
        let bytes = encode_grouped_results(
            &result,
            &[],
            &aggregates,
            GroupedOutputShape {
                limit: usize::MAX,
                bucket_interval_ms: 0,
                sort_keys: &[],
                having: &[],
                output_columns: &[],
            },
            GroupedKeyTypes {
                group_key_kinds: &[],
                aggregate_kinds: &[],
                bucket_kind: TimeKind::Millis,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("encode");
        let rmpv::Value::Array(rows) =
            crate::util::bounded_msgpack::read_value(&bytes).expect("decode")
        else {
            panic!("not an array");
        };
        let rmpv::Value::Map(fields) = &rows[0] else {
            panic!("not a map");
        };
        fields
            .iter()
            .map(|(k, v)| (k.as_str().unwrap_or_default().to_string(), v.clone()))
            .collect()
    }

    fn cell<'a>(cells: &'a [(String, rmpv::Value)], key: &str) -> &'a rmpv::Value {
        &cells
            .iter()
            .find(|(k, _)| k == key)
            .expect("aggregate cell")
            .1
    }

    #[test]
    fn integer_sum_min_max_first_last_stay_exact() {
        const ABOVE: i64 = 9_007_199_254_740_993;
        const AT: i64 = 9_007_199_254_740_992;
        let ops = ["sum", "min", "max", "first", "last", "avg"];
        let cells = aggregate_cells(&ops, |a| {
            a.feed_int(ABOVE);
            a.feed_int(AT);
        });
        assert_eq!(
            *cell(&cells, "sum(v)"),
            rmpv::Value::Integer((ABOVE + AT).into())
        );
        assert_eq!(*cell(&cells, "min(v)"), rmpv::Value::Integer(AT.into()));
        assert_eq!(*cell(&cells, "max(v)"), rmpv::Value::Integer(ABOVE.into()));
        assert_eq!(
            *cell(&cells, "first(v)"),
            rmpv::Value::Integer(ABOVE.into())
        );
        assert_eq!(*cell(&cells, "last(v)"), rmpv::Value::Integer(AT.into()));
        assert_eq!(*cell(&cells, "avg(v)"), rmpv::Value::F64(AT as f64));
    }

    #[test]
    fn nanosecond_timestamps_and_sum_past_i64() {
        let ops = ["sum", "min", "max"];
        let cells = aggregate_cells(&ops, |a| {
            a.feed_int(1_700_000_000_000_000_002);
            a.feed_int(1_700_000_000_000_000_001);
            a.feed_int(i64::MAX);
        });
        let want = 3_400_000_000_000_000_003_i128 + i128::from(i64::MAX);
        assert_eq!(
            *cell(&cells, "sum(v)"),
            rmpv::Value::String(want.to_string().into())
        );
        assert_eq!(
            *cell(&cells, "min(v)"),
            rmpv::Value::Integer(1_700_000_000_000_000_001_i64.into())
        );
        assert_eq!(
            *cell(&cells, "max(v)"),
            rmpv::Value::Integer(i64::MAX.into())
        );
    }

    #[test]
    fn mixed_int_float_and_nan_extremes() {
        let ops = ["sum", "min", "max"];
        let cells = aggregate_cells(&ops, |a| {
            a.feed(f64::NAN);
            a.feed_int(2);
            a.feed(0.5);
        });
        assert!(matches!(cell(&cells, "sum(v)"), rmpv::Value::F64(f) if f.is_nan()));
        // NaN sorts above every number, as in PostgreSQL, so it is the max.
        assert_eq!(*cell(&cells, "min(v)"), rmpv::Value::F64(0.5));
        assert!(matches!(cell(&cells, "max(v)"), rmpv::Value::F64(f) if f.is_nan()));

        let cells = aggregate_cells(&ops, |a| {
            a.feed_int(2);
            a.feed(0.5);
        });
        assert_eq!(*cell(&cells, "sum(v)"), rmpv::Value::F64(2.5));
    }

    #[test]
    fn partial_merge_keeps_integer_sum_exact() {
        use crate::engine::timeseries::columnar_agg::AggAccum;
        let mut a = AggAccum::default();
        a.feed_int(9_007_199_254_740_993);
        let mut b = AggAccum::default();
        b.feed_int(9_007_199_254_740_992);
        b.feed_int(1);
        a.merge(&b);
        assert_eq!(
            a.sum_value().unwrap(),
            nodedb_types::Value::Integer(18_014_398_509_481_986)
        );
        assert_eq!(a.min(), Some(&nodedb_types::Value::Integer(1)));
        assert_eq!(
            a.max(),
            Some(&nodedb_types::Value::Integer(9_007_199_254_740_993))
        );
        assert_eq!(a.last(), Some(&nodedb_types::Value::Integer(1)));
    }

    #[test]
    fn an_instant_group_key_is_a_typed_instant() {
        let cell = group_key_value(
            Some(&GroupCell::Int(BUCKET_MS)),
            TsGroupKeyKind::Instant(InstantKind::Utc),
        )
        .expect("group key");
        assert_eq!(
            cell,
            rmpv::Value::Ext(
                InstantKind::Utc.ext_type(),
                (BUCKET_MS * 1000).to_be_bytes().to_vec()
            )
        );
    }

    /// An empty-string symbol group renders `''`; a NULL symbol group
    /// renders NULL.
    #[test]
    fn empty_and_null_symbol_keys_differ() {
        assert_eq!(
            group_key_value(Some(&GroupCell::Text(String::new())), TsGroupKeyKind::Text)
                .expect("empty"),
            rmpv::Value::String("".into())
        );
        assert_eq!(
            group_key_value(Some(&GroupCell::Null), TsGroupKeyKind::Text).expect("null"),
            rmpv::Value::Nil
        );
    }

    /// End to end through the grouped scan: a row stored before the `host`
    /// column existed holds the NULL symbol, a later row holds `''`, and a
    /// third ingests an explicit NULL. NULL and `''` are two groups,
    /// rendered NULL and `''`.
    #[test]
    fn grouped_scan_keeps_null_and_empty_symbol_groups_apart() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
        };
        use crate::engine::timeseries::grouped_scan::aggregate_memtable;

        let mut mt = ColumnarMemtable::new(
            ColumnarSchema {
                columns: vec![
                    ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                    ("v".into(), ColumnType::Float64),
                ],
                timestamp_idx: 0,
                codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
            },
            ColumnarMemtableConfig::default(),
        );
        mt.ingest_row(
            0,
            &[ColumnValue::Timestamp(1_000), ColumnValue::Float64(1.0)],
        )
        .expect("row before the column");
        mt.add_column("host".into(), ColumnType::Symbol);
        mt.ingest_row(
            0,
            &[
                ColumnValue::Timestamp(2_000),
                ColumnValue::Float64(2.0),
                ColumnValue::Symbol(String::new()),
            ],
        )
        .expect("row with an empty host");
        mt.ingest_row(
            0,
            &[
                ColumnValue::Timestamp(3_000),
                ColumnValue::Float64(3.0),
                ColumnValue::Null,
            ],
        )
        .expect("row with a NULL host");

        let result = aggregate_memtable(
            &mt,
            &["host".to_string()],
            &[("count".to_string(), "*".to_string())],
            &[],
            (i64::MIN, i64::MAX),
            0,
        )
        .expect("aggregate")
        .expect("groups");
        assert_eq!(result.groups.len(), 2, "NULL and '' are separate groups");

        let bytes = encode_grouped_results(
            &result,
            &["host".to_string()],
            &[("count".to_string(), "*".to_string())],
            GroupedOutputShape {
                limit: usize::MAX,
                bucket_interval_ms: 0,
                sort_keys: &[],
                having: &[],
                output_columns: &[],
            },
            GroupedKeyTypes {
                group_key_kinds: &[TsGroupKeyKind::Text],
                aggregate_kinds: &[TsGroupKeyKind::Text],
                bucket_kind: TimeKind::Millis,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("encode");
        let rmpv::Value::Array(rows) =
            crate::util::bounded_msgpack::read_value(&bytes).expect("decode")
        else {
            panic!("not an array");
        };
        let mut hosts: Vec<rmpv::Value> = rows
            .iter()
            .map(|row| {
                let rmpv::Value::Map(fields) = row else {
                    panic!("not a map");
                };
                fields
                    .iter()
                    .find(|(k, _)| k.as_str() == Some("host"))
                    .map(|(_, v)| v.clone())
                    .expect("host column")
            })
            .collect();
        hosts.sort_by_key(|v| v.is_nil());
        assert_eq!(
            hosts,
            vec![rmpv::Value::String("".into()), rmpv::Value::Nil]
        );
    }
}

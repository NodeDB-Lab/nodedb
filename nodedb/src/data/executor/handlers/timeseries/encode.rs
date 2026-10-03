// SPDX-License-Identifier: BUSL-1.1

//! MessagePack encoding for grouped timeseries aggregate results.

use nodedb_query::agg_key::canonical_agg_key;

use crate::data::executor::core_loop::TsGroupKeyKind;
use crate::data::executor::handlers::columnar_read::rmpv_time_cell;
use crate::engine::timeseries::columnar_memtable::TimeKind;
use crate::util::rmpv_value::value_to_rmpv;

/// The wire types of a grouped result's key columns.
pub(in crate::data::executor) struct GroupedKeyTypes<'a> {
    /// One kind per GROUP BY column, in `group_by` order.
    pub group_key_kinds: &'a [TsGroupKeyKind],
    /// The kind of the collection's time key, which the `bucket` column
    /// derives from.
    pub bucket_kind: TimeKind,
}

/// Render one GROUP BY key part with the type its column carries ungrouped.
///
/// The grouped scan reduces every key to a string, so the column's own type
/// is put back here. An empty part is SQL NULL. A declared instant is stored
/// in milliseconds and rendered as a typed instant, exactly as row emission
/// renders it, so the two routes to one stored instant render it identically.
///
/// A part that does not parse as its column's type falls back to the text it
/// holds: the key is data the scan produced, and dropping the group would
/// lose a row.
fn group_key_value(part: Option<&&str>, kind: TsGroupKeyKind) -> crate::Result<rmpv::Value> {
    let Some(text) = part.filter(|s| !s.is_empty()) else {
        return Ok(rmpv::Value::Nil);
    };
    let value = match kind {
        TsGroupKeyKind::Instant(k) => match text.parse::<i64>() {
            Ok(millis) => rmpv_time_cell(TimeKind::Instant(k), millis)?,
            Err(_) => rmpv::Value::String((*text).into()),
        },
        TsGroupKeyKind::Integer => match text.parse::<i64>() {
            Ok(n) => rmpv::Value::Integer(n.into()),
            Err(_) => rmpv::Value::String((*text).into()),
        },
        TsGroupKeyKind::Float => match text.parse::<f64>() {
            Ok(f) => rmpv::Value::F64(f),
            Err(_) => rmpv::Value::String((*text).into()),
        },
        TsGroupKeyKind::Text => rmpv::Value::String((*text).into()),
    };
    Ok(value)
}

/// Serialize GroupedAggResult directly to MessagePack bytes.
///
/// Avoids building `Vec<serde_json::Value>` (2M allocations for 2M groups).
/// Writes an array of maps directly to the MessagePack buffer.
///
/// Key format in GroupedAggResult:
/// - GROUP BY only: "group1\0group2"
/// - time_bucket only: "bucket_ts"
/// - time_bucket + GROUP BY: "bucket_ts\0group1\0group2"
///
/// The `bucket` column is derived from the collection's time key and renders
/// with that column's own kind, `key_types.bucket_kind`.
pub(in crate::data::executor) fn encode_grouped_results(
    result: &crate::engine::timeseries::grouped_scan::GroupedAggResult,
    group_by: &[String],
    aggregates: &[(String, String)],
    limit: usize,
    bucket_interval_ms: i64,
    sort_keys: &[nodedb_physical::physical_plan::SortKeySpec],
    key_types: GroupedKeyTypes<'_>,
) -> crate::Result<Vec<u8>> {
    let GroupedKeyTypes {
        group_key_kinds,
        bucket_kind,
    } = key_types;
    let has_bucket = bucket_interval_ms > 0;
    // An ordered query has to see every group before cutting to `limit`:
    // groups arrive in hash-map order, so the first `limit` of them are an
    // arbitrary subset. Unordered queries keep the cheap early cut.
    let gather = if sort_keys.is_empty() {
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
        let parts: Vec<&str> = key.split('\0').collect();

        if has_bucket {
            let bucket_ts = parts
                .first()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            fields.push((
                rmpv::Value::String("bucket".into()),
                rmpv_time_cell(bucket_kind, bucket_ts)?,
            ));

            for (i, field) in group_by.iter().enumerate() {
                let kind = group_key_kinds
                    .get(i)
                    .copied()
                    .unwrap_or(TsGroupKeyKind::Text);
                let val = group_key_value(parts.get(i + 1), kind)?;
                fields.push((rmpv::Value::String(field.as_str().into()), val));
            }
        } else {
            for (i, field) in group_by.iter().enumerate() {
                let kind = group_key_kinds
                    .get(i)
                    .copied()
                    .unwrap_or(TsGroupKeyKind::Text);
                let val = group_key_value(parts.get(i), kind)?;
                fields.push((rmpv::Value::String(field.as_str().into()), val));
            }
        }

        for (agg_idx, agg_key) in agg_keys.iter().enumerate() {
            let accum = &accums[agg_idx];
            let op = &aggregates[agg_idx].0;
            // SUM is exact; MIN / MAX / FIRST / LAST keep the cell's own
            // type, so an integer column stays an integer.
            let cell = |v: Option<&nodedb_types::Value>| v.map_or(rmpv::Value::Nil, value_to_rmpv);
            let val = match op.as_str() {
                "count" => rmpv::Value::Integer((accum.count as i64).into()),
                "sum" => value_to_rmpv(&accum.sum_value()?),
                "avg" => accum.avg_f64()?.map_or(rmpv::Value::Nil, rmpv::Value::F64),
                "min" => cell(accum.min()),
                "max" => cell(accum.max()),
                "first" => cell(accum.first()),
                "last" => cell(accum.last()),
                "stddev" | "ts_stddev" if accum.count >= 2 => {
                    rmpv::Value::F64(accum.stddev_population())
                }
                _ => rmpv::Value::Nil,
            };
            fields.push((rmpv::Value::String(agg_key.as_str().into()), val));
        }

        rows.push(rmpv::Value::Map(fields));
    }

    super::sort::sort_rows(&mut rows, sort_keys)?;
    rows.truncate(limit);

    let array = rmpv::Value::Array(rows);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::grouped_scan::GroupedAggResult;
    use nodedb_types::InstantKind;

    const BUCKET_MS: i64 = 1_583_402_400_000;

    fn one_bucket() -> GroupedAggResult {
        let mut result = GroupedAggResult::new(1);
        result
            .groups
            .insert(BUCKET_MS.to_string(), vec![Default::default()]);
        result
    }

    fn bucket_cell(bucket_kind: TimeKind) -> rmpv::Value {
        let bytes = encode_grouped_results(
            &one_bucket(),
            &[],
            &[("count".to_string(), "v".to_string())],
            usize::MAX,
            60_000,
            &[],
            GroupedKeyTypes {
                group_key_kinds: &[],
                bucket_kind,
            },
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
        result.groups.insert(String::new(), accums);
        let aggregates: Vec<(String, String)> = ops
            .iter()
            .map(|op| (op.to_string(), "v".to_string()))
            .collect();
        let bytes = encode_grouped_results(
            &result,
            &[],
            &aggregates,
            usize::MAX,
            0,
            &[],
            GroupedKeyTypes {
                group_key_kinds: &[],
                bucket_kind: TimeKind::Millis,
            },
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
        &cells.iter().find(|(k, _)| k == key).expect("aggregate cell").1
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
        assert_eq!(*cell(&cells, "sum(v)"), rmpv::Value::Integer((ABOVE + AT).into()));
        assert_eq!(*cell(&cells, "min(v)"), rmpv::Value::Integer(AT.into()));
        assert_eq!(*cell(&cells, "max(v)"), rmpv::Value::Integer(ABOVE.into()));
        assert_eq!(*cell(&cells, "first(v)"), rmpv::Value::Integer(ABOVE.into()));
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
        assert_eq!(*cell(&cells, "max(v)"), rmpv::Value::Integer(i64::MAX.into()));
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
        assert_eq!(*cell(&cells, "min(v)"), rmpv::Value::F64(0.5));
        assert_eq!(*cell(&cells, "max(v)"), rmpv::Value::Integer(2.into()));

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
        assert_eq!(
            a.last(),
            Some(&nodedb_types::Value::Integer(1))
        );
    }

    #[test]
    fn an_instant_group_key_is_a_typed_instant() {
        let cell = group_key_value(
            Some(&BUCKET_MS.to_string().as_str()),
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
}

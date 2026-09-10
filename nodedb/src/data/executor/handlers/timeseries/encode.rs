// SPDX-License-Identifier: BUSL-1.1

//! MessagePack encoding for grouped timeseries aggregate results.

use nodedb_query::agg_key::canonical_agg_key;

use crate::data::executor::core_loop::TsGroupKeyKind;

/// Render one GROUP BY key part with the type its column carries ungrouped.
///
/// The grouped scan reduces every key to a string, so the column's own type
/// is put back here. An empty part is SQL NULL. A declared instant is stored
/// in milliseconds and read in microseconds, exactly as row emission reads
/// it, so the two routes to one stored instant render it identically.
///
/// A part that does not parse as its column's type falls back to the text it
/// holds: the key is data the scan produced, and dropping the group would
/// lose a row.
fn group_key_value(part: Option<&&str>, kind: TsGroupKeyKind) -> crate::Result<rmpv::Value> {
    let Some(text) = part.filter(|s| !s.is_empty()) else {
        return Ok(rmpv::Value::Nil);
    };
    let value = match kind {
        TsGroupKeyKind::Instant => match text.parse::<i64>() {
            Ok(millis) => {
                let micros = nodedb_types::NdbDateTime::from_millis(millis)
                    .map_err(|e| crate::Error::Internal {
                        detail: format!("grouped timeseries key at {millis} ms: {e}"),
                    })?
                    .micros;
                rmpv::Value::Integer(micros.into())
            }
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
pub(in crate::data::executor) fn encode_grouped_results(
    result: &crate::engine::timeseries::grouped_scan::GroupedAggResult,
    group_by: &[String],
    aggregates: &[(String, String)],
    limit: usize,
    bucket_interval_ms: i64,
    sort_keys: &[nodedb_physical::physical_plan::SortKeySpec],
    group_key_kinds: &[TsGroupKeyKind],
) -> crate::Result<Vec<u8>> {
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
                rmpv::Value::Integer(bucket_ts.into()),
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
            let val = match op.as_str() {
                "count" => rmpv::Value::Integer((accum.count as i64).into()),
                "sum" if accum.count > 0 => rmpv::Value::F64(accum.sum()),
                "avg" if accum.count > 0 => rmpv::Value::F64(accum.sum() / accum.count as f64),
                "min" if accum.count > 0 => rmpv::Value::F64(accum.min),
                "max" if accum.count > 0 => rmpv::Value::F64(accum.max),
                "first" if accum.count > 0 => rmpv::Value::F64(accum.first()),
                "last" if accum.count > 0 => rmpv::Value::F64(accum.last()),
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

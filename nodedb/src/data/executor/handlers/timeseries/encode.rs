// SPDX-License-Identifier: BUSL-1.1

//! MessagePack encoding for grouped timeseries aggregate results.

use nodedb_query::agg_key::canonical_agg_key;

use crate::data::executor::core_loop::TsGroupKeyKind;
use crate::data::executor::handlers::columnar_read::rmpv_time_cell;
use crate::engine::timeseries::columnar_memtable::TimeKind;

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

//! MessagePack encoding for grouped timeseries aggregate results.

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
) -> Vec<u8> {
    let has_bucket = bucket_interval_ms > 0;
    let num_groups = result.groups.len().min(limit);

    // Pre-compute aggregate key names once (not per group).
    let agg_keys: Vec<String> = aggregates
        .iter()
        .map(|(op, field)| format!("{op}_{field}").replace('*', "all"))
        .collect();

    // Fields per row: group_by columns + aggregates + optional bucket.
    let fields_per_row = group_by.len() + aggregates.len() + if has_bucket { 1 } else { 0 };

    let mut rows: Vec<rmpv::Value> = Vec::with_capacity(num_groups);

    for (count, (key, accums)) in result.groups.iter().enumerate() {
        if count >= limit {
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
                let val = parts
                    .get(i + 1)
                    .filter(|s| !s.is_empty())
                    .map(|s| rmpv::Value::String((*s).into()))
                    .unwrap_or(rmpv::Value::Nil);
                fields.push((rmpv::Value::String(field.as_str().into()), val));
            }
        } else {
            for (i, field) in group_by.iter().enumerate() {
                let val = parts
                    .get(i)
                    .filter(|s| !s.is_empty())
                    .map(|s| rmpv::Value::String((*s).into()))
                    .unwrap_or(rmpv::Value::Nil);
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

    let array = rmpv::Value::Array(rows);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
    buf
}

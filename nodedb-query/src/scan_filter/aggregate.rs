use crate::json_ops::compare_json_optional as compare_json_values;

/// Compute an aggregate function over a group of JSON documents.
///
/// Supported operations: count, sum, avg, min, max, count_distinct,
/// stddev, variance, array_agg, string_agg, percentile_cont.
pub fn compute_aggregate(op: &str, field: &str, docs: &[serde_json::Value]) -> serde_json::Value {
    match op {
        "count" => serde_json::json!(docs.len()),

        "sum" => {
            let total: f64 = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .sum();
            serde_json::json!(total)
        }

        "avg" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                serde_json::Value::Null
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                serde_json::json!(avg)
            }
        }

        "min" => {
            let min = docs
                .iter()
                .filter_map(|d| d.get(field))
                .min_by(|a, b| compare_json_values(Some(a), Some(b)));
            match min {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "max" => {
            let max = docs
                .iter()
                .filter_map(|d| d.get(field))
                .max_by(|a, b| compare_json_values(Some(a), Some(b)));
            match max {
                Some(v) => v.clone(),
                None => serde_json::Value::Null,
            }
        }

        "count_distinct" => {
            let mut seen = std::collections::HashSet::new();
            for d in docs {
                if let Some(v) = d.get(field) {
                    seen.insert(v.to_string());
                }
            }
            serde_json::json!(seen.len())
        }

        "stddev" | "stddev_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance.sqrt())
        }

        "stddev_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance.sqrt())
        }

        "variance" | "var_pop" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            serde_json::json!(variance)
        }

        "var_samp" => {
            let values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_f64()))
                .collect();
            if values.len() < 2 {
                return serde_json::Value::Null;
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            serde_json::json!(variance)
        }

        "array_agg" => {
            let values: Vec<serde_json::Value> = docs
                .iter()
                .filter_map(|d| d.get(field).cloned())
                .filter(|v| !v.is_null())
                .collect();
            serde_json::Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<String> = docs
                .iter()
                .filter_map(|d| d.get(field).and_then(|v| v.as_str()).map(String::from))
                .collect();
            serde_json::Value::String(values.join(","))
        }

        "percentile_cont" => {
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                let p: f64 = field[..idx].parse().unwrap_or(0.5);
                (p, &field[idx + 1..])
            } else {
                (0.5, field)
            };
            let mut values: Vec<f64> = docs
                .iter()
                .filter_map(|d| d.get(actual_field).and_then(|v| v.as_f64()))
                .collect();
            if values.is_empty() {
                return serde_json::Value::Null;
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            serde_json::json!(result)
        }

        // Collect distinct field values into a JSON array (deduplicated).
        "array_agg_distinct" => {
            let mut seen = Vec::new();
            for d in docs {
                if let Some(v) = d.get(field)
                    && !v.is_null()
                    && !seen.contains(v)
                {
                    seen.push(v.clone());
                }
            }
            serde_json::Value::Array(seen)
        }

        _ => serde_json::Value::Null,
    }
}

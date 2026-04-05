//! Window function specification for Data Plane evaluation.
//!
//! Serialized via serde (MessagePack) for SPSC bridge transport.
//! Evaluated in the DocumentScan handler after sort, before projection.

use crate::bridge::expr_eval::SqlExpr;

/// A window function specification.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct WindowFuncSpec {
    /// Output column name (e.g., "row_num", "running_sum").
    pub alias: String,
    /// Function name: row_number, rank, dense_rank, lag, lead, sum, count, avg, min, max.
    pub func_name: String,
    /// Function arguments (e.g., `salary` for SUM(salary)). Empty for ROW_NUMBER.
    pub args: Vec<SqlExpr>,
    /// PARTITION BY column names. Empty = single partition (entire result set).
    pub partition_by: Vec<String>,
    /// ORDER BY within each partition: [(field, ascending)].
    pub order_by: Vec<(String, bool)>,
    /// Window frame specification.
    pub frame: WindowFrame,
}

/// Window frame: defines which rows within the partition are visible to the function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct WindowFrame {
    /// Frame mode: "rows" or "range".
    pub mode: String,
    /// Start bound.
    pub start: FrameBound,
    /// End bound.
    pub end: FrameBound,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            mode: "range".into(),
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
        }
    }
}

/// Window frame boundary.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// Evaluate window functions over sorted, partitioned rows.
///
/// `rows` is the sorted result set. Each row is a `(doc_id, serde_json::Value)`.
/// Returns the same rows with window columns appended to each document.
pub fn evaluate_window_functions(
    rows: &mut [(String, serde_json::Value)],
    specs: &[WindowFuncSpec],
) {
    for spec in specs {
        // Build partitions: group row indices by partition key.
        let partitions = build_partitions(rows, &spec.partition_by);

        for partition_indices in &partitions {
            match spec.func_name.as_str() {
                "row_number" => apply_row_number(rows, partition_indices, &spec.alias),
                "rank" => apply_rank(rows, partition_indices, &spec.alias, &spec.order_by),
                "dense_rank" => {
                    apply_dense_rank(rows, partition_indices, &spec.alias, &spec.order_by)
                }
                "lag" => apply_lag(rows, partition_indices, spec),
                "lead" => apply_lead(rows, partition_indices, spec),
                "ntile" => apply_ntile(rows, partition_indices, spec),
                "sum" | "count" | "avg" | "min" | "max" | "first_value" | "last_value" => {
                    apply_aggregate_window(rows, partition_indices, spec)
                }
                _ => {} // Unknown function — skip silently
            }
        }
    }
}

/// Group row indices by partition key.
fn build_partitions(
    rows: &[(String, serde_json::Value)],
    partition_by: &[String],
) -> Vec<Vec<usize>> {
    if partition_by.is_empty() {
        // Single partition: all rows.
        return vec![(0..rows.len()).collect()];
    }

    let mut groups: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    let mut order = Vec::new(); // Preserve partition ordering

    for (i, (_id, doc)) in rows.iter().enumerate() {
        let key: String = partition_by
            .iter()
            .map(|col| {
                doc.get(col)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect::<Vec<_>>()
            .join("\x00");
        let entry = groups.entry(key.clone()).or_default();
        if entry.is_empty() {
            order.push(key);
        }
        entry.push(i);
    }

    order.iter().filter_map(|k| groups.remove(k)).collect()
}

fn set_window_col(row: &mut serde_json::Value, alias: &str, val: serde_json::Value) {
    if let serde_json::Value::Object(map) = row {
        map.insert(alias.to_string(), val);
    }
}

fn get_field(doc: &serde_json::Value, field: &str) -> serde_json::Value {
    doc.get(field).cloned().unwrap_or(serde_json::Value::Null)
}

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

// ── Ranking functions ──

fn apply_row_number(rows: &mut [(String, serde_json::Value)], indices: &[usize], alias: &str) {
    for (rank, &i) in indices.iter().enumerate() {
        set_window_col(&mut rows[i].1, alias, serde_json::json!(rank + 1));
    }
}

fn apply_rank(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(String, bool)],
) {
    if indices.is_empty() {
        return;
    }
    let mut current_rank = 1;
    set_window_col(&mut rows[indices[0]].1, alias, serde_json::json!(1));

    for pos in 1..indices.len() {
        let prev = &rows[indices[pos - 1]].1;
        let curr = &rows[indices[pos]].1;
        let same = order_by
            .iter()
            .all(|(col, _)| get_field(prev, col) == get_field(curr, col));
        if !same {
            current_rank = pos + 1;
        }
        set_window_col(
            &mut rows[indices[pos]].1,
            alias,
            serde_json::json!(current_rank),
        );
    }
}

fn apply_dense_rank(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    alias: &str,
    order_by: &[(String, bool)],
) {
    if indices.is_empty() {
        return;
    }
    let mut current_rank = 1;
    set_window_col(&mut rows[indices[0]].1, alias, serde_json::json!(1));

    for pos in 1..indices.len() {
        let prev = &rows[indices[pos - 1]].1;
        let curr = &rows[indices[pos]].1;
        let same = order_by
            .iter()
            .all(|(col, _)| get_field(prev, col) == get_field(curr, col));
        if !same {
            current_rank += 1;
        }
        set_window_col(
            &mut rows[indices[pos]].1,
            alias,
            serde_json::json!(current_rank),
        );
    }
}

fn apply_ntile(rows: &mut [(String, serde_json::Value)], indices: &[usize], spec: &WindowFuncSpec) {
    let n = spec
        .args
        .first()
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                as_f64(v).map(|x| x as usize)
            } else {
                None
            }
        })
        .unwrap_or(1)
        .max(1);
    let total = indices.len();
    for (pos, &i) in indices.iter().enumerate() {
        let bucket = (pos * n / total) + 1;
        set_window_col(&mut rows[i].1, &spec.alias, serde_json::json!(bucket));
    }
}

// ── Offset functions ──

fn apply_lag(rows: &mut [(String, serde_json::Value)], indices: &[usize], spec: &WindowFuncSpec) {
    let field = spec
        .args
        .first()
        .and_then(|e| {
            if let SqlExpr::Column(c) = e {
                Some(c.as_str())
            } else {
                None
            }
        })
        .unwrap_or("*");
    let offset = spec
        .args
        .get(1)
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                as_f64(v).map(|n| n as usize)
            } else {
                None
            }
        })
        .unwrap_or(1);
    let default = spec
        .args
        .get(2)
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                Some(v.clone())
            } else {
                None
            }
        })
        .unwrap_or(serde_json::Value::Null);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos >= offset {
            get_field(&rows[indices[pos - offset]].1, field)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
}

fn apply_lead(rows: &mut [(String, serde_json::Value)], indices: &[usize], spec: &WindowFuncSpec) {
    let field = spec
        .args
        .first()
        .and_then(|e| {
            if let SqlExpr::Column(c) = e {
                Some(c.as_str())
            } else {
                None
            }
        })
        .unwrap_or("*");
    let offset = spec
        .args
        .get(1)
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                as_f64(v).map(|n| n as usize)
            } else {
                None
            }
        })
        .unwrap_or(1);
    let default = spec
        .args
        .get(2)
        .and_then(|e| {
            if let SqlExpr::Literal(v) = e {
                Some(v.clone())
            } else {
                None
            }
        })
        .unwrap_or(serde_json::Value::Null);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + offset < indices.len() {
            get_field(&rows[indices[pos + offset]].1, field)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
}

// ── Aggregate window functions ──

fn apply_aggregate_window(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
) {
    let field = spec
        .args
        .first()
        .and_then(|e| {
            if let SqlExpr::Column(c) = e {
                Some(c.as_str())
            } else {
                None
            }
        })
        .unwrap_or("*");

    // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    // This gives cumulative/running aggregates.
    let use_running = spec.frame.mode == "range"
        && matches!(spec.frame.start, FrameBound::UnboundedPreceding)
        && matches!(spec.frame.end, FrameBound::CurrentRow);

    if use_running {
        // Running aggregate: accumulate across the partition.
        let mut running_sum = 0.0f64;
        let mut running_count = 0u64;
        let mut running_min: Option<f64> = None;
        let mut running_max: Option<f64> = None;

        for (pos, &i) in indices.iter().enumerate() {
            let val = get_field(&rows[i].1, field);
            if let Some(n) = as_f64(&val) {
                running_sum += n;
                running_count += 1;
                running_min = Some(running_min.map_or(n, |m: f64| m.min(n)));
                running_max = Some(running_max.map_or(n, |m: f64| m.max(n)));
            } else if spec.func_name == "count" {
                running_count += 1;
            }

            let result = match spec.func_name.as_str() {
                "sum" => serde_json::json!(running_sum),
                "count" => serde_json::json!(running_count),
                "avg" => {
                    if running_count > 0 {
                        serde_json::json!(running_sum / running_count as f64)
                    } else {
                        serde_json::Value::Null
                    }
                }
                "min" => running_min
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::Value::Null),
                "max" => running_max
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::Value::Null),
                "first_value" => get_field(&rows[indices[0]].1, field),
                "last_value" => get_field(&rows[indices[pos]].1, field),
                _ => serde_json::Value::Null,
            };
            set_window_col(&mut rows[i].1, &spec.alias, result);
        }
    } else {
        // Full partition aggregate (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING).
        let values: Vec<f64> = indices
            .iter()
            .filter_map(|&i| as_f64(&get_field(&rows[i].1, field)))
            .collect();

        let result = match spec.func_name.as_str() {
            "sum" => serde_json::json!(values.iter().sum::<f64>()),
            "count" => serde_json::json!(indices.len()),
            "avg" => {
                if values.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(values.iter().sum::<f64>() / values.len() as f64)
                }
            }
            "min" => values
                .iter()
                .copied()
                .reduce(f64::min)
                .map(|m| serde_json::json!(m))
                .unwrap_or(serde_json::Value::Null),
            "max" => values
                .iter()
                .copied()
                .reduce(f64::max)
                .map(|m| serde_json::json!(m))
                .unwrap_or(serde_json::Value::Null),
            "first_value" => get_field(&rows[indices[0]].1, field),
            "last_value" => get_field(&rows[*indices.last().unwrap()].1, field),
            _ => serde_json::Value::Null,
        };

        for &i in indices {
            set_window_col(&mut rows[i].1, &spec.alias, result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_rows() -> Vec<(String, serde_json::Value)> {
        vec![
            (
                "1".into(),
                json!({"dept": "eng", "salary": 100, "name": "Alice"}),
            ),
            (
                "2".into(),
                json!({"dept": "eng", "salary": 120, "name": "Bob"}),
            ),
            (
                "3".into(),
                json!({"dept": "eng", "salary": 90, "name": "Carol"}),
            ),
            (
                "4".into(),
                json!({"dept": "sales", "salary": 80, "name": "Dave"}),
            ),
            (
                "5".into(),
                json!({"dept": "sales", "salary": 110, "name": "Eve"}),
            ),
        ]
    }

    #[test]
    fn row_number_single_partition() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "rn".into(),
            func_name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        assert_eq!(rows[0].1["rn"], json!(1));
        assert_eq!(rows[4].1["rn"], json!(5));
    }

    #[test]
    fn row_number_partitioned() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "rn".into(),
            func_name: "row_number".into(),
            args: vec![],
            partition_by: vec!["dept".into()],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        // eng partition: 1, 2, 3
        assert_eq!(rows[0].1["rn"], json!(1));
        assert_eq!(rows[2].1["rn"], json!(3));
        // sales partition: 1, 2
        assert_eq!(rows[3].1["rn"], json!(1));
        assert_eq!(rows[4].1["rn"], json!(2));
    }

    #[test]
    fn running_sum() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "running_total".into(),
            func_name: "sum".into(),
            args: vec![SqlExpr::Column("salary".into())],
            partition_by: vec!["dept".into()],
            order_by: vec![("salary".into(), true)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        // eng: 100, 220, 310
        assert_eq!(rows[0].1["running_total"], json!(100.0));
        assert_eq!(rows[1].1["running_total"], json!(220.0));
        assert_eq!(rows[2].1["running_total"], json!(310.0));
        // sales: 80, 190
        assert_eq!(rows[3].1["running_total"], json!(80.0));
        assert_eq!(rows[4].1["running_total"], json!(190.0));
    }

    #[test]
    fn lag_function() {
        let mut rows = make_rows();
        let spec = WindowFuncSpec {
            alias: "prev_salary".into(),
            func_name: "lag".into(),
            args: vec![
                SqlExpr::Column("salary".into()),
                SqlExpr::Literal(json!(1)),
                SqlExpr::Literal(json!(0)),
            ],
            partition_by: vec![],
            order_by: vec![],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        assert_eq!(rows[0].1["prev_salary"], json!(0)); // default
        assert_eq!(rows[1].1["prev_salary"], json!(100));
        assert_eq!(rows[2].1["prev_salary"], json!(120));
    }

    #[test]
    fn rank_with_ties() {
        let mut rows = vec![
            ("1".into(), json!({"score": 100})),
            ("2".into(), json!({"score": 100})),
            ("3".into(), json!({"score": 90})),
            ("4".into(), json!({"score": 80})),
        ];
        let spec = WindowFuncSpec {
            alias: "r".into(),
            func_name: "rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![("score".into(), false)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        assert_eq!(rows[0].1["r"], json!(1)); // tied at 100
        assert_eq!(rows[1].1["r"], json!(1)); // tied at 100
        assert_eq!(rows[2].1["r"], json!(3)); // rank jumps to 3
        assert_eq!(rows[3].1["r"], json!(4));
    }

    #[test]
    fn dense_rank_no_gaps() {
        let mut rows = vec![
            ("1".into(), json!({"score": 100})),
            ("2".into(), json!({"score": 100})),
            ("3".into(), json!({"score": 90})),
        ];
        let spec = WindowFuncSpec {
            alias: "dr".into(),
            func_name: "dense_rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![("score".into(), false)],
            frame: WindowFrame::default(),
        };
        evaluate_window_functions(&mut rows, &[spec]);
        assert_eq!(rows[0].1["dr"], json!(1));
        assert_eq!(rows[1].1["dr"], json!(1));
        assert_eq!(rows[2].1["dr"], json!(2)); // no gap
    }
}

//! Time-range extraction from WHERE filters on timestamp columns.
//!
//! Walks the `Filter` / `SqlExpr` tree looking for comparisons or BETWEEN on
//! recognized time fields (`ts`, `created_at`, anything ending in `_at`/`_time`/`_ts`),
//! returning `(min_ts_ms, max_ts_ms)` for the timeseries engine's block pruning.

use nodedb_sql::types::{Filter, FilterExpr, SqlExpr, SqlValue};

/// Extract `(min_ts_ms, max_ts_ms)` time bounds from WHERE filters.
pub(crate) fn extract_time_range(filters: &[Filter]) -> (i64, i64) {
    let mut min_ts = i64::MIN;
    let mut max_ts = i64::MAX;

    for filter in filters {
        extract_time_bounds_from_filter(&filter.expr, &mut min_ts, &mut max_ts);
    }

    (min_ts, max_ts)
}

fn extract_time_bounds_from_filter(expr: &FilterExpr, min_ts: &mut i64, max_ts: &mut i64) {
    match expr {
        FilterExpr::Comparison { field, op, value } if is_time_field(field) => {
            if let Some(ms) = sql_value_to_timestamp_ms(value) {
                match op {
                    nodedb_sql::types::CompareOp::Ge | nodedb_sql::types::CompareOp::Gt => {
                        if ms > *min_ts {
                            *min_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Le | nodedb_sql::types::CompareOp::Lt => {
                        if ms < *max_ts {
                            *max_ts = ms;
                        }
                    }
                    nodedb_sql::types::CompareOp::Eq => {
                        *min_ts = ms;
                        *max_ts = ms;
                    }
                    _ => {}
                }
            }
        }
        FilterExpr::Between { field, low, high } if is_time_field(field) => {
            if let Some(lo) = sql_value_to_timestamp_ms(low) {
                *min_ts = lo;
            }
            if let Some(hi) = sql_value_to_timestamp_ms(high) {
                *max_ts = hi;
            }
        }
        FilterExpr::And(children) => {
            for child in children {
                extract_time_bounds_from_filter(&child.expr, min_ts, max_ts);
            }
        }
        FilterExpr::Expr(sql_expr) => {
            extract_time_bounds_from_expr(sql_expr, min_ts, max_ts);
        }
        _ => {}
    }
}

fn extract_time_bounds_from_expr(expr: &SqlExpr, min_ts: &mut i64, max_ts: &mut i64) {
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return;
    };
    match op {
        nodedb_sql::types::BinaryOp::And => {
            extract_time_bounds_from_expr(left, min_ts, max_ts);
            extract_time_bounds_from_expr(right, min_ts, max_ts);
        }
        nodedb_sql::types::BinaryOp::Ge | nodedb_sql::types::BinaryOp::Gt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms > *min_ts
            {
                *min_ts = ms;
            }
        }
        nodedb_sql::types::BinaryOp::Le | nodedb_sql::types::BinaryOp::Lt => {
            if let Some(field) = expr_column_name(left)
                && is_time_field(&field)
                && let Some(ms) = expr_to_timestamp_ms(right)
                && ms < *max_ts
            {
                *max_ts = ms;
            }
        }
        _ => {}
    }
}

fn is_time_field(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "ts"
        || lower == "timestamp"
        || lower == "time"
        || lower == "created_at"
        || lower.ends_with("_at")
        || lower.ends_with("_time")
        || lower.ends_with("_ts")
}

fn expr_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn expr_to_timestamp_ms(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Literal(val) => sql_value_to_timestamp_ms(val),
        _ => None,
    }
}

fn sql_value_to_timestamp_ms(val: &SqlValue) -> Option<i64> {
    match val {
        SqlValue::Int(ms) => Some(*ms),
        SqlValue::String(s) => parse_timestamp_to_ms(s),
        _ => None,
    }
}

fn parse_timestamp_to_ms(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis());
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
    }
    s.parse::<i64>().ok()
}

//! Shared argument-extraction helpers used by all function sub-modules.

/// Extract a string argument at `idx`, returning `None` for null/missing.
pub fn str_arg(args: &[serde_json::Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

/// Extract a numeric argument with bool coercion.
pub fn num_arg(args: &[serde_json::Value], idx: usize) -> Option<f64> {
    args.get(idx)
        .and_then(|v| crate::json_ops::json_to_f64(v, true))
}

/// Check if the first arg is a string matching a predicate.
pub fn bool_id_check(
    args: &[serde_json::Value],
    check: impl Fn(&str) -> bool,
) -> serde_json::Value {
    args.first()
        .and_then(|v| v.as_str())
        .map_or(serde_json::Value::Bool(false), |s| {
            serde_json::Value::Bool(check(s))
        })
}

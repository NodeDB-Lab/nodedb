//! Conditional scalar functions: coalesce, nullif, greatest, least.

use crate::json_ops::compare_json;

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return Some(arg.clone());
                }
            }
            serde_json::Value::Null
        }
        "nullif" => {
            if args.len() >= 2 && args[0] == args[1] {
                serde_json::Value::Null
            } else {
                args.first().cloned().unwrap_or(serde_json::Value::Null)
            }
        }
        "greatest" => args
            .iter()
            .filter(|v| !v.is_null())
            .max_by(|a, b| compare_json(a, b))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "least" => args
            .iter()
            .filter(|v| !v.is_null())
            .min_by(|a, b| compare_json(a, b))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        _ => return None,
    };
    Some(v)
}

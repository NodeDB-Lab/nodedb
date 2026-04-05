//! String scalar functions.

use super::shared::{num_arg, str_arg};
use crate::json_ops::json_to_display_string;

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "upper" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.to_uppercase())
        }),
        "lower" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.to_lowercase())
        }),
        "trim" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.trim().to_string())
        }),
        "ltrim" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.trim_start().to_string())
        }),
        "rtrim" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.trim_end().to_string())
        }),
        "length" | "char_length" | "character_length" => str_arg(args, 0)
            .map_or(serde_json::Value::Null, |s| {
                serde_json::Value::Number(serde_json::Number::from(s.len() as i64))
            }),
        "substr" | "substring" => {
            let Some(s) = str_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let start = num_arg(args, 1).unwrap_or(1.0) as usize;
            let len = num_arg(args, 2).map(|n| n as usize);
            let start_idx = start.saturating_sub(1); // SQL is 1-based.
            let result: String = match len {
                Some(l) => s.chars().skip(start_idx).take(l).collect(),
                None => s.chars().skip(start_idx).collect(),
            };
            serde_json::Value::String(result)
        }
        "concat" => {
            let parts: Vec<String> = args.iter().map(json_to_display_string).collect();
            serde_json::Value::String(parts.join(""))
        }
        "replace" => {
            let Some(s) = str_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let from = str_arg(args, 1).unwrap_or_default();
            let to = str_arg(args, 2).unwrap_or_default();
            serde_json::Value::String(s.replace(&from, &to))
        }
        "reverse" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.chars().rev().collect())
        }),
        _ => return None,
    };
    Some(v)
}

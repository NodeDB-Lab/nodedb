// SPDX-License-Identifier: Apache-2.0

//! String scalar functions.

use super::shared::{num_arg, str_arg};
use crate::scan_filter::like::{DEFAULT_LIKE_ESCAPE, sql_like_match_escaped};
use crate::value_ops::value_to_display_string;
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "upper" => str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.to_uppercase())),
        "lower" => str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.to_lowercase())),
        "trim" => str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.trim().to_string())),
        "ltrim" => {
            str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.trim_start().to_string()))
        }
        "rtrim" => {
            str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.trim_end().to_string()))
        }
        "length" | "char_length" | "character_length" => {
            str_arg(args, 0).map_or(Value::Null, |s| Value::Integer(s.len() as i64))
        }
        "substr" | "substring" => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let Some(s) = str_arg(args, 0) else {
                return Some(Value::Null);
            };
            let start = num_arg(args, 1).unwrap_or(1.0) as usize;
            let len = num_arg(args, 2).map(|n| n as usize);
            let start_idx = start.saturating_sub(1); // SQL is 1-based.
            let result: String = match len {
                Some(l) => s.chars().skip(start_idx).take(l).collect(),
                None => s.chars().skip(start_idx).collect(),
            };
            Value::String(result)
        }
        "concat" => {
            let parts: Vec<String> = args.iter().map(value_to_display_string).collect();
            Value::String(parts.join(""))
        }
        "replace" => {
            if args.iter().any(|a| matches!(a, Value::Null)) {
                return Some(Value::Null);
            }
            let Some(s) = str_arg(args, 0) else {
                return Some(Value::Null);
            };
            let from = str_arg(args, 1).unwrap_or_default();
            let to = str_arg(args, 2).unwrap_or_default();
            Value::String(s.replace(&from, &to))
        }
        "reverse" => {
            str_arg(args, 0).map_or(Value::Null, |s| Value::String(s.chars().rev().collect()))
        }
        "like" => like(args, false),
        "ilike" => like(args, true),
        _ => return None,
    };
    Some(v)
}

/// `like(input, pattern[, escape])` / `ilike(...)`: SQL `LIKE` / `ILIKE`.
///
/// - A NULL input or pattern gives NULL.
/// - A non-text scalar operand matches as its display text.
/// - `escape` defaults to `\`. An empty escape disables escaping. An escape
///   longer than one character is invalid and gives NULL.
///
/// `NOT LIKE` is the negation of this call, so NULL stays NULL.
fn like(args: &[Value], case_insensitive: bool) -> Value {
    let (Some(input), Some(pattern)) = (like_text(args.first()), like_text(args.get(1))) else {
        return Value::Null;
    };
    let escape = match args.get(2) {
        None => Some(DEFAULT_LIKE_ESCAPE),
        Some(v) => {
            let Some(text) = like_text(Some(v)) else {
                return Value::Null;
            };
            let mut chars = text.chars();
            match (chars.next(), chars.next()) {
                (None, _) => None,
                (Some(c), None) => Some(c),
                (Some(_), Some(_)) => return Value::Null,
            }
        }
    };
    Value::Bool(sql_like_match_escaped(
        &input,
        &pattern,
        case_insensitive,
        escape,
    ))
}

/// The text a LIKE operand matches as. `None` for NULL or a missing operand.
fn like_text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(value_to_display_string(other)),
    }
}

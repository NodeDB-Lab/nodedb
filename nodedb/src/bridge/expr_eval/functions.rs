//! Scalar function evaluation for SqlExpr.
//!
//! All functions return `serde_json::Value::Null` on invalid/missing
//! arguments (SQL NULL propagation semantics).

use crate::bridge::json_ops::{compare_json, json_to_display_string, json_to_f64, to_json_number};

/// Evaluate a scalar function call.
pub(super) fn eval_function(name: &str, args: &[serde_json::Value]) -> serde_json::Value {
    match name {
        // ── String functions ──
        // NULL input → NULL output (SQL NULL propagation).
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
                return serde_json::Value::Null;
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
                return serde_json::Value::Null;
            };
            let from = str_arg(args, 1).unwrap_or_default();
            let to = str_arg(args, 2).unwrap_or_default();
            serde_json::Value::String(s.replace(&from, &to))
        }
        "reverse" => str_arg(args, 0).map_or(serde_json::Value::Null, |s| {
            serde_json::Value::String(s.chars().rev().collect())
        }),

        // ── Math functions ──
        "abs" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.abs())),
        "round" => {
            let Some(n) = num_arg(args, 0) else {
                return serde_json::Value::Null;
            };
            let decimals = num_arg(args, 1).unwrap_or(0.0) as i32;
            let factor = 10.0_f64.powi(decimals);
            to_json_number((n * factor).round() / factor)
        }
        "ceil" | "ceiling" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ceil()))
        }
        "floor" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.floor())),
        "power" | "pow" => {
            let Some(base) = num_arg(args, 0) else {
                return serde_json::Value::Null;
            };
            let exp = num_arg(args, 1).unwrap_or(1.0);
            to_json_number(base.powf(exp))
        }
        "sqrt" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.sqrt())),
        // Modulo by zero returns NULL.
        "mod" => {
            let Some(a) = num_arg(args, 0) else {
                return serde_json::Value::Null;
            };
            let b = num_arg(args, 1).unwrap_or(1.0);
            if b == 0.0 {
                serde_json::Value::Null
            } else {
                to_json_number(a % b)
            }
        }
        "sign" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.signum())),
        "log" | "ln" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ln()))
        }
        "log10" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log10())),
        "log2" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log2())),
        "exp" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.exp())),

        // ── Conditional ──
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return arg.clone();
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

        // ── ID generation ──
        "uuid" | "uuid_v4" | "gen_random_uuid" => {
            serde_json::Value::String(nodedb_types::id_gen::uuid_v4())
        }
        "uuid_v7" => serde_json::Value::String(nodedb_types::id_gen::uuid_v7()),
        "ulid" => serde_json::Value::String(nodedb_types::id_gen::ulid()),
        "cuid2" => serde_json::Value::String(nodedb_types::id_gen::cuid2()),
        "nanoid" => {
            let len = num_arg(args, 0).map(|n| n as usize);
            match len {
                Some(l) => serde_json::Value::String(nodedb_types::id_gen::nanoid_with_length(l)),
                None => serde_json::Value::String(nodedb_types::id_gen::nanoid()),
            }
        }

        // ── ID type detection ──
        "is_uuid" => bool_id_check(args, nodedb_types::id_gen::is_uuid),
        "is_ulid" => bool_id_check(args, nodedb_types::id_gen::is_ulid),
        "is_cuid2" => bool_id_check(args, nodedb_types::id_gen::is_cuid2),
        "is_nanoid" => bool_id_check(args, nodedb_types::id_gen::is_nanoid),
        "id_type" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(serde_json::Value::String("unknown".into()), |s| {
                serde_json::Value::String(nodedb_types::id_gen::detect_id_type(s).to_string())
            }),
        "uuid_version" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(serde_json::Value::Number(0.into()), |s| {
                serde_json::Value::Number(nodedb_types::id_gen::uuid_version(s).into())
            }),
        "ulid_timestamp" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::id_gen::ulid_timestamp_ms)
            .map_or(serde_json::Value::Null, |ms| {
                serde_json::Value::Number(serde_json::Number::from(ms as i64))
            }),

        // ── DateTime ──
        // Returns NULL on parse failure (invalid format or out-of-range).
        "now" | "current_timestamp" => {
            let dt = nodedb_types::NdbDateTime::now();
            serde_json::Value::String(dt.to_iso8601())
        }
        "datetime" | "to_datetime" => args
            .first()
            .and_then(|v| match v {
                serde_json::Value::String(s) => nodedb_types::NdbDateTime::parse(s)
                    .map(|dt| serde_json::Value::String(dt.to_iso8601())),
                serde_json::Value::Number(n) => {
                    let micros = n.as_i64().unwrap_or(0);
                    Some(serde_json::Value::String(
                        nodedb_types::NdbDateTime::from_micros(micros).to_iso8601(),
                    ))
                }
                _ => None,
            })
            .unwrap_or(serde_json::Value::Null),
        "unix_secs" | "epoch_secs" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(serde_json::Value::Null, |dt| {
                serde_json::Value::Number(dt.unix_secs().into())
            }),
        "unix_millis" | "epoch_millis" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDateTime::parse)
            .map_or(serde_json::Value::Null, |dt| {
                serde_json::Value::Number(dt.unix_millis().into())
            }),
        // EXTRACT(part FROM datetime) — extract year/month/day/hour/minute/second.
        // Usage: extract('year', '2024-03-15T10:30:00Z') → 2024
        "extract" | "date_part" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let val: i64 = match part.to_lowercase().as_str() {
                        "year" | "y" => c.year as i64,
                        "month" | "mon" => c.month as i64,
                        "day" | "d" => c.day as i64,
                        "hour" | "h" => c.hour as i64,
                        "minute" | "min" | "m" => c.minute as i64,
                        "second" | "sec" | "s" => c.second as i64,
                        "microsecond" | "us" => c.microsecond as i64,
                        "epoch" => dt.unix_secs(),
                        "dow" | "dayofweek" => {
                            // 0=Sunday ... 6=Saturday (ISO: Monday=1)
                            let days = dt.micros / 86_400_000_000;
                            (days + 4) % 7 // epoch was Thursday=4
                        }
                        _ => return serde_json::Value::Null,
                    };
                    serde_json::Value::Number(val.into())
                }
                None => serde_json::Value::Null,
            }
        }
        // DATE_TRUNC(part, datetime) — truncate to boundary.
        // Usage: date_trunc('month', '2024-03-15T10:30:00Z') → '2024-03-01T00:00:00.000000Z'
        "date_trunc" | "datetrunc" => {
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let dt = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match dt {
                Some(dt) => {
                    let c = dt.components();
                    let truncated = match part.to_lowercase().as_str() {
                        "year" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-01-01T00:00:00Z",
                            c.year
                        )),
                        "month" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-01T00:00:00Z",
                            c.year, c.month
                        )),
                        "day" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T00:00:00Z",
                            c.year, c.month, c.day
                        )),
                        "hour" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:00:00Z",
                            c.year, c.month, c.day, c.hour
                        )),
                        "minute" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
                            c.year, c.month, c.day, c.hour, c.minute
                        )),
                        "second" => nodedb_types::NdbDateTime::parse(&format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                            c.year, c.month, c.day, c.hour, c.minute, c.second
                        )),
                        _ => None,
                    };
                    truncated.map_or(serde_json::Value::Null, |t| {
                        serde_json::Value::String(t.to_iso8601())
                    })
                }
                None => serde_json::Value::Null,
            }
        }
        // date_add(datetime, duration_string) → new datetime
        "date_add" | "datetime_add" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => {
                    serde_json::Value::String(dt.add_duration(dur).to_iso8601())
                }
                _ => serde_json::Value::Null,
            }
        }
        // date_sub(datetime, duration_string) → new datetime
        "date_sub" | "datetime_sub" => {
            let dt = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dur = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDuration::parse);
            match (dt, dur) {
                (Some(dt), Some(dur)) => {
                    serde_json::Value::String(dt.sub_duration(dur).to_iso8601())
                }
                _ => serde_json::Value::Null,
            }
        }
        // date_diff(datetime1, datetime2) → duration in seconds (f64)
        "date_diff" | "datediff" => {
            let dt1 = args
                .first()
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            let dt2 = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(nodedb_types::NdbDateTime::parse);
            match (dt1, dt2) {
                (Some(a), Some(b)) => to_json_number(a.duration_since(&b).as_secs_f64()),
                _ => serde_json::Value::Null,
            }
        }
        // duration('1h30m') → parse and normalize
        "duration" | "to_duration" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::NdbDuration::parse)
            .map_or(serde_json::Value::Null, |d| {
                serde_json::Value::String(d.to_human())
            }),

        // ── Geo ──
        "geo_distance" | "haversine_distance" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_json_number(nodedb_types::geometry::haversine_distance(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_bearing" | "haversine_bearing" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_json_number(nodedb_types::geometry::haversine_bearing(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_point" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let point = nodedb_types::geometry::Geometry::point(lng, lat);
            serde_json::to_value(&point).unwrap_or(serde_json::Value::Null)
        }
        // Returns NULL on parse failure (invalid decimal format).
        "decimal" | "to_decimal" => args.first().map_or(serde_json::Value::Null, |v| {
            let s = json_to_display_string(v);
            match s.parse::<rust_decimal::Decimal>() {
                Ok(d) => serde_json::Value::String(d.to_string()),
                Err(_) => serde_json::Value::Null,
            }
        }),

        // ── JSON manipulation ──
        // json_extract(doc, 'path.to.field') → nested field value
        "json_extract" | "json_get" => {
            let obj = args.first().unwrap_or(&serde_json::Value::Null);
            let path = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let mut current = obj;
            for key in path.split('.') {
                current = match current {
                    serde_json::Value::Object(map) => {
                        map.get(key).unwrap_or(&serde_json::Value::Null)
                    }
                    serde_json::Value::Array(arr) => key
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| arr.get(i))
                        .unwrap_or(&serde_json::Value::Null),
                    _ => &serde_json::Value::Null,
                };
            }
            current.clone()
        }
        // json_set(doc, 'key', value) → new object with key set
        "json_set" => {
            let mut obj = args
                .first()
                .cloned()
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
            let key = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let val = args.get(2).cloned().unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(ref mut map) = obj {
                map.insert(key.to_string(), val);
            }
            obj
        }
        // json_remove(doc, 'key') → new object without key
        "json_remove" => {
            let mut obj = args.first().cloned().unwrap_or(serde_json::Value::Null);
            let key = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if let serde_json::Value::Object(ref mut map) = obj {
                map.remove(key);
            }
            obj
        }
        // json_keys(doc) → array of keys
        "json_keys" => match args.first() {
            Some(serde_json::Value::Object(map)) => serde_json::Value::Array(
                map.keys()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .collect(),
            ),
            _ => serde_json::Value::Null,
        },
        // json_values(doc) → array of values
        "json_values" => match args.first() {
            Some(serde_json::Value::Object(map)) => {
                serde_json::Value::Array(map.values().cloned().collect())
            }
            _ => serde_json::Value::Null,
        },
        // json_length(doc_or_array) → number of keys or elements
        "json_length" | "json_len" => match args.first() {
            Some(serde_json::Value::Object(map)) => {
                serde_json::Value::Number(serde_json::Number::from(map.len() as i64))
            }
            Some(serde_json::Value::Array(arr)) => {
                serde_json::Value::Number(serde_json::Number::from(arr.len() as i64))
            }
            Some(serde_json::Value::String(s)) => {
                serde_json::Value::Number(serde_json::Number::from(s.len() as i64))
            }
            _ => serde_json::Value::Null,
        },
        // json_type(value) → "null", "boolean", "number", "string", "array", "object"
        "json_type" => {
            let type_name = match args.first() {
                Some(serde_json::Value::Null) | None => "null",
                Some(serde_json::Value::Bool(_)) => "boolean",
                Some(serde_json::Value::Number(_)) => "number",
                Some(serde_json::Value::String(_)) => "string",
                Some(serde_json::Value::Array(_)) => "array",
                Some(serde_json::Value::Object(_)) => "object",
            };
            serde_json::Value::String(type_name.into())
        }
        // json_array(a, b, c) → [a, b, c]
        "json_array" => serde_json::Value::Array(args.to_vec()),
        // json_object('k1', v1, 'k2', v2) → {"k1": v1, "k2": v2}
        "json_object" => {
            let mut map = serde_json::Map::new();
            let mut i = 0;
            while i + 1 < args.len() {
                let key = json_to_display_string(&args[i]);
                let val = args[i + 1].clone();
                map.insert(key, val);
                i += 2;
            }
            serde_json::Value::Object(map)
        }
        // json_contains(array_or_object, value) → bool
        "json_contains" => {
            let container = args.first().unwrap_or(&serde_json::Value::Null);
            let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
            let result = match container {
                serde_json::Value::Array(arr) => arr.contains(needle),
                serde_json::Value::Object(map) => {
                    if let Some(key) = needle.as_str() {
                        map.contains_key(key)
                    } else {
                        false
                    }
                }
                _ => false,
            };
            serde_json::Value::Bool(result)
        }
        // json_merge(obj1, obj2) → merged object (obj2 fields override obj1)
        "json_merge" | "json_patch" => {
            let mut base = match args.first() {
                Some(serde_json::Value::Object(m)) => m.clone(),
                _ => serde_json::Map::new(),
            };
            if let Some(serde_json::Value::Object(overlay)) = args.get(1) {
                for (k, v) in overlay {
                    base.insert(k.clone(), v.clone());
                }
            }
            serde_json::Value::Object(base)
        }

        // ── Type checking ──
        "typeof" | "type_of" => {
            let type_name = match args.first() {
                Some(serde_json::Value::Null) => "null",
                Some(serde_json::Value::Bool(_)) => "bool",
                Some(serde_json::Value::Number(n)) => {
                    if n.is_i64() {
                        "int"
                    } else {
                        "float"
                    }
                }
                Some(serde_json::Value::String(_)) => "string",
                Some(serde_json::Value::Array(_)) => "array",
                Some(serde_json::Value::Object(_)) => "object",
                None => "null",
            };
            serde_json::Value::String(type_name.to_string())
        }

        _ => serde_json::Value::Null,
    }
}

// ── Argument helpers ──

/// Extract a string argument, returning None for null/missing.
fn str_arg(args: &[serde_json::Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

/// Extract a numeric argument with bool coercion.
fn num_arg(args: &[serde_json::Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| json_to_f64(v, true))
}

/// Check if the first arg is a string matching a predicate.
fn bool_id_check(args: &[serde_json::Value], check: impl Fn(&str) -> bool) -> serde_json::Value {
    args.first()
        .and_then(|v| v.as_str())
        .map_or(serde_json::Value::Bool(false), |s| {
            serde_json::Value::Bool(check(s))
        })
}

#[cfg(test)]
mod tests {
    use super::super::eval::SqlExpr;
    use serde_json::json;

    fn eval_fn(name: &str, args: Vec<serde_json::Value>) -> serde_json::Value {
        super::eval_function(name, &args)
    }

    #[test]
    fn upper() {
        assert_eq!(eval_fn("upper", vec![json!("hello")]), json!("HELLO"));
    }

    #[test]
    fn upper_null_propagation() {
        assert_eq!(eval_fn("upper", vec![json!(null)]), json!(null));
    }

    #[test]
    fn substr_null_propagation() {
        assert_eq!(eval_fn("substr", vec![json!(null), json!(1)]), json!(null));
    }

    #[test]
    fn replace_null_propagation() {
        assert_eq!(
            eval_fn("replace", vec![json!(null), json!("a"), json!("b")]),
            json!(null)
        );
    }

    #[test]
    fn substring() {
        assert_eq!(
            eval_fn("substr", vec![json!("hello"), json!(2), json!(3)]),
            json!("ell")
        );
    }

    #[test]
    fn round_with_decimals() {
        assert_eq!(
            eval_fn("round", vec![json!(3.15159), json!(2)]),
            json!(3.15)
        );
    }

    #[test]
    fn typeof_int() {
        assert_eq!(eval_fn("typeof", vec![json!(42)]), json!("int"));
    }

    #[test]
    fn typeof_float() {
        assert_eq!(eval_fn("typeof", vec![json!(3.15)]), json!("float"));
    }

    #[test]
    fn typeof_null() {
        assert_eq!(eval_fn("typeof", vec![json!(null)]), json!("null"));
    }

    #[test]
    fn function_via_expr() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        let doc = json!({"name": "alice"});
        assert_eq!(expr.eval(&doc), json!("ALICE"));
    }
}

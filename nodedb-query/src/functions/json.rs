//! JSON manipulation scalar functions.

use crate::json_ops::json_to_display_string;

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
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
        "json_remove" => {
            let mut obj = args.first().cloned().unwrap_or(serde_json::Value::Null);
            let key = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if let serde_json::Value::Object(ref mut map) = obj {
                map.remove(key);
            }
            obj
        }
        "json_keys" => match args.first() {
            Some(serde_json::Value::Object(map)) => serde_json::Value::Array(
                map.keys()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .collect(),
            ),
            _ => serde_json::Value::Null,
        },
        "json_values" => match args.first() {
            Some(serde_json::Value::Object(map)) => {
                serde_json::Value::Array(map.values().cloned().collect())
            }
            _ => serde_json::Value::Null,
        },
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
        "json_array" => serde_json::Value::Array(args.to_vec()),
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
        _ => return None,
    };
    Some(v)
}

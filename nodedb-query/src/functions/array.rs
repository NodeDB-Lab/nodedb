//! Array scalar functions.

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "array_length" | "cardinality" => match args.first() {
            Some(serde_json::Value::Array(arr)) => serde_json::json!(arr.len() as i64),
            _ => serde_json::Value::Null,
        },
        "array_append" => {
            let mut arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a.clone(),
                _ => return Some(serde_json::Value::Null),
            };
            if let Some(val) = args.get(1) {
                arr.push(val.clone());
            }
            serde_json::Value::Array(arr)
        }
        "array_prepend" => {
            let val = args.first().cloned().unwrap_or(serde_json::Value::Null);
            let mut arr = match args.get(1) {
                Some(serde_json::Value::Array(a)) => a.clone(),
                _ => return Some(serde_json::Value::Null),
            };
            arr.insert(0, val);
            serde_json::Value::Array(arr)
        }
        "array_remove" => {
            let arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a,
                _ => return Some(serde_json::Value::Null),
            };
            let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
            serde_json::Value::Array(arr.iter().filter(|v| *v != needle).cloned().collect())
        }
        "array_concat" | "array_cat" => {
            let mut result = match args.first() {
                Some(serde_json::Value::Array(a)) => a.clone(),
                _ => return Some(serde_json::Value::Null),
            };
            if let Some(serde_json::Value::Array(b)) = args.get(1) {
                result.extend(b.iter().cloned());
            }
            serde_json::Value::Array(result)
        }
        "array_distinct" => {
            let arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a,
                _ => return Some(serde_json::Value::Null),
            };
            let mut unique = Vec::new();
            for v in arr {
                if !unique.contains(v) {
                    unique.push(v.clone());
                }
            }
            serde_json::Value::Array(unique)
        }
        "array_contains" => {
            let arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a,
                _ => return Some(serde_json::Value::Bool(false)),
            };
            let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
            serde_json::Value::Bool(arr.contains(needle))
        }
        "array_position" => {
            let arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a,
                _ => return Some(serde_json::Value::Null),
            };
            let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
            match arr.iter().position(|v| v == needle) {
                Some(pos) => serde_json::json!((pos + 1) as i64),
                None => serde_json::Value::Null,
            }
        }
        "array_reverse" => {
            let arr = match args.first() {
                Some(serde_json::Value::Array(a)) => a,
                _ => return Some(serde_json::Value::Null),
            };
            let mut reversed = arr.clone();
            reversed.reverse();
            serde_json::Value::Array(reversed)
        }
        _ => return None,
    };
    Some(v)
}

//! JSON / MessagePack conversion and secondary-index value extraction.

/// Strip the leading `$.` or `$` prefix from a JSON path expression.
pub(crate) fn normalize_path(path: &str) -> &str {
    path.strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path)
}

/// Extract scalar values at a JSON path for secondary indexing.
pub fn extract_index_values(doc: &serde_json::Value, path: &str, is_array: bool) -> Vec<String> {
    let path = normalize_path(path);
    let target = navigate_json(doc, path);

    match target {
        Some(serde_json::Value::Array(arr)) if is_array => {
            arr.iter().filter_map(json_scalar_to_string).collect()
        }
        Some(val) => json_scalar_to_string(val).into_iter().collect(),
        None => Vec::new(),
    }
}

/// Extract values from an rmpv::Value at a path.
pub(crate) fn extract_index_values_rmpv(
    value: &rmpv::Value,
    path: &str,
    is_array: bool,
) -> Vec<String> {
    let path = normalize_path(path);
    let target = navigate_rmpv(value, path);

    match target {
        Some(rmpv::Value::Array(arr)) if is_array => {
            arr.iter().filter_map(rmpv_scalar_to_string).collect()
        }
        Some(val) => rmpv_scalar_to_string(val).into_iter().collect(),
        None => Vec::new(),
    }
}

fn navigate_json<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn navigate_rmpv<'a>(value: &'a rmpv::Value, path: &str) -> Option<&'a rmpv::Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        let map = current.as_map()?;
        let mut found = false;
        for (k, v) in map {
            if k.as_str() == Some(segment) {
                current = v;
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    Some(current)
}

fn json_scalar_to_string(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn rmpv_scalar_to_string(val: &rmpv::Value) -> Option<String> {
    match val {
        rmpv::Value::String(s) => Some(s.as_str()?.to_string()),
        rmpv::Value::Integer(i) => Some(i.to_string()),
        rmpv::Value::Boolean(b) => Some(b.to_string()),
        rmpv::Value::F32(f) => Some(f.to_string()),
        rmpv::Value::F64(f) => Some(f.to_string()),
        _ => None,
    }
}

/// Convert serde_json::Value to rmpv::Value for MessagePack serialization.
pub fn json_to_msgpack(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(rmpv::Integer::from(i))
            } else if let Some(u) = n.as_u64() {
                rmpv::Value::Integer(rmpv::Integer::from(u))
            } else {
                rmpv::Value::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(rmpv::Utf8String::from(s.as_str())),
        serde_json::Value::Array(arr) => {
            rmpv::Value::Array(arr.iter().map(json_to_msgpack).collect())
        }
        serde_json::Value::Object(obj) => rmpv::Value::Map(
            obj.iter()
                .map(|(k, v)| {
                    (
                        rmpv::Value::String(rmpv::Utf8String::from(k.as_str())),
                        json_to_msgpack(v),
                    )
                })
                .collect(),
        ),
    }
}

/// Convert rmpv::Value back to serde_json::Value.
pub(crate) fn rmpv_to_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::json!(n)
            } else if let Some(n) = i.as_u64() {
                serde_json::json!(n)
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F32(f) => serde_json::json!(*f),
        rmpv::Value::F64(f) => serde_json::json!(*f),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => serde_json::Value::String(base64_encode(b)),
        rmpv::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(rmpv_to_json).collect()),
        rmpv::Value::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in entries {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    other => format!("{other}"),
                };
                obj.insert(key, rmpv_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msgpack_is_compact() {
        let doc = serde_json::json!({
            "name": "Alice Wonderland",
            "age": 30,
            "tags": ["admin", "user"],
            "active": true
        });

        let json_bytes = serde_json::to_vec(&doc).unwrap();
        let rmpv_val = json_to_msgpack(&doc);
        let mut msgpack_bytes = Vec::new();
        rmpv::encode::write_value(&mut msgpack_bytes, &rmpv_val).unwrap();

        assert!(
            msgpack_bytes.len() < json_bytes.len(),
            "msgpack {} bytes vs json {} bytes",
            msgpack_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn json_msgpack_roundtrip_preserves_types() {
        let doc = serde_json::json!({
            "string": "hello",
            "int": 42,
            "float": 1.234,
            "bool": true,
            "null": null,
            "array": [1, "two", false],
            "nested": {"a": {"b": 3}}
        });

        let rmpv_val = json_to_msgpack(&doc);
        let recovered = rmpv_to_json(&rmpv_val);

        assert_eq!(recovered["string"], "hello");
        assert_eq!(recovered["int"], 42);
        assert_eq!(recovered["bool"], true);
        assert!(recovered["null"].is_null());
        assert_eq!(recovered["array"][0], 1);
        assert_eq!(recovered["array"][1], "two");
        assert_eq!(recovered["nested"]["a"]["b"], 3);
    }
}

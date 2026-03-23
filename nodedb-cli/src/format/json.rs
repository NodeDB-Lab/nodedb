//! JSON (NDJSON) output format.

use nodedb_types::result::QueryResult;

/// Format a QueryResult as NDJSON (one JSON object per line).
pub fn format(qr: &QueryResult) -> String {
    if qr.columns.is_empty() && qr.rows.is_empty() {
        if qr.rows_affected > 0 {
            return format!("{{\"rows_affected\":{}}}\n", qr.rows_affected);
        }
        return String::new();
    }

    let mut out = String::new();
    for row in &qr.rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in qr.columns.iter().enumerate() {
            let val = row.get(i).cloned().unwrap_or(nodedb_types::Value::Null);
            obj.insert(col.clone(), value_to_json(&val));
        }
        if let Ok(line) = serde_json::to_string(&obj) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

fn value_to_json(v: &nodedb_types::Value) -> serde_json::Value {
    match v {
        nodedb_types::Value::Null => serde_json::Value::Null,
        nodedb_types::Value::Bool(b) => serde_json::Value::Bool(*b),
        nodedb_types::Value::Integer(i) => serde_json::json!(*i),
        nodedb_types::Value::Float(f) => serde_json::json!(*f),
        nodedb_types::Value::String(s) => serde_json::Value::String(s.clone()),
        nodedb_types::Value::Bytes(b) => serde_json::Value::String(format!(
            "\\x{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        )),
        nodedb_types::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        nodedb_types::Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_output() {
        let qr = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![vec![
                nodedb_types::Value::Integer(1),
                nodedb_types::Value::String("Alice".into()),
            ]],
            rows_affected: 0,
        };
        let out = format(&qr);
        assert!(out.contains("\"id\":1"));
        assert!(out.contains("\"name\":\"Alice\""));
    }
}

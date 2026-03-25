//! COPY FROM: NDJSON and CSV file import into collections.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Import documents from NDJSON (newline-delimited JSON) text.
    ///
    /// Each line is a JSON object. The "id" field is used as document ID;
    /// if missing, a UUID v7 is generated.
    ///
    /// Returns the number of documents imported.
    pub async fn copy_from_ndjson(&self, collection: &str, ndjson: &str) -> NodeDbResult<u64> {
        let mut docs: Vec<(String, Vec<(String, loro::LoroValue)>)> = Vec::new();
        for line in ndjson.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let obj: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| NodeDbError::bad_request(format!("invalid JSON: {e}")))?;

            let id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(nodedb_types::id_gen::uuid_v7);

            let mut fields = Vec::new();
            if let serde_json::Value::Object(map) = &obj {
                for (k, v) in map {
                    if k == "id" {
                        continue;
                    }
                    fields.push((k.clone(), json_to_loro(v)));
                }
            }
            docs.push((id, fields));
        }

        let mut crdt = self.crdt.lock_or_recover();
        let count = docs.len() as u64;

        use crate::engine::crdt::engine::CrdtField;
        let borrowed_fields: Vec<Vec<CrdtField<'_>>> = docs
            .iter()
            .map(|(_, fields)| {
                fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect()
            })
            .collect();

        let ops: Vec<(&str, &str, &[CrdtField<'_>])> = docs
            .iter()
            .zip(borrowed_fields.iter())
            .map(|((id, _), fields)| (collection, id.as_str(), fields.as_slice()))
            .collect();

        crdt.batch_upsert(&ops).map_err(NodeDbError::storage)?;
        Ok(count)
    }

    /// Import documents from CSV text.
    ///
    /// First line is the header (column names). Each subsequent line is a row.
    /// An "id" column is used as document ID; if missing, UUIDs are generated.
    /// Import documents from CSV text.
    ///
    /// Two-pass: first pass infers column types (int/float/bool/string) from
    /// ALL rows. Second pass coerces every value to the column's dominant type.
    /// This ensures type consistency within each column.
    pub async fn copy_from_csv(&self, collection: &str, csv_text: &str) -> NodeDbResult<u64> {
        let mut lines_iter = csv_text.lines();
        let header = lines_iter
            .next()
            .ok_or_else(|| NodeDbError::bad_request("CSV has no header"))?;
        let columns: Vec<&str> = header.split(',').map(|s| s.trim()).collect();
        let id_col = columns.iter().position(|c| *c == "id");

        // Collect all data rows.
        let rows: Vec<Vec<String>> = lines_iter
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.split(',').map(|s| s.trim().to_string()).collect())
            .collect();

        // Pass 1: infer column types. A column is "int" only if ALL values parse
        // as int. "float" if all parse as float. "bool" if all are true/false.
        // Otherwise "string".
        let col_types: Vec<&str> = (0..columns.len())
            .map(|ci| {
                let all_int = rows
                    .iter()
                    .all(|r| r.get(ci).map(|v| v.parse::<i64>().is_ok()).unwrap_or(true));
                if all_int {
                    return "int";
                }
                let all_float = rows
                    .iter()
                    .all(|r| r.get(ci).map(|v| v.parse::<f64>().is_ok()).unwrap_or(true));
                if all_float {
                    return "float";
                }
                let all_bool = rows.iter().all(|r| {
                    r.get(ci)
                        .map(|v| v == "true" || v == "false")
                        .unwrap_or(true)
                });
                if all_bool {
                    return "bool";
                }
                "string"
            })
            .collect();

        // Pass 2: build NDJSON with consistent types.
        let mut ndjson = String::new();
        for row in &rows {
            let mut obj = serde_json::Map::new();
            for (i, &col) in columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    let json_val = match col_types[i] {
                        "int" => val
                            .parse::<i64>()
                            .map(|n| serde_json::Value::Number(n.into()))
                            .unwrap_or(serde_json::Value::Null),
                        "float" => val
                            .parse::<f64>()
                            .ok()
                            .and_then(serde_json::Number::from_f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null),
                        "bool" => serde_json::Value::Bool(val == "true"),
                        _ => serde_json::Value::String(val.clone()),
                    };
                    obj.insert(col.to_string(), json_val);
                }
            }
            if id_col.is_none() {
                obj.insert(
                    "id".to_string(),
                    serde_json::Value::String(nodedb_types::id_gen::uuid_v7()),
                );
            }
            let row_json =
                serde_json::to_string(&obj).map_err(|e| NodeDbError::serialization("json", e))?;
            ndjson.push_str(&row_json);
            ndjson.push('\n');
        }
        self.copy_from_ndjson(collection, &ndjson).await
    }
}

/// Convert serde_json::Value to LoroValue for COPY FROM import.
fn json_to_loro(v: &serde_json::Value) -> loro::LoroValue {
    match v {
        serde_json::Value::Null => loro::LoroValue::Null,
        serde_json::Value::Bool(b) => loro::LoroValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                loro::LoroValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                loro::LoroValue::Double(f)
            } else {
                loro::LoroValue::String(n.to_string().into())
            }
        }
        serde_json::Value::String(s) => loro::LoroValue::String(s.clone().into()),
        serde_json::Value::Array(arr) => {
            loro::LoroValue::List(arr.iter().map(json_to_loro).collect::<Vec<_>>().into())
        }
        serde_json::Value::Object(map) => loro::LoroValue::Map(
            map.iter()
                .map(|(k, v)| (k.to_string(), json_to_loro(v)))
                .collect::<std::collections::HashMap<_, _>>()
                .into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_loro_roundtrip() {
        let json = serde_json::json!({
            "name": "Alice",
            "age": 30,
            "active": true,
            "score": 99.5,
            "tags": ["a", "b"]
        });
        let loro = json_to_loro(&json);
        assert!(matches!(loro, loro::LoroValue::Map(_)));
    }
}

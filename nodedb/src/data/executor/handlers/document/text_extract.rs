//! Text extraction utilities for full-text search indexing.

/// Extract all indexable text from a JSON document, including nested arrays.
///
/// For flat documents: concatenates all top-level string values.
/// For block documents: also recurses into arrays of objects, extracting
/// string fields like `content`, `text`, and any other string values.
/// This enables FTS across all blocks in a collaborative document.
pub fn extract_indexable_text(doc: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    collect_text(doc, &mut parts);
    parts.join(" ")
}

pub(super) fn collect_text(val: &serde_json::Value, parts: &mut Vec<String>) {
    match val {
        serde_json::Value::String(s) if !s.is_empty() => {
            parts.push(s.clone());
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                collect_text(v, parts);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                collect_text(item, parts);
            }
        }
        _ => {}
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Pure JSON/plan projection and flattening helpers for SELECT responses.
//!
//! These operate purely on parsed SQL and `serde_json::Value` — no pgwire
//! wire types — so they are shared across any protocol-specific response
//! shaper. Protocol-specific encode glue that turns these into wire rows
//! (e.g. pgwire's `DataRow`) lives in each protocol's own handler code.

/// Convert a JSON scalar value to its PostgreSQL text-format string.
///
/// - `String` values are returned as-is (no extra quoting).
/// - `Bool` uses PostgreSQL text format: `t` for true, `f` for false.
/// - All other scalars (`Number`, `Array`, `Object`) use their JSON
///   `Display` representation; arrays/objects should not normally appear
///   as individual cell values but are rendered faithfully.
pub fn json_value_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        // PostgreSQL text format for boolean is `t`/`f`.
        serde_json::Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        other => other.to_string(),
    }
}

/// Flatten a parsed JSON value into row objects.
pub fn push_flat_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                push_flat_rows(item, out);
            }
        }
        serde_json::Value::Object(mut map) => {
            if is_scan_wrapper(&map)
                && let Some(serde_json::Value::Object(mut inner)) = map.remove("data")
            {
                // The envelope carries the row's storage-key identity. A body
                // with no `id` field carries it nowhere else. `or_insert`
                // leaves a declared primary key as the authority.
                if let Some(id) = map.remove("id") {
                    inner.entry("id").or_insert(id);
                }
                out.push(inner);
                return;
            }
            out.push(map);
        }
        _ => {}
    }
}

/// The Data Plane's raw document-scan codec emits objects with exactly
/// the keys `id` (string) and `data` (object). This is the wire shape
/// we unwrap before column projection.
pub fn is_scan_wrapper(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(serde_json::Value::String(_)))
        && matches!(map.get("data"), Some(serde_json::Value::Object(_)))
}

/// Unique per-column cell keys for a shaped column list.
///
/// SQL output column names may legally repeat — `SELECT w.id, b.id` displays
/// both columns as `id` — but a shaped row is a JSON map and cannot hold two
/// cells under one key: inserting the second cell would overwrite the first,
/// making both wire columns render the last value. The shaper therefore
/// stores the first occurrence of a name under the name itself and each later
/// duplicate under `<name>_<n>` (n = 1, 2, …), skipping any candidate that
/// collides with another column's name. Row writers and every protocol
/// encoder derive keys through this one function so they always agree; for a
/// duplicate-free column list this is the identity mapping.
pub fn cell_keys(columns: &[String]) -> Vec<String> {
    use std::collections::HashSet;

    let names: HashSet<&str> = columns.iter().map(String::as_str).collect();
    let mut used: HashSet<String> = HashSet::with_capacity(columns.len());
    columns
        .iter()
        .map(|name| {
            if used.insert(name.clone()) {
                return name.clone();
            }
            let mut n = 1usize;
            loop {
                let candidate = format!("{name}_{n}");
                if !names.contains(candidate.as_str()) && used.insert(candidate.clone()) {
                    return candidate;
                }
                n += 1;
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::cell_keys;

    fn keys(cols: &[&str]) -> Vec<String> {
        cell_keys(&cols.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    /// Duplicate-free column lists map to themselves.
    #[test]
    fn cell_keys_is_identity_without_duplicates() {
        assert_eq!(keys(&["id", "name", "score"]), ["id", "name", "score"]);
        assert_eq!(keys(&[]), Vec::<String>::new());
    }

    /// Later duplicates get a `_n` suffix; the first occurrence keeps the
    /// bare name (`SELECT w.id, b.id` → `id`, `id_1`).
    #[test]
    fn cell_keys_suffixes_later_duplicates() {
        assert_eq!(keys(&["id", "id"]), ["id", "id_1"]);
        assert_eq!(keys(&["id", "id", "id"]), ["id", "id_1", "id_2"]);
    }

    /// A suffix candidate never collides with a real column of that name:
    /// with columns `id, id, id_1` the second `id` skips `id_1` (taken by a
    /// real column) and becomes `id_2`.
    #[test]
    fn cell_keys_skips_candidates_that_shadow_real_columns() {
        assert_eq!(keys(&["id", "id", "id_1"]), ["id", "id_2", "id_1"]);
    }
}

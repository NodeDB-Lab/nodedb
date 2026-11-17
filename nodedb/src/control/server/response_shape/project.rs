// SPDX-License-Identifier: BUSL-1.1

//! Pure projection and flattening helpers for SELECT responses.
//!
//! These operate on decoded `nodedb_types::Value` rows — no pgwire wire
//! types — so they are shared across any protocol-specific response shaper.
//! Protocol-specific encode glue that turns these into wire rows (e.g.
//! pgwire's `DataRow`) lives in each protocol's own handler code; the
//! per-cell text form lives in `response_shape::cell`.

use std::collections::HashMap;

use nodedb_types::Value;

use super::types::ShapedRow;
use crate::data::executor::response_codec::RowContext;

/// Flatten a decoded Data-Plane value into typed row objects.
///
/// The envelope `id` is a rendered [`StorageKey`](crate::engine::document::store::StorageKey)
/// by construction.
/// A value that fails `StorageKey::parse` is surfaced as `Err`, never accommodated.
///
/// `context` is the row shape of the payload's producer, from its plan. Only
/// a [`RowContext::ScanEnvelope`] row is unwrapped: a flat row whose columns
/// are `id` and `data` is a user row and stays as it is.
///
/// `identity_column` is the column a scan row's identity renders under: the
/// scanned collection's declared key, else the implicit `id`.
pub fn push_flat_rows(
    value: Value,
    context: RowContext,
    identity_column: &str,
    out: &mut Vec<ShapedRow>,
) -> crate::Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                push_flat_rows(item, context, identity_column, out)?;
            }
        }
        Value::Object(mut map) => {
            if context == RowContext::ScanEnvelope
                && is_scan_wrapper(&map)
                && let Some(Value::Object(inner)) = map.remove("data")
            {
                let mut inner: ShapedRow = inner.into_iter().collect();
                // The envelope carries the row's storage key, which is
                // internal. A body that lacks its identity column renders the
                // identity there, by the rule the Data Plane's row shaping
                // applies: the body's `_rowid`, else the storage key. A copy
                // under a new surrogate keeps its `_rowid`, so it keeps its
                // identity. A body that holds its identity column keeps the
                // stored value and gains no column: a declared key stays the
                // authority, and no `id` appears beside it.
                if let Some(Value::String(key)) = map.remove("id") {
                    let storage_key = crate::engine::document::store::StorageKey::parse(&key)
                        .ok_or_else(|| crate::Error::Internal {
                            detail: format!("scan envelope id is not a storage key: '{key}'"),
                        })?;
                    if !inner.contains_key(identity_column) {
                        let identity = match inner.get(nodedb_types::ROWID_COLUMN) {
                            Some(Value::Integer(rowid)) => rowid.to_string(),
                            _ => storage_key.to_identity().into_string(),
                        };
                        inner.insert(identity_column.to_string(), Value::String(identity));
                    }
                }
                out.push(inner);
                return Ok(());
            }
            out.push(map.into_iter().collect());
        }
        // A scalar is not a row: the shaper only ever answers with objects.
        Value::Null
        | Value::Bool(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::Bytes(_)
        | Value::Uuid(_)
        | Value::Ulid(_)
        | Value::DateTime(_)
        | Value::NaiveDateTime(_)
        | Value::Duration(_)
        | Value::Decimal(_)
        | Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        | Value::Vector(_) => {}
        // `Value` is `#[non_exhaustive]`: a variant this crate cannot name is
        // not a row either.
        _ => {}
    }
    Ok(())
}

/// Push each row's body: under [`RowContext::ScanEnvelope`] the `data`
/// object of a `{id, data}` envelope, else the row itself. Unlike
/// [`push_flat_rows`] no identity column is added. A scalar is not a row.
pub fn push_row_bodies(value: Value, context: RowContext, out: &mut Vec<ShapedRow>) {
    match value {
        Value::Array(items) => {
            for item in items {
                push_row_bodies(item, context, out);
            }
        }
        Value::Object(mut map) => {
            if context == RowContext::ScanEnvelope
                && is_scan_wrapper(&map)
                && let Some(Value::Object(inner)) = map.remove("data")
            {
                out.push(inner.into_iter().collect());
                return;
            }
            out.push(map.into_iter().collect());
        }
        _ => {}
    }
}

/// The Data Plane's raw document-scan codec emits objects with exactly
/// the keys `id` (string) and `data` (object). This is the wire shape
/// we unwrap before column projection.
pub fn is_scan_wrapper(map: &HashMap<String, Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(Value::String(_)))
        && matches!(map.get("data"), Some(Value::Object(_)))
}

/// [`is_scan_wrapper`] for a row still held as JSON: the same two-key
/// `{id: string, data: object}` shape, read off a `serde_json::Map`. The
/// JSON-only redaction paths (`redact_envelope_row`) check the envelope
/// without lifting the row to a typed value.
pub fn is_scan_wrapper_json(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.len() == 2
        && matches!(map.get("id"), Some(serde_json::Value::String(_)))
        && matches!(map.get("data"), Some(serde_json::Value::Object(_)))
}

/// Unique per-column cell keys for a shaped column list.
///
/// SQL output column names can legally repeat — `SELECT w.id, b.id` displays
/// both columns as `id` — but a shaped row is a JSON map and cannot hold two
/// cells under one key: inserting the second cell will overwrite the first,
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
    use super::{RowContext, Value, cell_keys, push_flat_rows};

    /// A scan envelope around `fields`, keyed by surrogate 7.
    fn envelope(fields: &[(&str, &str)]) -> Value {
        let key = nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(7));
        let data = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), Value::String((*v).to_string())))
            .collect();
        Value::Object(
            [
                ("id".to_string(), Value::String(key.to_string())),
                ("data".to_string(), Value::Object(data)),
            ]
            .into_iter()
            .collect(),
        )
    }

    fn flat(value: Value, identity_column: &str) -> Vec<(String, Value)> {
        let mut rows = Vec::new();
        push_flat_rows(value, RowContext::ScanEnvelope, identity_column, &mut rows)
            .expect("a well-formed envelope");
        assert_eq!(rows.len(), 1);
        rows.remove(0).into_iter().collect()
    }

    #[test]
    fn a_row_holding_its_declared_key_gains_no_column() {
        let row = flat(envelope(&[("sku", "p1"), ("name", "pen")]), "sku");
        assert_eq!(
            row,
            vec![
                ("name".to_string(), Value::String("pen".into())),
                ("sku".to_string(), Value::String("p1".into())),
            ]
        );
    }

    #[test]
    fn a_row_lacking_its_identity_column_renders_the_identity_there() {
        let row = flat(envelope(&[("name", "pen")]), "sku");
        assert!(row.contains(&("sku".to_string(), Value::String("7".into()))));
        assert!(!row.iter().any(|(k, _)| k == "id"), "{row:?}");

        let row = flat(envelope(&[("name", "pen")]), "id");
        assert!(row.contains(&("id".to_string(), Value::String("7".into()))));
    }

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

    /// A flat row whose columns are `id` and `data` is a user row: it is
    /// never unwrapped, whatever `data` holds.
    #[test]
    fn a_flat_id_data_row_is_never_unwrapped() {
        let row = envelope(&[("k", "v")]);
        let mut rows = Vec::new();
        push_flat_rows(row.clone(), RowContext::Flat, "id", &mut rows).expect("flat row");
        let Value::Object(fields) = row else {
            panic!("row is an object");
        };
        assert_eq!(rows, vec![fields.into_iter().collect()]);
    }
}

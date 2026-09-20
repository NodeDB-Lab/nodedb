// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for `FieldSet` (HSET-style field merge), shared by
//! the autocommit handler (`field.rs`), the predicate update, the transaction
//! resolver, in-transaction staging, and WAL replay, so a staged value and its
//! COMMIT-time durable replay are always computed by the exact same code —
//! mirrors the `engine_atomic_compute` / `stage_kv_atomic` split for
//! `Incr`/`Cas`/etc.

use nodedb_query::msgpack_scan::{KvBodyError, KvBodyShape, kv_body_to_row, row_to_kv_body};
use nodedb_types::Value;

use crate::bridge::envelope::ErrorCode;

/// Result of merging field updates into a KV document body.
#[derive(Debug)]
pub(in crate::data::executor) struct FieldSetComputation {
    /// The re-encoded (plain msgpack, not zerompk-tagged) document body.
    pub new_value: Vec<u8>,
    /// Count of updated fields that did not previously exist on the document.
    pub fields_added: u64,
}

/// Merge `updates` into the map body `current` (an absent key starts from an
/// empty map) and re-encode.
///
/// A raw scalar body (the single-`value` SQL form, RESP `SET`) is not a
/// hash: a field set against it is a type mismatch, the verdict Redis gives
/// `HSET` on a string key. It is never silently replaced by a map.
pub(in crate::data::executor) fn merge_field_updates(
    collection: &str,
    current: Option<&[u8]>,
    updates: &[(String, Vec<u8>)],
) -> Result<FieldSetComputation, ErrorCode> {
    let mut doc = match current {
        None => std::collections::HashMap::new(),
        Some(body) => {
            let (row, shape) = kv_body_to_row(body)
                .map_err(|e| ErrorCode::from(crate::Error::from(KvBodyError::from(e))))?;
            if shape == KvBodyShape::Raw {
                return Err(ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail: "key holds a bare value, not a hash; field set requires typed \
                             columns"
                        .into(),
                });
            }
            let kind = row.type_name();
            let Value::Object(map) = row else {
                return Err(ErrorCode::from(crate::Error::from(
                    KvBodyError::RowNotObject { kind },
                )));
            };
            map
        }
    };

    let mut fields_added = 0u64;
    for (field, value_bytes) in updates {
        let new_value = if value_bytes.is_empty() {
            Value::Null
        } else {
            nodedb_types::value_from_msgpack(value_bytes).map_err(|e| ErrorCode::Internal {
                detail: format!("field set '{field}': msgpack decode: {e}"),
            })?
        };
        if !doc.contains_key(field) {
            fields_added += 1;
        }
        doc.insert(field.clone(), new_value);
    }

    let new_value = row_to_kv_body(&Value::Object(doc), KvBodyShape::Map)
        .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;

    Ok(FieldSetComputation {
        new_value,
        fields_added,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i64) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&Value::Integer(v)).unwrap()
    }

    fn decode(bytes: &[u8]) -> Value {
        nodedb_types::value_from_msgpack(bytes).unwrap()
    }

    #[test]
    fn merges_into_empty_document() {
        let result = merge_field_updates("c", None, &[("score".to_string(), int(42))]).unwrap();
        assert_eq!(result.fields_added, 1);
        assert_eq!(
            decode(&result.new_value).get("score"),
            Some(&Value::Integer(42))
        );
    }

    #[test]
    fn overwrite_does_not_count_as_added() {
        let existing = {
            let mut m = std::collections::HashMap::new();
            m.insert("score".to_string(), Value::Integer(1));
            nodedb_types::value_to_msgpack(&Value::Object(m)).unwrap()
        };
        let result =
            merge_field_updates("c", Some(&existing), &[("score".to_string(), int(2))]).unwrap();
        assert_eq!(result.fields_added, 0);
        assert_eq!(
            decode(&result.new_value).get("score"),
            Some(&Value::Integer(2))
        );
    }

    #[test]
    fn empty_value_bytes_set_null() {
        let result = merge_field_updates("c", None, &[("f".to_string(), Vec::new())]).unwrap();
        assert_eq!(decode(&result.new_value).get("f"), Some(&Value::Null));
    }

    #[test]
    fn raw_body_is_a_type_mismatch_not_replaced_by_a_map() {
        for body in [b"first".as_slice(), b"1".as_slice()] {
            let err = merge_field_updates("c", Some(body), &[("f".to_string(), int(1))])
                .expect_err("HSET on a bare-value key must be refused");
            match err {
                ErrorCode::TypeMismatch { collection, .. } => assert_eq!(collection, "c"),
                other => panic!("expected TypeMismatch, got {other:?}"),
            }
        }
    }

    #[test]
    fn corrupt_map_body_is_an_error_not_an_empty_document() {
        // fixmap header claiming one entry, then nothing.
        let err = merge_field_updates("c", Some(&[0x81]), &[("f".to_string(), int(1))])
            .expect_err("a truncated body must not merge onto an empty map");
        assert!(!matches!(err, ErrorCode::TypeMismatch { .. }), "{err:?}");
    }
}

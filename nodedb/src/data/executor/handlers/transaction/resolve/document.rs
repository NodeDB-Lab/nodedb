// SPDX-License-Identifier: BUSL-1.1

//! Document serializer for transaction resolve.
//!
//! Turns the document post-images a transaction staged into its overlay into
//! the engine-native WAL sub-record shapes the document redo replay path
//! decodes (`wal_replay_redo_document.rs`) — the SAME shapes the autocommit
//! document WAL path produces, so producer and replay share one encoding:
//!
//! * A staged value ([`Staged::Put`]) → `RecordType::Put`,
//!   `(collection, document_id, value, Option<SyncProvenance>, surrogate)`. For
//!   a `bitemporal=true` collection this becomes an 8-tuple that appends the
//!   resolve-time stamp `(sys_from_ms, valid_from_ms, valid_until_ms)` so replay
//!   restores the row on the versioned store at the same stamp the commit-time
//!   install uses. The replay decoder distinguishes the two forms by arity.
//! * A staged tombstone ([`Staged::Tombstone`]) → `RecordType::Delete`,
//!   `(collection, document_id, Option<SyncProvenance>, surrogate)`. The redo
//!   delete shape carries the surrogate (unlike the autocommit delete shape)
//!   because replay keys redb by `surrogate_to_doc_id(surrogate)`.
//!
//! ## Stored form vs replay input
//!
//! The overlay body is in STORED form, which differs by storage mode:
//!
//! * **Schemaless** collections store canonical MessagePack — emitted verbatim.
//! * **Strict** collections store a Binary Tuple, NOT MessagePack. The redo
//!   `value` is consumed on replay by `apply_point_put`, which re-encodes it
//!   via `bytes_to_binary_tuple` and therefore expects MessagePack. Emitting
//!   the Binary Tuple verbatim would fail that decode and silently drop every
//!   strict-mode row. So a strict body is decoded back to canonical MessagePack
//!   with [`strict_format::binary_tuple_to_msgpack`] — the exact `Value →
//!   MessagePack` inverse of the `MessagePack → Value → Binary Tuple` pipeline
//!   `bytes_to_binary_tuple` runs on replay, so the round-trip is lossless (no
//!   JSON intermediary that could drop type fidelity).
//!
//! ## `SyncProvenance`
//!
//! Ordinary transaction writes carry no CRDT sync provenance, so the fourth
//! tuple element is `None` — matching the autocommit redo producer and the
//! replay decoder, both of which treat it as `Option<SyncProvenance>`.
//!
//! ## Determinism
//!
//! The overlay keys slots by surrogate in a `HashMap`, so entries are collected
//! into a `BTreeMap` keyed by the row's client identity before emitting. Two
//! replicas resolving the same transaction produce byte-identical redo ops.

use std::collections::BTreeMap;

use nodedb_types::RowIdentity;
use nodedb_types::columnar::StrictSchema;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::data::executor::handlers::transaction::overlay::{Staged, TxnOverlay};
use crate::data::executor::strict_format;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

/// Append the redo sub-records for every document post-image staged in
/// `overlay` for `coll_key` to `ops`, in deterministic doc-id order.
///
/// `strict_schema` is `Some` for a strict (Binary Tuple) collection and `None`
/// for a schemaless (already MessagePack) one; a strict `Put` body is decoded
/// back to MessagePack before emission so replay's `bytes_to_binary_tuple` can
/// re-encode it (see module docs).
pub(super) fn serialize_document_collection(
    overlay: &TxnOverlay,
    coll_key: &(DatabaseId, TenantId, String),
    collection: &str,
    strict_schema: Option<&StrictSchema>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut entries: BTreeMap<&RowIdentity, (u32, &Staged)> = BTreeMap::new();
    for (doc_id, staged) in overlay.iter_doc_entries_for_collection(coll_key) {
        let surrogate = overlay
            .surrogate_for_doc_id(coll_key, doc_id)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "document resolve: staged doc-id '{doc_id}' has no bound surrogate"
                ),
            })?;
        entries.insert(doc_id, (surrogate, staged));
    }

    for (doc_id, (surrogate, staged)) in entries {
        match staged {
            Staged::Put(body) => {
                let value = match strict_schema {
                    Some(schema) => strict_format::binary_tuple_to_msgpack(body, schema)
                        .ok_or_else(|| crate::Error::Storage {
                            engine: "binary_tuple".into(),
                            detail: format!(
                                "document resolve: failed to decode Binary Tuple for staged \
                                 put of '{doc_id}'"
                            ),
                        })?,
                    None => body.clone(),
                };
                let prov: Option<SyncProvenance> = None;
                // A `bitemporal=true` collection's staged put carries the
                // resolve-time system/valid-time stamp assigned by
                // `resolve_txn_ops` into the overlay sidecar. Emit it as the
                // trailing three elements of an 8-tuple so WAL replay installs
                // the row on the VERSIONED store at the SAME stamp the
                // commit-time base install uses (both read one stamp). A
                // non-bitemporal collection has no sidecar entry and keeps the
                // 5-tuple; the replay decoder distinguishes the two by arity.
                let payload = match overlay.get_bitemporal(coll_key, surrogate) {
                    Some(stamp) => zerompk::to_msgpack_vec(&(
                        collection,
                        doc_id.as_str(),
                        value,
                        prov,
                        surrogate,
                        stamp.sys_from_ms,
                        stamp.valid_from_ms,
                        stamp.valid_until_ms,
                    )),
                    None => zerompk::to_msgpack_vec(&(
                        collection,
                        doc_id.as_str(),
                        value,
                        prov,
                        surrogate,
                    )),
                }
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("document resolve put: {e}"),
                })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload,
                });
            }
            Staged::Tombstone => {
                let prov: Option<SyncProvenance> = None;
                let payload =
                    zerompk::to_msgpack_vec(&(collection, doc_id.as_str(), prov, surrogate))
                        .map_err(|e| crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("document resolve delete: {e}"),
                        })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Delete as u32,
                    payload,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn coll_key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(DB), TenantId::new(TID), coll.to_string())
    }

    fn strict_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("body", ColumnType::String),
        ])
        .unwrap()
    }

    /// A strict stored body (Binary Tuple) carrying `_rowid` + `body`, exactly
    /// as the strict staging path would leave it in the overlay.
    fn strict_tuple(rowid: i64, body: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("_rowid".to_string(), Value::Integer(rowid));
        obj.insert("body".to_string(), Value::String(body.to_string()));
        strict_format::value_to_binary_tuple(&Value::Object(obj), &strict_schema(), "docs")
            .expect("encode binary tuple")
    }

    /// A schemaless stored body: canonical MessagePack.
    fn schemaless_body(name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("name".to_string(), Value::String(name.to_string()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).expect("encode msgpack")
    }

    fn id(text: &str) -> RowIdentity {
        RowIdentity::from_user_key(text)
    }

    #[test]
    fn strict_put_emits_msgpack_not_binary_tuple() {
        let schema = strict_schema();
        let tuple = strict_tuple(7, "elephant");
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(coll_key("docs"), 7, &id("row1"), tuple.clone());

        let mut ops = Vec::new();
        serialize_document_collection(&overlay, &coll_key("docs"), "docs", Some(&schema), &mut ops)
            .expect("serialize strict");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Put as u32);

        let (collection, doc_id, value, prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                &ops[0].payload,
            )
            .expect("decode document put tuple");
        assert_eq!(collection, "docs");
        assert_eq!(doc_id, "row1");
        assert_eq!(surrogate, 7);
        assert!(prov.is_none());

        // The critical assertion: the emitted value is NOT the stored Binary
        // Tuple; it is canonical MessagePack that decodes to the document.
        assert_ne!(
            value, tuple,
            "strict body must be decoded to MessagePack, never emitted as the Binary Tuple"
        );
        let decoded = nodedb_types::value_from_msgpack(&value).expect("value is msgpack");
        match decoded {
            Value::Object(map) => {
                assert_eq!(map.get("body"), Some(&Value::String("elephant".into())));
            }
            other => panic!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn schemaless_put_emits_body_verbatim() {
        let body = schemaless_body("alice");
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(coll_key("notes"), 3, &id("userpk"), body.clone());

        let mut ops = Vec::new();
        serialize_document_collection(&overlay, &coll_key("notes"), "notes", None, &mut ops)
            .expect("serialize schemaless");
        assert_eq!(ops.len(), 1);

        let (_c, _d, value, _p, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                &ops[0].payload,
            )
            .expect("decode");
        assert_eq!(surrogate, 3);
        assert_eq!(value, body, "schemaless body must be emitted verbatim");
    }

    #[test]
    fn tombstone_emits_delete_carrying_surrogate() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_tombstone(coll_key("notes"), 11, &id("gone"));

        let mut ops = Vec::new();
        serialize_document_collection(&overlay, &coll_key("notes"), "notes", None, &mut ops)
            .expect("serialize delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Delete as u32);

        let (collection, doc_id, prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(&ops[0].payload)
                .expect("decode document delete tuple");
        assert_eq!(collection, "notes");
        assert_eq!(doc_id, "gone");
        assert!(prov.is_none());
        assert_eq!(surrogate, 11, "delete tuple must carry the surrogate");
    }

    #[test]
    fn entries_emit_in_deterministic_doc_id_order() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(coll_key("notes"), 30, &id("c"), schemaless_body("c"));
        overlay.insert_put(coll_key("notes"), 10, &id("a"), schemaless_body("a"));
        overlay.insert_put(coll_key("notes"), 20, &id("b"), schemaless_body("b"));

        let mut ops = Vec::new();
        serialize_document_collection(&overlay, &coll_key("notes"), "notes", None, &mut ops)
            .expect("serialize");
        let doc_ids: Vec<String> = ops
            .iter()
            .map(|op| {
                zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                    &op.payload,
                )
                .expect("decode")
                .1
            })
            .collect();
        assert_eq!(doc_ids, vec!["a", "b", "c"], "doc-id ascending order");
    }
}

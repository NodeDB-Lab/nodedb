// SPDX-License-Identifier: BUSL-1.1

//! Columnar serializer for transaction resolve. Overlay-driven.
//!
//! Every columnar write a transaction runs is staged per surrogate: a plain
//! INSERT and an `ON CONFLICT DO UPDATE` stage the row as it will exist, an
//! UPDATE stages its post-image, and a DELETE stages a tombstone. That staged
//! image is what the transaction's own reads showed. The redo record carries
//! it verbatim as one `columnar_image` record per collection
//! (`ColumnarImageWalRecord`). Replay installs the image and never re-runs a
//! merge, a SET list or a predicate against the replaying node's state.
//!
//! * A staged put → a row with its image. When the statement that first
//!   staged the surrogate matched a base row (an UPDATE), the row also names
//!   that base row's primary key, and replay removes the base row before it
//!   installs the image. A key-changing UPDATE therefore leaves no row behind
//!   under the old key.
//! * A staged tombstone → a row with the base key and no image. A tombstone
//!   for a row the transaction itself inserted names no base row and emits
//!   nothing.
//! * A staged TRUNCATE → the `ColumnarTruncate` record ahead of the rows.
//!   The truncate removes every base row, so rows after it carry no base key.
//!
//! Rows are emitted in surrogate order, so two resolves of one transaction
//! produce byte-identical records.
//!
//! Session and Calvin transactions both stage every columnar write, so both
//! resolve here.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::ColumnarOp;
use nodedb_types::columnar::{
    COLUMNAR_IMAGE_KIND, ColumnarImageWalRecord, ColumnarImageWalRow, ColumnarSchema,
};
use nodedb_types::value::Value;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::encode_columnar_truncate_payload;
use crate::data::executor::handlers::columnar_write::row_values_to_object;
use crate::data::executor::handlers::transaction::overlay::{
    Staged, TxnOverlay, decode_staged_row,
};
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

/// The columnar collections a transaction wrote, each with the catalog schema
/// its INSERT plans carried (empty when none carried one).
pub(super) type ColumnarCollections = BTreeMap<String, Vec<u8>>;

/// Collect the collection of every columnar write in `op`. Reads contribute
/// nothing. A sync-provenance INSERT is refused: the sync ingest path applies
/// it outside any transaction, and a transaction's image record carries no
/// sync high-water mark.
pub(super) fn classify_columnar_op(
    op: &ColumnarOp,
    collections: &mut ColumnarCollections,
) -> crate::Result<()> {
    match op {
        ColumnarOp::Insert {
            collection,
            schema_bytes,
            provenance,
            ..
        } => {
            if provenance.is_some() {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "columnar insert into '{collection}' carries sync provenance, which \
                         the sync ingest path applies outside any transaction"
                    ),
                });
            }
            let entry = collections.entry(collection.to_string()).or_default();
            if entry.is_empty() {
                entry.clone_from(schema_bytes);
            }
            Ok(())
        }
        ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. }
        | ColumnarOp::ResolvedUpdate { collection, .. }
        | ColumnarOp::ResolvedDelete { collection, .. }
        | ColumnarOp::Truncate { collection, .. } => {
            collections.entry(collection.to_string()).or_default();
            Ok(())
        }
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => Ok(()),
    }
}

/// What [`serialize_columnar_collection`] reads for one collection.
pub(super) struct ColumnarCollectionImages<'a> {
    pub overlay: &'a TxnOverlay,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    /// The collection's engine schema. `None` when no engine exists on this
    /// core, which is legal only when the transaction staged no put.
    pub schema: Option<&'a ColumnarSchema>,
    pub schema_bytes: &'a [u8],
}

/// Append the truncate record (when the transaction truncated the
/// collection) and the collection's `columnar_image` record to `ops`.
pub(super) fn serialize_columnar_collection(
    params: ColumnarCollectionImages<'_>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let ColumnarCollectionImages {
        overlay,
        coll_key,
        schema,
        schema_bytes,
    } = params;
    let collection = coll_key.2.as_str();
    let truncated = overlay.is_truncated(coll_key);
    if truncated {
        ops.push(RedoSubRecord {
            record_type: RecordType::ColumnarTruncate as u32,
            payload: encode_columnar_truncate_payload(collection)?,
        });
    }

    let entries: BTreeMap<u32, &Staged> = overlay.iter_for_collection(coll_key).collect();
    let mut rows = Vec::with_capacity(entries.len());
    for (surrogate, staged) in entries {
        let prior_pk_msgpack = if truncated {
            Vec::new()
        } else {
            overlay
                .base_pk(coll_key, surrogate)
                .map(<[u8]>::to_vec)
                .unwrap_or_default()
        };
        let image_msgpack = match staged {
            Staged::Put(body) => staged_image(collection, schema, surrogate, body)?,
            Staged::Tombstone if prior_pk_msgpack.is_empty() => continue,
            Staged::Tombstone => Vec::new(),
        };
        rows.push(ColumnarImageWalRow {
            surrogate,
            prior_pk_msgpack,
            image_msgpack,
        });
    }
    if rows.is_empty() {
        return Ok(());
    }

    let record = ColumnarImageWalRecord {
        kind: COLUMNAR_IMAGE_KIND.to_string(),
        collection: collection.to_string(),
        schema_bytes: schema_bytes.to_vec(),
        rows,
    };
    let payload = zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar image record for '{collection}': {e}"),
    })?;
    ops.push(RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload,
    });
    Ok(())
}

/// The staged row body as a column-name object, MessagePack-encoded.
fn staged_image(
    collection: &str,
    schema: Option<&ColumnarSchema>,
    surrogate: u32,
    body: &[u8],
) -> crate::Result<Vec<u8>> {
    let schema = schema.ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "columnar resolve: '{collection}' has a staged row (surrogate {surrogate}) but no \
             engine schema on this core"
        ),
    })?;
    let values = decode_staged_row(body).ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "columnar resolve: staged row of '{collection}' (surrogate {surrogate}) does not \
             decode"
        ),
    })?;
    if values.len() != schema.columns.len() {
        return Err(crate::Error::Internal {
            detail: format!(
                "columnar resolve: staged row of '{collection}' (surrogate {surrogate}) has {} \
                 values for {} columns",
                values.len(),
                schema.columns.len()
            ),
        });
    }
    let image: Value = row_values_to_object(schema, &values);
    nodedb_types::value_to_msgpack(&image).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar resolve image of '{collection}': {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::RowIdentity;
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    fn coll_key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(1), "m".to_string())
    }

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Int64),
        ])
        .expect("valid schema")
    }

    fn body(id: &str, v: i64) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&Value::Array(vec![
            Value::String(id.into()),
            Value::Integer(v),
        ]))
        .expect("encode staged row")
    }

    fn decode(op: &RedoSubRecord) -> ColumnarImageWalRecord {
        zerompk::from_msgpack(&op.payload).expect("decode image record")
    }

    fn serialize(overlay: &TxnOverlay) -> Vec<RedoSubRecord> {
        let schema = schema();
        let mut ops = Vec::new();
        serialize_columnar_collection(
            ColumnarCollectionImages {
                overlay,
                coll_key: &coll_key(),
                schema: Some(&schema),
                schema_bytes: &[],
            },
            &mut ops,
        )
        .expect("serialize");
        ops
    }

    #[test]
    fn a_staged_put_carries_the_image_the_transaction_was_shown() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(
            coll_key(),
            5,
            &RowIdentity::from_user_key("a"),
            body("a", 7),
        );
        let ops = serialize(&overlay);
        assert_eq!(ops.len(), 1);
        let record = decode(&ops[0]);
        assert_eq!(record.kind, COLUMNAR_IMAGE_KIND);
        assert_eq!(record.rows.len(), 1);
        assert!(record.rows[0].prior_pk_msgpack.is_empty());
        let image =
            nodedb_types::value_from_msgpack(&record.rows[0].image_msgpack).expect("image decodes");
        let Value::Object(map) = image else {
            panic!("image must be an object");
        };
        assert_eq!(map.get("v"), Some(&Value::Integer(7)));
        assert_eq!(map.get("id"), Some(&Value::String("a".into())));
    }

    #[test]
    fn a_tombstone_of_a_base_row_names_its_key_and_an_own_insert_emits_nothing() {
        let mut overlay = TxnOverlay::new();
        let key = nodedb_types::value_to_msgpack(&Value::String("b".into())).expect("key");
        overlay.note_base_pk(&coll_key(), 6, key.clone());
        overlay.insert_tombstone(coll_key(), 6, &RowIdentity::from_user_key("b"));
        overlay.insert_tombstone(coll_key(), 9, &RowIdentity::from_user_key("own"));
        let record = decode(&serialize(&overlay)[0]);
        assert_eq!(record.rows.len(), 1);
        assert_eq!(record.rows[0].surrogate, 6);
        assert_eq!(record.rows[0].prior_pk_msgpack, key);
        assert!(record.rows[0].image_msgpack.is_empty());
    }

    #[test]
    fn a_truncate_precedes_the_rows_and_drops_their_base_keys() {
        let mut overlay = TxnOverlay::new();
        let key = nodedb_types::value_to_msgpack(&Value::String("c".into())).expect("key");
        overlay.note_base_pk(&coll_key(), 4, key);
        overlay.mark_truncated(coll_key());
        overlay.insert_put(
            coll_key(),
            4,
            &RowIdentity::from_user_key("c"),
            body("c", 1),
        );
        let ops = serialize(&overlay);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].record_type, RecordType::ColumnarTruncate as u32);
        let record = decode(&ops[1]);
        assert!(record.rows[0].prior_pk_msgpack.is_empty());
    }

    #[test]
    fn rows_emit_in_surrogate_order() {
        let mut overlay = TxnOverlay::new();
        for (s, id) in [(30, "c"), (10, "a"), (20, "b")] {
            overlay.insert_put(coll_key(), s, &RowIdentity::from_user_key(id), body(id, 1));
        }
        let record = decode(&serialize(&overlay)[0]);
        let order: Vec<u32> = record.rows.iter().map(|r| r.surrogate).collect();
        assert_eq!(order, vec![10, 20, 30]);
    }
}

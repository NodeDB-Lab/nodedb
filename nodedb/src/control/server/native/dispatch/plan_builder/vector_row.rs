// SPDX-License-Identifier: BUSL-1.1

//! The row writes of a `VectorInsert` that carries the row's fields, and of
//! a keyed `VectorBatchInsert`.
//!
//! A row's non-vector fields are a native object `[7, {field: tagged value}]`
//! (`VectorInsert::data`, `BatchVector::metadata`), so every field keeps its
//! exact type. The vector and the fields land as one row under its id:
//!
//! - A vector-primary collection stores the vector in its HNSW index and the
//!   fields as the row's sidecar, replacing the row the id held. This is the
//!   `VectorOp::DirectUpsert` an SQL `UPSERT` plans. A batch is one
//!   `VectorOp::DirectBatchWrite` page.
//! - A document collection merges the fields and the vector, under its
//!   vector column, into the document. This is the `DocumentOp::Upsert` a
//!   native upsert plans, and a batch is one `DocumentOp::BatchUpsert`. The
//!   document write indexes the vector.
//!
//! A batch lands whole or not at all.

use std::collections::HashMap;

use nodedb_types::protocol::{BatchVector, TextFields};
use nodedb_types::{
    CollectionType, DEFAULT_IDENTITY_COLUMN, PrimaryEngine, QualifiedCollection, RlsWriteCheck,
    Value, VectorPrimaryConfig,
};

use super::super::DispatchCtx;
use super::document_identity::{
    check_body_id, identified_body, identity_column, stores_schemaless_bodies,
};
use super::{declared_primary_key, require_doc_id};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    DocumentOp, VectorDirectRow, VectorDirectWriteIntent, VectorOp,
};

/// The vector column a document collection's field-less vector index covers.
const DEFAULT_VECTOR_COLUMN: &str = "embedding";

/// Where a collection's vector rows land.
enum Target {
    /// A vector-primary collection: HNSW plus a sidecar keyed by
    /// `key_column`.
    VectorPrimary {
        cfg: VectorPrimaryConfig,
        key_column: String,
    },
    /// A document collection: the vector under `vector_column` in the
    /// document. `identity` is the identity column a schemaless body names
    /// its id under, `None` for a strict body.
    Document {
        vector_column: String,
        identity: Option<String>,
    },
}

/// Resolve where `collection`'s vector rows land. `field_name` names the
/// vector column. An empty name is the collection's default.
fn resolve_target(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    field_name: &str,
) -> crate::Result<Target> {
    let stored = ctx.state.credentials.catalog().get_collection(
        ctx.database_id(),
        ctx.identity.tenant_id.as_u64(),
        collection,
    )?;
    match stored {
        Some(coll) if coll.primary == PrimaryEngine::Vector => {
            let Some(cfg) = coll.vector_primary else {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "vector insert into '{collection}': the vector-primary collection \
                         has no vector configuration"
                    ),
                });
            };
            if !field_name.is_empty() && field_name != cfg.vector_field {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "vector insert into '{collection}': field '{field_name}' is not the \
                         collection's vector column '{}'",
                        cfg.vector_field
                    ),
                });
            }
            let key_column = declared_primary_key(ctx, collection)?
                .unwrap_or_else(|| DEFAULT_IDENTITY_COLUMN.to_string());
            Ok(Target::VectorPrimary { cfg, key_column })
        }
        Some(coll) if !matches!(coll.collection_type, CollectionType::Document(_)) => {
            Err(crate::Error::BadRequest {
                detail: format!(
                    "vector insert into '{collection}': a {} collection holds no vector rows",
                    coll.collection_type.as_str()
                ),
            })
        }
        stored => {
            let schemaless =
                stores_schemaless_bodies(stored.as_ref().map(|coll| &coll.collection_type));
            let identity = if schemaless {
                Some(identity_column(declared_primary_key(ctx, collection)?))
            } else {
                None
            };
            let vector_column = if field_name.is_empty() {
                DEFAULT_VECTOR_COLUMN
            } else {
                field_name
            };
            Ok(Target::Document {
                vector_column: vector_column.to_string(),
                identity,
            })
        }
    }
}

/// Plan the row write of a `VectorInsert` whose `data` holds `body`.
pub(super) async fn build_row_insert(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
    vector: Vec<f32>,
    body: &[u8],
) -> crate::Result<PhysicalPlan> {
    let doc_id = require_doc_id(fields)?;
    let row = row_fields(&doc_id, body)?;
    let field_name = fields.field_name.as_deref().unwrap_or_default();
    let qualified = QualifiedCollection::new(ctx.database_id(), collection);
    match resolve_target(ctx, collection, field_name)? {
        Target::VectorPrimary { cfg, key_column } => {
            let payload = sidecar_payload(&doc_id, &cfg, &key_column, row)?;
            let pk_bytes = doc_id.into_bytes();
            let surrogate = super::helpers::assign_surrogate(ctx, collection, &pk_bytes).await?;
            Ok(PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: qualified,
                field: cfg.vector_field,
                surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes,
                // The native protocol has no RETURNING clause.
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: Vec::new(),
                rls_write_check: RlsWriteCheck::pending_injection(),
            }))
        }
        Target::Document {
            vector_column,
            identity,
        } => {
            let target = DocumentRow {
                doc_id: &doc_id,
                vector_column: &vector_column,
                identity: identity.as_deref(),
            };
            let value = target.body(ctx, collection, vector, row)?;
            let surrogate =
                super::helpers::assign_surrogate(ctx, collection, doc_id.as_bytes()).await?;
            Ok(PhysicalPlan::Document(DocumentOp::Upsert {
                collection: qualified,
                document_id: doc_id,
                value,
                on_conflict_updates: Vec::new(),
                surrogate,
                rls_write_check: RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }))
        }
    }
}

/// Plan a keyed `VectorBatchInsert`: every vector is a row under its id,
/// written exactly as one [`build_row_insert`] writes it, in one page.
pub(super) async fn build_row_batch(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
    batch: &[BatchVector],
) -> crate::Result<PhysicalPlan> {
    let field_name = fields.field_name.as_deref().unwrap_or_default();
    let qualified = QualifiedCollection::new(ctx.database_id(), collection);
    let target = resolve_target(ctx, collection, field_name)?;
    let pks: Vec<&[u8]> = batch.iter().map(|v| v.id.as_bytes()).collect();
    let surrogates = super::helpers::assign_surrogates(ctx, collection, &pks).await?;
    match target {
        Target::VectorPrimary { cfg, key_column } => {
            let mut rows = Vec::with_capacity(batch.len());
            for (vector, surrogate) in batch.iter().zip(surrogates) {
                let row = metadata_fields(vector)?;
                rows.push(VectorDirectRow {
                    surrogate,
                    pk_bytes: vector.id.as_bytes().to_vec(),
                    vector: vector.embedding.clone(),
                    payload: sidecar_payload(&vector.id, &cfg, &key_column, row)?,
                });
            }
            Ok(PhysicalPlan::Vector(VectorOp::DirectBatchWrite {
                collection: qualified,
                field: cfg.vector_field,
                intent: VectorDirectWriteIntent::Upsert,
                rows,
                quantization: cfg.quantization,
                storage_dtype: cfg.storage_dtype,
                payload_indexes: cfg.payload_indexes,
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: Vec::new(),
                rls_write_check: RlsWriteCheck::pending_injection(),
            }))
        }
        Target::Document {
            vector_column,
            identity,
        } => {
            let mut documents = Vec::with_capacity(batch.len());
            for vector in batch {
                let target = DocumentRow {
                    doc_id: &vector.id,
                    vector_column: &vector_column,
                    identity: identity.as_deref(),
                };
                let value = target.body(
                    ctx,
                    collection,
                    vector.embedding.clone(),
                    metadata_fields(vector)?,
                )?;
                documents.push((vector.id.clone(), value));
            }
            Ok(PhysicalPlan::Document(DocumentOp::BatchUpsert {
                collection: qualified,
                documents,
                surrogates,
                on_conflict_updates: Vec::new(),
                rls_write_check: RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }))
        }
    }
}

/// Whether a batch is keyed: every vector names its id. A batch where no
/// vector names an id and none carries metadata is headless. A mix, or
/// metadata on an id-less vector, is refused: a row needs its id.
pub(super) fn batch_is_keyed(batch: &[BatchVector]) -> crate::Result<bool> {
    let keyed = batch.iter().filter(|v| !v.id.is_empty()).count();
    if keyed == batch.len() {
        return Ok(true);
    }
    if let Some(position) = batch
        .iter()
        .position(|v| v.id.is_empty() && v.metadata.is_some())
    {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "vector batch insert: vector {position} carries metadata but no id; \
                 a row with fields needs its id"
            ),
        });
    }
    if keyed > 0 {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "vector batch insert: {keyed} of {} vectors name an id; name an id on \
                 every vector or on none",
                batch.len()
            ),
        });
    }
    Ok(false)
}

/// The fields of one batch vector: its metadata object, or none.
fn metadata_fields(vector: &BatchVector) -> crate::Result<HashMap<String, Value>> {
    match &vector.metadata {
        None => Ok(HashMap::new()),
        Some(Value::Object(fields)) => Ok(fields.clone()),
        Some(other) => Err(crate::Error::BadRequest {
            detail: format!(
                "vector row '{}': metadata holds {}, expected an object",
                vector.id,
                other.type_name()
            ),
        }),
    }
}

/// The row's fields from a native object body.
fn row_fields(doc_id: &str, body: &[u8]) -> crate::Result<HashMap<String, Value>> {
    let view = nodedb_types::ObjectView::new(body).map_err(|e| crate::Error::BadRequest {
        detail: format!("vector row '{doc_id}': 'data' is not a MessagePack object: {e}"),
    })?;
    if view.format() != nodedb_types::ObjectFormat::Native {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "vector row '{doc_id}': 'data' is a plain MessagePack map; \
                 a vector row's fields are a native object of tagged values"
            ),
        });
    }
    match view.decode().map_err(|e| crate::Error::BadRequest {
        detail: format!("vector row '{doc_id}': 'data' does not decode: {e}"),
    })? {
        Value::Object(row) => Ok(row),
        other => Err(crate::Error::BadRequest {
            detail: format!(
                "vector row '{doc_id}': 'data' decodes as {}, expected an object",
                other.type_name()
            ),
        }),
    }
}

/// Refuse a field that names the vector column: the vector travels as the
/// embedding, and a field there would overwrite it.
fn refuse_vector_field(
    doc_id: &str,
    row: &HashMap<String, Value>,
    vector_column: &str,
) -> crate::Result<()> {
    if row.contains_key(vector_column) {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "vector row '{doc_id}': field '{vector_column}' is the vector column; \
                 send the vector as the embedding, not as a field"
            ),
        });
    }
    Ok(())
}

/// The sidecar of a vector-primary row: its fields, with its key under the
/// identity column, as the SQL write stores it.
fn sidecar_payload(
    doc_id: &str,
    cfg: &VectorPrimaryConfig,
    key_column: &str,
    mut row: HashMap<String, Value>,
) -> crate::Result<Vec<u8>> {
    refuse_vector_field(doc_id, &row, &cfg.vector_field)?;
    match row.get(key_column) {
        Some(value) => check_body_id(value, doc_id, key_column)?,
        None => {
            row.insert(key_column.to_string(), Value::String(doc_id.to_string()));
        }
    }
    zerompk::to_msgpack_vec(&row).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("vector row '{doc_id}': sidecar encode failed: {e}"),
    })
}

/// One document row a vector write lands in.
struct DocumentRow<'a> {
    doc_id: &'a str,
    vector_column: &'a str,
    /// The identity column a schemaless body names its id under. `None` for
    /// a strict body.
    identity: Option<&'a str>,
}

impl DocumentRow<'_> {
    /// The document body: `row` and the vector under the vector column, a
    /// native object. A body that supplies a generated column is refused.
    fn body(
        &self,
        ctx: &DispatchCtx<'_>,
        collection: &str,
        vector: Vec<f32>,
        mut row: HashMap<String, Value>,
    ) -> crate::Result<Vec<u8>> {
        let doc_id = self.doc_id;
        refuse_vector_field(doc_id, &row, self.vector_column)?;
        row.insert(
            self.vector_column.to_string(),
            Value::Array(
                vector
                    .into_iter()
                    .map(|f| Value::Float(f64::from(f)))
                    .collect(),
            ),
        );
        let body = zerompk::to_msgpack_vec(&Value::Object(row)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("vector row '{doc_id}': document encode failed: {e}"),
            }
        })?;
        super::generated_gate::refuse_generated_body(ctx, collection, &body)?;
        match self.identity {
            Some(column) => identified_body(&body, doc_id, column),
            None => Ok(body),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_body(fields: &[(&str, Value)]) -> Vec<u8> {
        let map = fields
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect();
        zerompk::to_msgpack_vec(&Value::Object(map)).expect("native body")
    }

    fn batch_vector(id: &str, metadata: Option<Value>) -> BatchVector {
        BatchVector {
            id: id.into(),
            embedding: vec![0.5],
            metadata,
        }
    }

    #[test]
    fn row_fields_keep_every_type() {
        let price = Value::Decimal("12.50".parse().expect("decimal literal"));
        let uid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let body = native_body(&[("price", price.clone()), ("uid", uid.clone())]);
        let row = row_fields("v1", &body).expect("decode");
        assert_eq!(row.get("price"), Some(&price));
        assert_eq!(row.get("uid"), Some(&uid));
    }

    #[test]
    fn a_plain_map_or_garbage_body_is_refused() {
        let plain =
            zerompk::to_msgpack_vec(&HashMap::from([("a".to_string(), 1_i64)])).expect("plain");
        assert!(matches!(
            row_fields("v1", &plain),
            Err(crate::Error::BadRequest { .. })
        ));
        assert!(matches!(
            row_fields("v1", &[0xc1]),
            Err(crate::Error::BadRequest { .. })
        ));
    }

    #[test]
    fn a_field_on_the_vector_column_is_refused() {
        let row = HashMap::from([("embedding".to_string(), Value::Integer(1))]);
        assert!(matches!(
            refuse_vector_field("v1", &row, "embedding"),
            Err(crate::Error::BadRequest { .. })
        ));
        assert!(refuse_vector_field("v1", &row, "body_embedding").is_ok());
    }

    #[test]
    fn a_sidecar_names_the_key_and_keeps_types() {
        let cfg = VectorPrimaryConfig {
            vector_field: "vec".into(),
            ..Default::default()
        };
        let price = Value::Decimal("12.50".parse().expect("decimal literal"));
        let row = HashMap::from([("price".to_string(), price.clone())]);
        let payload = sidecar_payload("v1", &cfg, "id", row).expect("sidecar");
        let decoded: HashMap<String, Value> = zerompk::from_msgpack(&payload).expect("decode");
        assert_eq!(decoded.get("price"), Some(&price));
        assert_eq!(decoded.get("id"), Some(&Value::String("v1".into())));
        let wrong = HashMap::from([("id".to_string(), Value::String("v2".into()))]);
        assert!(sidecar_payload("v1", &cfg, "id", wrong).is_err());
    }

    #[test]
    fn batch_metadata_must_be_an_object() {
        assert!(metadata_fields(&batch_vector("v1", Some(Value::Integer(1)))).is_err());
        assert!(metadata_fields(&batch_vector("v1", None)).is_ok());
    }

    #[test]
    fn a_batch_is_keyed_headless_or_refused() {
        let meta = Some(Value::Object(HashMap::new()));
        assert_eq!(
            batch_is_keyed(&[batch_vector("a", None), batch_vector("b", meta.clone())]).ok(),
            Some(true)
        );
        assert_eq!(
            batch_is_keyed(&[batch_vector("", None), batch_vector("", None)]).ok(),
            Some(false)
        );
        assert!(batch_is_keyed(&[batch_vector("a", None), batch_vector("", None)]).is_err());
        assert!(batch_is_keyed(&[batch_vector("", meta)]).is_err());
    }
}

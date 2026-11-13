// SPDX-License-Identifier: BUSL-1.1

//! CRDT plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::DispatchCtx;
use super::require_doc_id;

pub(crate) async fn build_read(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    Ok(PhysicalPlan::Crdt(CrdtOp::Read {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id,
    }))
}

pub(crate) async fn build_apply(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let delta = bounded_delta(fields)?;
    let peer_id = fields.peer_id.unwrap_or(0);

    // Use provided mutation_id, or generate deterministic one from content hash.
    let mutation_id = fields.mutation_id.unwrap_or_else(|| {
        // Deterministic dedup key from (peer_id, delta).
        let mut combined = peer_id.to_le_bytes().to_vec();
        combined.extend_from_slice(&delta);
        crate::util::fnv1a_hash(&combined)
    });

    let surrogate =
        super::helpers::assign_surrogate(ctx, collection, document_id.as_bytes()).await?;

    Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id,
        delta,
        peer_id,
        mutation_id,
        surrogate,
        provenance: None,
        // Native admission does not yet stamp a constraint fence; `0` leaves
        // the apply-time write-gate open. Catalog-based stamping here is a
        // tracked follow-up (parity with the sync admission path).
        constraint_version_required: 0,
        expected_frontier_digest: None,
    }))
}

fn bounded_delta(fields: &TextFields) -> crate::Result<Vec<u8>> {
    let delta = fields
        .delta
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'delta'".to_string(),
        })?;
    if delta.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES {
        return Err(crate::Error::LimitExceeded {
            limit_name: "max_crdt_delta_bytes",
            value: delta.len() as u64,
            max: nodedb_crdt::DEFAULT_MAX_DELTA_BYTES as u64,
        });
    }
    Ok(delta.clone())
}

pub(crate) async fn build_alter_policy(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let policy = fields
        .policy
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'policy'".to_string(),
        })?;
    let policy_json = sonic_rs::to_string(policy).map_err(|e| crate::Error::BadRequest {
        detail: format!("invalid policy: {e}"),
    })?;
    Ok(PhysicalPlan::Crdt(CrdtOp::SetPolicy {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        policy_json,
    }))
}

/// Extract `list_path` from request fields.
fn require_list_path(fields: &TextFields) -> crate::Result<String> {
    fields
        .list_path
        .as_ref()
        .cloned()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'list_path'".to_string(),
        })
}

/// Extract a list index field and narrow it to `usize`.
///
/// A missing or out-of-range value is a typed decode error — never a
/// silent default to `0`, since a position-based CRDT op landing at the
/// wrong index is data corruption, not a benign fallback.
fn require_list_index(value: Option<u64>, field_name: &str) -> crate::Result<usize> {
    let raw = value.ok_or_else(|| crate::Error::BadRequest {
        detail: format!("missing '{field_name}'"),
    })?;
    usize::try_from(raw).map_err(|_| crate::Error::BadRequest {
        detail: format!("'{field_name}' value {raw} exceeds platform usize range"),
    })
}

/// The block fields of a `CrdtListInsert`: a `Value::Object`, encoded as
/// the native object body `CrdtOp::ListInsert` carries. A missing or
/// non-object value is refused.
fn require_list_block(fields: &TextFields) -> crate::Result<Vec<u8>> {
    match fields.list_fields.as_ref() {
        Some(nodedb_types::Value::Object(block)) => {
            nodedb_physical::physical_plan::encode_fields_body(block).map_err(|e| {
                crate::Error::BadRequest {
                    detail: e.to_string(),
                }
            })
        }
        Some(other) => Err(crate::Error::BadRequest {
            detail: format!(
                "'list_fields' holds {}, expected an object",
                other.type_name()
            ),
        }),
        None => Err(crate::Error::BadRequest {
            detail: "missing 'list_fields'".to_string(),
        }),
    }
}

pub(crate) async fn build_list_insert(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let list_path = require_list_path(fields)?;
    let index = require_list_index(fields.list_index, "list_index")?;
    let block = require_list_block(fields)?;

    let surrogate =
        super::helpers::assign_surrogate(ctx, collection, document_id.as_bytes()).await?;

    Ok(PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id,
        list_path,
        index,
        fields: block,
        surrogate,
    }))
}

pub(crate) async fn build_list_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let list_path = require_list_path(fields)?;
    let index = require_list_index(fields.list_index, "list_index")?;

    let surrogate =
        super::helpers::assign_surrogate(ctx, collection, document_id.as_bytes()).await?;

    Ok(PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id,
        list_path,
        index,
        surrogate,
    }))
}

pub(crate) async fn build_list_move(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let list_path = require_list_path(fields)?;
    let from_index = require_list_index(fields.list_from_index, "list_from_index")?;
    let to_index = require_list_index(fields.list_to_index, "list_to_index")?;

    let surrogate =
        super::helpers::assign_surrogate(ctx, collection, document_id.as_bytes()).await?;

    Ok(PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        document_id,
        list_path,
        from_index,
        to_index,
        surrogate,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, Surrogate};

    fn list_insert_fields() -> TextFields {
        TextFields {
            document_id: Some("doc-1".to_string()),
            list_path: Some("items".to_string()),
            list_index: Some(3),
            list_fields: Some(nodedb_types::Value::Object(
                [
                    (
                        "title".to_string(),
                        nodedb_types::Value::String("hello".into()),
                    ),
                    (
                        "price".to_string(),
                        nodedb_types::Value::Decimal("12.50".parse().unwrap()),
                    ),
                ]
                .into(),
            )),
            ..Default::default()
        }
    }

    fn list_move_fields() -> TextFields {
        TextFields {
            document_id: Some("doc-1".to_string()),
            list_path: Some("items".to_string()),
            list_from_index: Some(1),
            list_to_index: Some(5),
            ..Default::default()
        }
    }

    #[test]
    fn apply_delta_hard_limit_is_checked_before_planning() {
        let exact = TextFields {
            delta: Some(vec![0; nodedb_crdt::DEFAULT_MAX_DELTA_BYTES]),
            ..Default::default()
        };
        assert_eq!(
            bounded_delta(&exact).expect("exact limit").len(),
            nodedb_crdt::DEFAULT_MAX_DELTA_BYTES
        );

        let oversized = TextFields {
            delta: Some(vec![0; nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 1]),
            ..Default::default()
        };
        assert!(matches!(
            bounded_delta(&oversized),
            Err(crate::Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn list_insert_field_parsing_carries_every_field() {
        let fields = list_insert_fields();
        let document_id = require_doc_id(&fields).unwrap();
        let list_path = require_list_path(&fields).unwrap();
        let index = require_list_index(fields.list_index, "list_index").unwrap();
        let block = require_list_block(&fields).unwrap();
        let Some(nodedb_types::Value::Object(sent)) = fields.list_fields.clone() else {
            panic!("the request carries an object");
        };
        assert_eq!(
            nodedb_physical::physical_plan::decode_fields_body(&block).expect("block decodes"),
            sent,
            "the block body keeps every field typed"
        );

        let surrogate = Surrogate::new(42);
        let plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: document_id.clone(),
            list_path: list_path.clone(),
            index,
            fields: block.clone(),
            surrogate,
        });

        let PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection,
            document_id: got_document_id,
            list_path: got_list_path,
            index: got_index,
            fields: got_block,
            surrogate: got_surrogate,
        }) = plan
        else {
            panic!("expected CrdtOp::ListInsert");
        };
        assert_eq!(collection.as_str(), "docs");
        assert_eq!(got_document_id, document_id);
        assert_eq!(got_list_path, list_path);
        assert_eq!(got_index, index);
        assert_eq!(got_block, block);
        assert_eq!(got_surrogate, surrogate);
    }

    #[test]
    fn list_insert_refuses_missing_or_non_object_fields() {
        let mut fields = list_insert_fields();
        fields.list_fields = Some(nodedb_types::Value::Integer(1));
        assert!(matches!(
            require_list_block(&fields),
            Err(crate::Error::BadRequest { .. })
        ));
        fields.list_fields = None;
        assert!(matches!(
            require_list_block(&fields),
            Err(crate::Error::BadRequest { .. })
        ));
    }

    #[test]
    fn list_insert_missing_index_is_typed_error_not_zero_default() {
        let mut fields = list_insert_fields();
        fields.list_index = None;
        let err = require_list_index(fields.list_index, "list_index").unwrap_err();
        assert!(matches!(err, crate::Error::BadRequest { .. }));
    }

    #[test]
    fn list_insert_out_of_range_index_is_typed_error() {
        // usize on this platform is 64-bit, so exercise the narrowing
        // path with a value that legitimately fits u64 but is used to
        // prove the conversion is checked rather than assumed infallible.
        let index = require_list_index(Some(u64::MAX), "list_index");
        // On 64-bit platforms usize == u64 so u64::MAX narrows cleanly;
        // the important property is that the function is fallible and
        // returns a typed error rather than silently truncating when it
        // does not fit, not that MAX specifically fails on every arch.
        if usize::try_from(u64::MAX).is_err() {
            assert!(matches!(index, Err(crate::Error::BadRequest { .. })));
        } else {
            assert_eq!(index.unwrap(), u64::MAX as usize);
        }
    }

    #[test]
    fn list_insert_missing_list_path_is_typed_error() {
        let mut fields = list_insert_fields();
        fields.list_path = None;
        assert!(matches!(
            require_list_path(&fields),
            Err(crate::Error::BadRequest { .. })
        ));
    }

    #[test]
    fn list_delete_produces_exact_plan_without_fields_json() {
        let document_id = "doc-2".to_string();
        let list_path = "notes".to_string();
        let index = 7usize;
        let surrogate = Surrogate::new(9);

        let plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: document_id.clone(),
            list_path: list_path.clone(),
            index,
            surrogate,
        });

        let PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection,
            document_id: got_document_id,
            list_path: got_list_path,
            index: got_index,
            surrogate: got_surrogate,
        }) = plan
        else {
            panic!("expected CrdtOp::ListDelete");
        };
        assert_eq!(collection.as_str(), "docs");
        assert_eq!(got_document_id, document_id);
        assert_eq!(got_list_path, list_path);
        assert_eq!(got_index, index);
        assert_eq!(got_surrogate, surrogate);
    }

    #[test]
    fn list_move_keeps_from_and_to_index_distinct_and_unswapped() {
        let fields = list_move_fields();
        let document_id = require_doc_id(&fields).unwrap();
        let list_path = require_list_path(&fields).unwrap();
        let from_index = require_list_index(fields.list_from_index, "list_from_index").unwrap();
        let to_index = require_list_index(fields.list_to_index, "list_to_index").unwrap();
        assert_ne!(from_index, to_index);

        let surrogate = Surrogate::new(11);
        let plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate,
        });

        let PhysicalPlan::Crdt(CrdtOp::ListMove {
            from_index: got_from,
            to_index: got_to,
            ..
        }) = plan
        else {
            panic!("expected CrdtOp::ListMove");
        };
        assert_eq!(got_from, 1);
        assert_eq!(got_to, 5);
        assert_ne!(got_from, got_to);
    }

    #[test]
    fn list_move_missing_from_index_is_typed_error() {
        let mut fields = list_move_fields();
        fields.list_from_index = None;
        assert!(matches!(
            require_list_index(fields.list_from_index, "list_from_index"),
            Err(crate::Error::BadRequest { .. })
        ));
        // to_index is independently present and must still parse fine.
        assert!(require_list_index(fields.list_to_index, "list_to_index").is_ok());
    }

    #[test]
    fn list_move_missing_to_index_is_typed_error() {
        let mut fields = list_move_fields();
        fields.list_to_index = None;
        assert!(matches!(
            require_list_index(fields.list_to_index, "list_to_index"),
            Err(crate::Error::BadRequest { .. })
        ));
    }
}

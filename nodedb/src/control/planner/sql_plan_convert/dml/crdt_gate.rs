// SPDX-License-Identifier: BUSL-1.1

//! CRDT-routing gate for document-collection DML.
//!
//! Detects `crdt = true` document collections and builds the `fields` body
//! that `CrdtOp::DocUpsert` consumes: a native object, so every value keeps
//! its exact type. Lives at the `dml` module level
//! so INSERT / UPSERT (in `insert.rs`) and UPDATE / DELETE (in
//! `update_delete/`) all reach the same detection + payload builders.
//!
//! Routing contract: on a CRDT document collection, PK-targeted
//! INSERT / UPSERT (full replace) / UPDATE-SET (literal RHS) / DELETE lower to
//! `CrdtOp::DocUpsert` / `DocDelete`; every unsupported shape (predicate
//! UPDATE/DELETE, non-literal UPDATE RHS, UPDATE ... RETURNING, `IF NOT EXISTS`
//! INSERT, explicit `ON CONFLICT DO UPDATE`) is rejected with a typed error.
//! There is NO silent fallthrough to a non-CRDT `DocumentOp` — that would
//! bypass CRDT convergence and is a correctness bug.

use nodedb_sql::types::{SqlExpr, SqlValue};

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::value::sql_value_to_nodedb_value;

/// `true` when `key` names a CRDT document collection.
///
/// The lookup reads the catalog row `key` names.
///
/// A genuine catalog READ error propagates: misrouting a write to the non-CRDT
/// path would silently bypass CRDT convergence. An ABSENT credential store or
/// catalog, or an absent collection row (`Ok(None)`), is treated as non-CRDT
/// (`Ok(false)`). Mirrors `document_collection_is_edge_bearing` in
/// `update_delete/shared.rs` exactly.
pub(in crate::control::planner::sql_plan_convert::dml) fn document_collection_is_crdt(
    ctx: &ConvertContext,
    key: nodedb_types::CollectionKey<'_>,
) -> crate::Result<bool> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(false);
    };
    let catalog = credentials.catalog();
    Ok(catalog
        .get_collection(key.database_id(), ctx.tenant_id.as_u64(), key.name())?
        .map(|c| c.crdt)
        .unwrap_or(false))
}

/// Full-row fields → the native object body `DocUpsert.fields` carries.
///
/// Each value converts to its typed `Value`, so a Decimal keeps its scale and
/// a UUID or an instant keeps its kind through the CRDT write.
pub(in crate::control::planner::sql_plan_convert::dml) fn row_to_fields_body(
    row: &[(String, SqlValue)],
) -> crate::Result<Vec<u8>> {
    let fields: std::collections::HashMap<String, nodedb_types::Value> = row
        .iter()
        .map(|(name, value)| (name.clone(), sql_value_to_nodedb_value(value)))
        .collect();
    nodedb_physical::physical_plan::encode_fields_body(&fields).map_err(|detail| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: detail.to_string(),
        }
    })
}

/// UPDATE SET (partial) `fields` body from LITERAL assignments only.
///
/// An expression RHS is rejected: the Control Plane planner cannot evaluate a
/// per-row expression here (same restriction the KV and columnar/spatial UPDATE
/// paths enforce). Only the provided fields go into the object — `DocUpsert`
/// with `partial = true` writes exactly these (LWW-per-field), leaving untouched
/// keys intact.
pub(in crate::control::planner::sql_plan_convert::dml) fn literal_assignments_to_fields_body(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<u8>> {
    let mut row: Vec<(String, SqlValue)> = Vec::with_capacity(assignments.len());
    for (field, expr) in assignments {
        match expr {
            SqlExpr::Literal(v) => row.push((field.clone(), v.clone())),
            _ => {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "UPDATE with non-literal RHS on CRDT collection (field '{field}') \
                         is not supported; use a literal value"
                    ),
                });
            }
        }
    }
    row_to_fields_body(&row)
}

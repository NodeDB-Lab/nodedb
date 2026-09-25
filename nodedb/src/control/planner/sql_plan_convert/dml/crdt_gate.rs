// SPDX-License-Identifier: BUSL-1.1

//! CRDT-routing gate for document-collection DML.
//!
//! Detects `crdt = true` document collections and builds the `fields_json`
//! payload that `CrdtOp::DocUpsert` consumes. Lives at the `dml` module level
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
use crate::control::planner::sql_plan_convert::value::row_to_msgpack;

/// `true` when `collection` (already db-qualified by the caller) is a CRDT
/// document collection.
///
/// A genuine catalog READ error propagates: misrouting a write to the non-CRDT
/// path would silently bypass CRDT convergence. An ABSENT credential store or
/// catalog, or an absent collection row (`Ok(None)`), is treated as non-CRDT
/// (`Ok(false)`). Mirrors `document_collection_is_edge_bearing` in
/// `update_delete/shared.rs` exactly.
pub(in crate::control::planner::sql_plan_convert::dml) fn document_collection_is_crdt(
    ctx: &ConvertContext,
    collection: &str,
) -> crate::Result<bool> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(false);
    };
    let catalog = credentials.catalog();
    // Callers route the plan on the database-qualified collection, but the
    // catalog keys collections by the bare name, so strip the qualifier back off
    // here or a CRDT collection in a non-default database reads as non-CRDT and
    // its predicate write silently bypasses CRDT convergence. Identity for
    // `DatabaseId::DEFAULT`.
    let bare = crate::control::target_identity::bare_collection_name(ctx.database_id, collection);
    Ok(catalog
        .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), &bare)?
        .map(|c| c.crdt)
        .unwrap_or(false))
}

/// Full-row fields → JSON object string for `DocUpsert.fields_json`.
///
/// Reuses the DML msgpack writer + `json_from_msgpack` so the stored payload is
/// byte-identical to what the non-CRDT `PointInsert` path would produce for the
/// same row, then serializes to JSON text (the shape `DocUpsert` consumes).
pub(in crate::control::planner::sql_plan_convert::dml) fn row_to_fields_json(
    row: &[(String, SqlValue)],
) -> crate::Result<String> {
    let mpk = row_to_msgpack(row)?;
    let json = nodedb_types::json_from_msgpack(&mpk).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("crdt fields_json encode: {e}"),
    })?;
    sonic_rs::to_string(&json).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("crdt fields_json encode: {e}"),
    })
}

/// UPDATE SET (partial) `fields_json` from LITERAL assignments only.
///
/// An expression RHS is rejected: the Control Plane planner cannot evaluate a
/// per-row expression here (same restriction the KV and columnar/spatial UPDATE
/// paths enforce). Only the provided fields go into the object — `DocUpsert`
/// with `partial = true` writes exactly these (LWW-per-field), leaving untouched
/// keys intact.
pub(in crate::control::planner::sql_plan_convert::dml) fn literal_assignments_to_fields_json(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<String> {
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
    row_to_fields_json(&row)
}

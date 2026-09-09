// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers used across per-engine plan builders.

use nodedb_types::DatabaseId;
use nodedb_types::protocol::TextFields;

use super::super::DispatchCtx;

/// Single catalog lookup returning the collection's storage type.
///
/// Returns `None` when: no catalog available, collection not found,
/// or catalog read error. Callers treat `None` as "default to document".
pub(in crate::control::server::native::dispatch) fn collection_type(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> Option<nodedb_types::CollectionType> {
    let catalog = ctx.state.credentials.catalog();
    let coll = catalog
        .get_collection(
            DatabaseId::DEFAULT,
            ctx.identity.tenant_id.as_u64(),
            collection,
        )
        .ok()??;
    Some(coll.collection_type.clone())
}

/// `collection`'s DDL-declared `PRIMARY KEY` column name, for the apply-time
/// NOT NULL guard on `PointUpdate` / `BulkUpdate`. `None` means no `PRIMARY
/// KEY` was declared, so the guard has nothing to enforce.
pub(in crate::control::server::native::dispatch) fn declared_primary_key(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<Option<String>> {
    ctx.state.credentials.catalog().declared_primary_key(
        ctx.database_id(),
        ctx.identity.tenant_id.as_u64(),
        collection,
    )
}

/// Extract document_id from request fields.
pub(in crate::control::server::native::dispatch) fn require_doc_id(
    fields: &TextFields,
) -> crate::Result<String> {
    fields
        .document_id
        .as_ref()
        .cloned()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'document_id'".to_string(),
        })
}

/// Parse direction string for graph operations.
pub(in crate::control::server::native::dispatch) fn parse_direction(
    s: Option<&str>,
) -> crate::engine::graph::edge_store::Direction {
    match s {
        Some("in") => crate::engine::graph::edge_store::Direction::In,
        Some("both") => crate::engine::graph::edge_store::Direction::Both,
        _ => crate::engine::graph::edge_store::Direction::Out,
    }
}

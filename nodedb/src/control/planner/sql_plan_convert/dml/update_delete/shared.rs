// SPDX-License-Identifier: BUSL-1.1

//! Helpers shared by [`super::update::convert_update`] and
//! [`super::delete::convert_delete`]: edge-bearing detection and
//! PK-effective-filter synthesis.

use nodedb_sql::types::{Filter, SqlValue};

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::sql_value_to_string;

/// Returns `true` when the schemaless-document `collection` (already
/// db-qualified by the caller) carries implicit edges, mirroring the
/// edge-bearing gate in `execute.rs`.
///
/// A genuine catalog READ error propagates (misrouting a delete on a real I/O
/// fault would silently skip edge cleanup → dangling edges). An ABSENT
/// credential store or catalog, or an absent collection row (`Ok(None)`), is
/// treated as non-edge-bearing (`Ok(false)`).
pub(super) fn document_collection_is_edge_bearing(
    ctx: &ConvertContext,
    collection: &str,
) -> crate::Result<bool> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(false);
    };
    let catalog = credentials.catalog();
    // Callers route the plan on the database-qualified collection, but the
    // catalog keys collections by the bare name, so strip the qualifier back off
    // here or an edge-bearing collection in a non-default database reads as
    // non-edge-bearing and its PK-equality UPDATE/DELETE skips the mirrored-edge
    // cleanup, leaking stale edges. Identity for `DatabaseId::DEFAULT`.
    let bare = crate::control::target_identity::bare_collection_name(ctx.database_id, collection);
    Ok(catalog
        .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), &bare)?
        .map(|c| c.has_implicit_edges)
        .unwrap_or(false))
}

/// Effective filter for a PK-pre-resolved write (shared by the columnar UPDATE
/// path and the edge-bearing PK-equality DELETE path).
///
/// Prefers the user's serialized `WHERE` predicate (`filter_bytes`) verbatim.
/// Only when it is empty AND the planner pre-resolved `target_keys` does it
/// synthesize one `id = <key>` `Eq` filter per key. When `target_keys` is also
/// empty (no WHERE at all) the empty `filter_bytes` is returned as-is (match
/// all) — so callers that must NEVER match all rows (the DELETE gate) must only
/// call this with a non-empty `target_keys`, which then guarantees a non-empty
/// result.
pub(super) fn pk_effective_filter(
    filter_bytes: Vec<u8>,
    target_keys: &[SqlValue],
) -> crate::Result<Vec<u8>> {
    if !filter_bytes.is_empty() || target_keys.is_empty() {
        return Ok(filter_bytes);
    }
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    let pk_filters: Vec<ScanFilter> = target_keys
        .iter()
        .map(|key| ScanFilter {
            field: "id".to_string(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::String(sql_value_to_string(key)),
            clauses: Vec::new(),
            expr: None,
        })
        .collect();
    zerompk::to_msgpack_vec(&pk_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("pk filter encode: {e}"),
    })
}

/// Build the filter bytes for an edge-bearing PK-equality DELETE routed as a
/// `BulkDelete`. Thin wrapper over [`pk_effective_filter`]: serializes the
/// user's `WHERE` predicate, then defers to the shared synthesis. The DELETE
/// gate only calls this with a non-empty `target_keys`, so the result is NEVER
/// an empty filter (which would match ALL rows).
pub(super) fn delete_effective_filter(
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> crate::Result<Vec<u8>> {
    pk_effective_filter(serialize_filters(filters)?, target_keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_effective_filter_never_empty_for_non_empty_keys() {
        let keys = vec![
            SqlValue::String("a".to_string()),
            SqlValue::String("b".to_string()),
        ];
        let bytes = delete_effective_filter(&[], &keys).expect("synthesize filter");
        assert!(!bytes.is_empty());
    }
}

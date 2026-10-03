// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers used across per-engine plan builders.

use nodedb_types::protocol::TextFields;

use super::super::DispatchCtx;

/// Single catalog lookup returning the collection's storage type.
///
/// `Ok(None)` means the catalog holds no such collection; callers treat that
/// as "default to document". A catalog read error propagates.
pub(in crate::control::server::native::dispatch) fn collection_type(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<Option<nodedb_types::CollectionType>> {
    let catalog = ctx.state.credentials.catalog();
    Ok(catalog
        .get_collection(
            ctx.database_id(),
            ctx.identity.tenant_id.as_u64(),
            collection,
        )?
        .map(|coll| coll.collection_type))
}

/// Whether an edge was ever written into `collection`. An absent collection
/// row is not edge-bearing.
pub(in crate::control::server::native::dispatch) fn collection_is_edge_bearing(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<bool> {
    let catalog = ctx.state.credentials.catalog();
    Ok(catalog
        .get_collection(
            ctx.database_id(),
            ctx.identity.tenant_id.as_u64(),
            collection,
        )?
        .is_some_and(|coll| coll.has_implicit_edges))
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

/// The `direction` field of a graph request. An absent field is `Out`. A
/// value that names no direction is a `BadRequest` naming it.
pub(in crate::control::server::native::dispatch) fn parse_direction(
    s: Option<&str>,
) -> crate::Result<crate::engine::graph::edge_store::Direction> {
    match s {
        None => Ok(crate::engine::graph::edge_store::Direction::Out),
        Some(text) => text
            .parse()
            .map_err(
                |e: nodedb_types::graph::ParseDirectionError| crate::Error::BadRequest {
                    detail: format!("graph request 'direction': {e}"),
                },
            ),
    }
}

/// The surrogates of `pks` in `collection`, bound at the collection's home
/// when a key has none, in `pks` order. The async routed exchange answers
/// them in one batch, and a binding this node's catalog holds answers without
/// a request.
pub(super) async fn assign_surrogates(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    pks: &[&[u8]],
) -> crate::Result<Vec<nodedb_types::Surrogate>> {
    crate::control::server::surrogate_exchange::assign_surrogates_routed(
        ctx.state,
        nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
        ctx.tenant_id(),
        pks,
        crate::types::TraceId::ZERO,
    )
    .await
}

/// [`assign_surrogates`] for one key.
pub(super) async fn assign_surrogate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    pk: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    crate::control::server::surrogate_exchange::assign_surrogate_routed(
        ctx.state,
        nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
        ctx.tenant_id(),
        pk,
        crate::types::TraceId::ZERO,
    )
    .await
}

/// The surrogate `pk` is bound to in `collection`, or `None` when the key
/// names no row: the home's binding, through the async routed exchange.
/// Never binds.
pub(super) async fn existing_surrogate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    pk: &[u8],
) -> crate::Result<Option<nodedb_types::Surrogate>> {
    crate::control::server::surrogate_exchange::lookup_surrogate_routed(
        ctx.state,
        nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
        ctx.tenant_id(),
        pk,
        crate::types::TraceId::ZERO,
    )
    .await
}

/// [`existing_surrogate`] for many keys, in `pks` order. Never binds.
pub(super) async fn existing_surrogates(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    pks: &[&[u8]],
) -> crate::Result<Vec<Option<nodedb_types::Surrogate>>> {
    crate::control::server::surrogate_exchange::lookup_surrogates_routed(
        ctx.state,
        nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
        ctx.tenant_id(),
        pks,
        crate::types::TraceId::ZERO,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::parse_direction;
    use crate::engine::graph::edge_store::Direction;

    #[test]
    fn an_absent_direction_is_out() {
        assert_eq!(parse_direction(None).unwrap(), Direction::Out);
    }

    #[test]
    fn a_known_direction_parses_in_any_case() {
        assert_eq!(parse_direction(Some("IN")).unwrap(), Direction::In);
        assert_eq!(parse_direction(Some("both")).unwrap(), Direction::Both);
    }

    #[test]
    fn an_unknown_direction_is_a_bad_request_naming_it() {
        match parse_direction(Some("sideways")) {
            Err(crate::Error::BadRequest { detail }) => {
                assert!(detail.contains("'sideways'"), "{detail}");
            }
            other => panic!("expected a bad request, got {other:?}"),
        }
    }
}

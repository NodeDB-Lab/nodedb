// SPDX-License-Identifier: BUSL-1.1

//! Catalog data source: dispatches a relation name to its per-table row
//! producer and returns msgpack-encoded rows. This is the single entry the
//! per-request materializer calls; schema comes from
//! [`super::schema::catalog_collection_info`].

use nodedb_sql::types::CollectionInfo;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::schema::{catalog_collection_info, known_table};
use super::{audit_log, dropped_collections, l2_cleanup_queue, tables};

/// Produce the rows of a catalog relation, identity-scoped, per request.
///
/// Each returned `Vec<u8>` is one msgpack-encoded row object: a map of
/// column-name → value, with the columns named and ordered as in
/// [`catalog_collection_info`]. There is no synthesized `id` column — catalog
/// relations carry only their declared schema columns, matching the columnar
/// scan row convention.
///
/// Returns `Ok(vec![])` for any name that is not a catalog relation.
pub async fn catalog_rows(
    name: &str,
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    // Normalize to the canonical static name so case-insensitive references
    // (`PG_CLASS`) dispatch correctly; unknown names produce no rows.
    match known_table(name) {
        Some("pg_database") => tables::pg_database(),
        Some("pg_namespace") => tables::pg_namespace(),
        Some("pg_type") => tables::pg_type(),
        Some("pg_class") => tables::pg_class(state, identity),
        Some("pg_attribute") => tables::pg_attribute(state, identity),
        Some("pg_attrdef") => tables::pg_attrdef(state, identity),
        Some("pg_collation") => tables::pg_collation(),
        Some("pg_index") => tables::pg_index(state, identity),
        Some("pg_range") => tables::pg_range(),
        Some("pg_authid") => tables::pg_authid(state, identity),
        Some("_system.audit_log") => audit_log::audit_log(state, identity),
        Some("_system.dropped_collections") => {
            dropped_collections::dropped_collections(state, identity).await
        }
        Some("_system.l2_cleanup_queue") => l2_cleanup_queue::l2_cleanup_queue(state, identity),
        _ => Ok(Vec::new()),
    }
}

/// Thin handle over the catalog data source. The two free functions
/// ([`catalog_collection_info`], [`catalog_rows`]) are the contract; this
/// struct is a convenience for callers that prefer a value to thread.
pub struct CatalogProvider;

impl CatalogProvider {
    /// Identity-independent schema for a known catalog relation.
    pub fn schema(&self, name: &str) -> Option<CollectionInfo> {
        catalog_collection_info(name)
    }

    /// Identity-scoped rows for a known catalog relation.
    pub async fn rows(
        &self,
        name: &str,
        state: &SharedState,
        identity: &AuthenticatedIdentity,
    ) -> crate::Result<Vec<Vec<u8>>> {
        catalog_rows(name, state, identity).await
    }
}

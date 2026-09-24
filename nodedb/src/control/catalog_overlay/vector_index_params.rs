// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for vector index build parameters.
//!
//! See [`super::collection`] for the mechanism this mirrors: a `CREATE
//! VECTOR INDEX` buffered inside an open transaction must be visible to a
//! later `ALTER VECTOR INDEX` or duplicate check in that same transaction,
//! before COMMIT writes the row.

use nodedb_types::StoredVectorIndexParams;

use crate::control::catalog_entry::CatalogEntry;

/// The vector index one `(database, tenant, collection, field)` names.
struct Target<'a> {
    database_id: u64,
    tenant_id: u64,
    collection: &'a str,
    field_name: &'a str,
}

/// True when `entry` mutates the vector index `target` names.
fn targets(entry: &CatalogEntry, target: &Target<'_>) -> bool {
    match entry {
        CatalogEntry::PutVectorIndexParams(stored) => {
            stored.database_id == target.database_id
                && stored.tenant_id == target.tenant_id
                && stored.collection == target.collection
                && stored.field_name == target.field_name
        }
        CatalogEntry::DeleteVectorIndexParams {
            database_id,
            tenant_id,
            collection,
            field_name,
        } => {
            *database_id == target.database_id
                && *tenant_id == target.tenant_id
                && collection == target.collection
                && field_name == target.field_name
        }
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(
    current: Option<StoredVectorIndexParams>,
    entry: &CatalogEntry,
) -> Option<StoredVectorIndexParams> {
    match entry {
        CatalogEntry::PutVectorIndexParams(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteVectorIndexParams { .. } => None,
        _ => current,
    }
}

/// Resolve one vector index's build parameters through this connection's
/// uncommitted DDL.
pub fn resolve_vector_index_params(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
    committed: Option<StoredVectorIndexParams>,
) -> Option<StoredVectorIndexParams> {
    let target = Target {
        database_id,
        tenant_id,
        collection,
        field_name,
    };
    super::core::resolve(committed, |entry| targets(entry, &target), step)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn params(m: usize) -> StoredVectorIndexParams {
        StoredVectorIndexParams {
            database_id: 0,
            tenant_id: 1,
            collection: "docs".to_owned(),
            field_name: "emb".to_owned(),
            dim: 3,
            metric: "cosine".to_owned(),
            m,
            ef_construction: 200,
            index_type: "hnsw".to_owned(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }
    }

    fn resolve(committed: Option<StoredVectorIndexParams>) -> Option<StoredVectorIndexParams> {
        resolve_vector_index_params(0, 1, "docs", "emb", committed)
    }

    #[tokio::test]
    async fn a_buffered_create_then_alter_resolves_to_the_altered_row() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(CatalogEntry::PutVectorIndexParams(Box::new(params(16))));
            ddl_buffer::try_buffer(CatalogEntry::PutVectorIndexParams(Box::new(params(32))));
            assert_eq!(resolve(None).map(|p| p.m), Some(32));
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_drop_hides_the_committed_row() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(CatalogEntry::DeleteVectorIndexParams {
                database_id: 0,
                tenant_id: 1,
                collection: "docs".to_owned(),
                field_name: "emb".to_owned(),
            });
            assert!(resolve(Some(params(16))).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn outside_a_transaction_the_committed_row_wins() {
        conn_scope::scoped(async {
            assert_eq!(resolve(Some(params(16))).map(|p| p.m), Some(16));
        })
        .await;
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-version fence for an interactive transaction's buffered writes.
//!
//! A buffered write is planned at STATEMENT time and dispatched at COMMIT —
//! an arbitrarily later point, bounded only by how long the client holds the
//! block open. The statement's `QueryLeaseScope` is retained for the whole
//! block, but a lease grant never compares the requested version against the
//! catalog: a lease taken after a DDL already committed is still granted at
//! the superseded version. Re-comparing the retained holds against the
//! catalog immediately before the durable dispatch is what closes that
//! window.

use crate::control::gateway::version_check::check_descriptor_holds;
use crate::control::security::catalog::SystemCatalog;

use super::connection::SessionId;
use super::outcome::AbortReason;
use super::store::SessionStore;

/// Re-compare every descriptor version this transaction's buffered writes were
/// planned against with the catalog as it stands now.
///
/// Returns `Some(AbortReason::SchemaChanged)` when the catalog has moved on,
/// or when this node lost a lease the buffered statements hold. Either aborts
/// the COMMIT before any durable write.
pub(super) fn check_buffered_descriptors(
    catalog: &SystemCatalog,
    sessions: &SessionStore,
    session_id: SessionId,
) -> Option<AbortReason> {
    // A lease this node lost while the block was open: other nodes can have
    // committed a DDL this node's catalog has not applied yet.
    if let Some(revoked) = sessions.tx_lease_revoked(session_id) {
        return Some(AbortReason::SchemaChanged {
            detail: revoked.to_string(),
        });
    }
    let holds = sessions.tx_descriptor_versions(session_id);
    check_descriptor_holds(catalog, &holds)
        .err()
        .map(|error| AbortReason::SchemaChanged {
            detail: error.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::{DescriptorId, DescriptorKind};

    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::types::DatabaseId;

    const TENANT: u64 = 11;

    fn catalog_with(collections: &[(&str, u64)]) -> SystemCatalog {
        let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
        for (name, version) in collections {
            let mut stored = StoredCollection::stamped_for_test(TENANT, name, "owner");
            stored.descriptor_version = *version;
            catalog
                .put_collection(DatabaseId::DEFAULT, &stored)
                .expect("store collection");
        }
        catalog
    }

    fn hold(kind: DescriptorKind, name: &str, version: u64) -> (DescriptorId, u64) {
        (
            DescriptorId::new(DatabaseId::DEFAULT.as_u64(), TENANT, kind, name),
            version,
        )
    }

    fn check(catalog: &SystemCatalog, holds: &[(DescriptorId, u64)]) -> Option<AbortReason> {
        check_descriptor_holds(catalog, holds)
            .err()
            .map(|error| AbortReason::SchemaChanged {
                detail: error.to_string(),
            })
    }

    #[test]
    fn unchanged_versions_pass() {
        let catalog = catalog_with(&[("orders", 3)]);
        assert!(check(&catalog, &[hold(DescriptorKind::Collection, "orders", 3)]).is_none());
    }

    #[test]
    fn a_version_bump_since_the_statement_aborts_the_commit() {
        let catalog = catalog_with(&[("orders", 4)]);
        match check(&catalog, &[hold(DescriptorKind::Collection, "orders", 3)]) {
            Some(AbortReason::SchemaChanged { detail }) => {
                assert!(
                    detail.contains("orders"),
                    "detail names the collection: {detail}"
                );
            }
            _ => panic!("expected SchemaChanged"),
        }
    }

    #[test]
    fn non_collection_holds_are_skipped() {
        let catalog = catalog_with(&[]);
        assert!(check(&catalog, &[hold(DescriptorKind::Index, "orders_by_id", 9)]).is_none());
    }

    #[test]
    fn a_hold_in_another_tenant_is_compared_against_that_tenant() {
        let catalog = catalog_with(&[("orders", 3)]);
        let other = (
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT + 1,
                DescriptorKind::Collection,
                "orders",
            ),
            3,
        );
        // The same name at the same version in a tenant that has no such
        // collection must not be satisfied by this tenant's row.
        assert!(matches!(
            check(&catalog, &[other]),
            Some(AbortReason::SchemaChanged { .. })
        ));
    }

    /// A KV write staged under `n INT` holds the collection at the version
    /// it was planned against. A declared-column change commits as the next
    /// version, so the COMMIT aborts as retryable and never installs a value
    /// staging fitted under the old columns.
    #[test]
    fn a_kv_declared_column_change_after_staging_aborts_the_commit() {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

        let schema = |n: ColumnType| {
            StrictSchema::new(vec![
                ColumnDef::required("key", ColumnType::String).with_primary_key(),
                ColumnDef::nullable("n", n),
            ])
            .expect("schema")
        };
        let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
        let mut kv = StoredCollection::stamped_for_test(TENANT, "kv", "owner");
        kv.collection_type = nodedb_types::CollectionType::kv(schema(ColumnType::Int64));
        kv.descriptor_version = 3;
        catalog
            .put_collection(DatabaseId::DEFAULT, &kv)
            .expect("store collection");
        let staged = [hold(DescriptorKind::Collection, "kv", 3)];
        assert!(
            check(&catalog, &staged).is_none(),
            "unchanged columns commit"
        );

        kv.collection_type = nodedb_types::CollectionType::kv(schema(ColumnType::Float64));
        kv.descriptor_version = 4;
        catalog
            .put_collection(DatabaseId::DEFAULT, &kv)
            .expect("store collection");
        assert!(matches!(
            check(&catalog, &staged),
            Some(AbortReason::SchemaChanged { .. })
        ));
    }

    #[test]
    fn a_session_with_no_buffered_writes_has_nothing_to_fence() {
        let catalog = catalog_with(&[("orders", 3)]);
        let sessions = SessionStore::new();
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 4001));
        sessions.ensure_session(addr);
        assert!(check_buffered_descriptors(&catalog, &sessions, SessionId::from(addr)).is_none());
    }
}

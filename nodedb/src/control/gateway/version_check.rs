// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-version fence shared by local and cross-node dispatch.
//!
//! A plan is stamped at plan time with the descriptor versions it was built
//! against (`GatewayVersionSet`). Holding a descriptor lease does not make
//! that stamp current: a lease grant never compares the requested version
//! against the catalog, so a plan stamped shortly before a DDL committed still
//! acquires its lease afterwards, at the superseded version. Every dispatch
//! path therefore
//! re-compares the stamped versions against the executing node's own catalog
//! before the plan runs.

use std::collections::BTreeMap;

use nodedb_cluster::{DescriptorId, DescriptorKind};

use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// Why a stamped descriptor version set was refused.
#[derive(Debug, thiserror::Error)]
pub enum DescriptorCheckError {
    /// The catalog holds a different version than the plan was built against.
    #[error(
        "descriptor version mismatch on {collection}: plan expected {expected_version}, catalog holds {actual_version}"
    )]
    VersionMismatch {
        collection: String,
        expected_version: u64,
        actual_version: u64,
    },

    /// The catalog read itself failed, so the versions were not compared.
    #[error("catalog lookup failed for {collection}: {detail}")]
    CatalogLookup { collection: String, detail: String },
}

impl From<DescriptorCheckError> for crate::Error {
    fn from(e: DescriptorCheckError) -> Self {
        match e {
            DescriptorCheckError::VersionMismatch { collection, .. } => {
                crate::Error::RetryableSchemaChanged {
                    descriptor: collection,
                }
            }
            DescriptorCheckError::CatalogLookup { collection, detail } => crate::Error::Internal {
                detail: format!("descriptor check for {collection}: {detail}"),
            },
        }
    }
}

/// Compare a plan's stamped `(collection, version)` entries against the local
/// catalog.
///
/// `entries` carries one pair per collection the plan touches — the payload of
/// `GatewayVersionSet` on the local path and of `ExecuteRequest`'s
/// `descriptor_versions` on the remote one.
pub fn check_descriptor_versions<'a, I>(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    entries: I,
) -> Result<(), DescriptorCheckError>
where
    I: IntoIterator<Item = (&'a str, u64)>,
{
    for (collection, expected_version) in entries {
        // A `GatewayVersionSet` also carries synthetic, non-collection
        // entries (permission-tree / RLS tenant versions) so the plan cache
        // can fence on them too — see `version_set::with_extra`. They are
        // never real catalog descriptors, so there is nothing to re-compare
        // here: the gateway plan cache's own key equality already fenced
        // the plan against them at lookup time.
        if collection.starts_with('\0') {
            continue;
        }
        let looked_up = super::version_set::catalog_name(database_id, collection)
            .and_then(|(owner, name)| catalog.get_collection(owner, tenant_id, name));
        match looked_up {
            Ok(Some(stored)) => {
                // A collection created before the metadata applier stamped it
                // still carries 0 in the catalog; planning floors that to 1, so
                // the comparison floors it the same way.
                let actual_version = stored.descriptor_version.max(1);
                if actual_version != expected_version {
                    tracing::debug!(
                        %collection,
                        expected_version,
                        actual_version,
                        "descriptor version mismatch against local catalog"
                    );
                    return Err(DescriptorCheckError::VersionMismatch {
                        collection: collection.to_string(),
                        expected_version,
                        actual_version,
                    });
                }
            }
            Ok(None) => {
                // Version 0 means the plan bound no version for this
                // collection, so there is nothing to fence — an absent
                // collection only conflicts with a bound version.
                if expected_version != 0 {
                    return Err(DescriptorCheckError::VersionMismatch {
                        collection: collection.to_string(),
                        expected_version,
                        actual_version: 0,
                    });
                }
            }
            Err(e) => {
                return Err(DescriptorCheckError::CatalogLookup {
                    collection: collection.to_string(),
                    detail: e.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Compare a set of held `(descriptor, version)` pairs against the local
/// catalog.
///
/// The pairs come from the descriptor leases a plan took, which name their own
/// database and tenant, so they are grouped by that scope rather than compared
/// against a single assumed one. Only collection descriptors carry a catalog
/// version; other kinds are not collections and are skipped.
pub fn check_descriptor_holds(
    catalog: &SystemCatalog,
    holds: &[(DescriptorId, u64)],
) -> Result<(), DescriptorCheckError> {
    let mut by_scope: BTreeMap<(u64, u64), Vec<(&str, u64)>> = BTreeMap::new();
    for (id, version) in holds {
        if id.kind != DescriptorKind::Collection {
            continue;
        }
        by_scope
            .entry((id.database_id, id.tenant_id))
            .or_default()
            .push((id.name.as_str(), *version));
    }

    for ((database_id, tenant_id), entries) in by_scope {
        check_descriptor_versions(catalog, DatabaseId::new(database_id), tenant_id, entries)?;
    }
    Ok(())
}

/// Re-compare a plan's stamped descriptor versions against this node's own
/// catalog before a local dispatch. A mismatch surfaces as
/// [`crate::Error::RetryableSchemaChanged`], which the gateway's cache-miss
/// retry absorbs by re-planning against fresh state.
pub(super) fn check_local_descriptor_versions(
    shared: &crate::control::state::SharedState,
    tenant_id: crate::types::TenantId,
    database_id: DatabaseId,
    version_set: &super::version_set::GatewayVersionSet,
) -> Result<(), crate::Error> {
    check_descriptor_versions(
        shared.credentials.catalog(),
        database_id,
        tenant_id.as_u64(),
        version_set
            .iter()
            .map(|(collection, version)| (collection.as_str(), *version)),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;

    const TENANT: u64 = 7;

    /// A plan names a collection of a non-default database by its qualified
    /// name. The check still finds the row, so a stale plan draws a mismatch.
    #[test]
    fn a_qualified_name_is_fenced_against_its_catalog_row() {
        let database = DatabaseId::new(9);
        let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
        let mut stored = StoredCollection::stamped_for_test(TENANT, "orders", "owner");
        stored.descriptor_version = 4;
        catalog
            .put_collection(database, &stored)
            .expect("store collection");

        let check_at = |version: u64| {
            check_descriptor_versions(&catalog, database, TENANT, [("9/orders", version)])
        };
        assert!(check_at(4).is_ok());
        assert!(matches!(
            check_at(3),
            Err(DescriptorCheckError::VersionMismatch {
                actual_version: 4,
                ..
            })
        ));
    }

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

    fn check(catalog: &SystemCatalog, entries: &[(&str, u64)]) -> Result<(), DescriptorCheckError> {
        check_descriptor_versions(
            catalog,
            DatabaseId::DEFAULT,
            TENANT,
            entries.iter().map(|(name, version)| (*name, *version)),
        )
    }

    #[test]
    fn matching_version_passes() {
        let catalog = catalog_with(&[("orders", 4)]);
        assert!(check(&catalog, &[("orders", 4)]).is_ok());
    }

    /// A pseudo entry (permission-tree / RLS tenant version, folded into
    /// `GatewayVersionSet` by `with_extra`) names no real catalog descriptor,
    /// so it is never compared here — regardless of its value.
    #[test]
    fn pseudo_entry_is_skipped_regardless_of_value() {
        let catalog = catalog_with(&[("orders", 4)]);
        assert!(check(&catalog, &[("orders", 4), ("\0__rls_version::7", 999)]).is_ok());
    }

    #[test]
    fn stale_version_is_refused_with_both_versions() {
        let catalog = catalog_with(&[("orders", 5)]);
        match check(&catalog, &[("orders", 4)]) {
            Err(DescriptorCheckError::VersionMismatch {
                collection,
                expected_version,
                actual_version,
            }) => {
                assert_eq!(collection, "orders");
                assert_eq!(expected_version, 4);
                assert_eq!(actual_version, 5);
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn unstamped_catalog_version_compares_as_one() {
        let catalog = catalog_with(&[("orders", 0)]);
        assert!(check(&catalog, &[("orders", 1)]).is_ok());
        assert!(matches!(
            check(&catalog, &[("orders", 0)]),
            Err(DescriptorCheckError::VersionMismatch {
                actual_version: 1,
                ..
            })
        ));
    }

    #[test]
    fn absent_collection_at_version_zero_passes() {
        let catalog = catalog_with(&[]);
        assert!(check(&catalog, &[("orders", 0)]).is_ok());
    }

    #[test]
    fn absent_collection_at_bound_version_is_refused() {
        let catalog = catalog_with(&[]);
        assert!(matches!(
            check(&catalog, &[("orders", 3)]),
            Err(DescriptorCheckError::VersionMismatch {
                actual_version: 0,
                ..
            })
        ));
    }

    #[test]
    fn every_entry_is_compared() {
        let catalog = catalog_with(&[("orders", 2), ("users", 9)]);
        assert!(check(&catalog, &[("orders", 2), ("users", 9)]).is_ok());
        assert!(matches!(
            check(&catalog, &[("orders", 2), ("users", 8)]),
            Err(DescriptorCheckError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn mismatch_maps_to_retryable_schema_changed() {
        let err = crate::Error::from(DescriptorCheckError::VersionMismatch {
            collection: "orders".into(),
            expected_version: 1,
            actual_version: 2,
        });
        match err {
            crate::Error::RetryableSchemaChanged { descriptor } => {
                assert_eq!(descriptor, "orders");
            }
            other => panic!("expected RetryableSchemaChanged, got {other:?}"),
        }
    }

    #[test]
    fn lookup_failure_maps_to_internal() {
        let err = crate::Error::from(DescriptorCheckError::CatalogLookup {
            collection: "orders".into(),
            detail: "redb closed".into(),
        });
        assert!(matches!(err, crate::Error::Internal { .. }));
    }
}

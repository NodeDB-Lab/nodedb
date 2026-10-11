// SPDX-License-Identifier: BUSL-1.1

//! Boot seeding of the metadata group's durable host state.
//!
//! The applier persists descriptor leases, drains, the cluster version, the
//! DDL preparation owner, and pending DDL records as it applies them. Boot
//! loads those rows into the in-memory state before any entry applies.
//! State that no row holds starts fresh:
//! - `last_applied_hlc` is the highest expiry over the seeded leases and drains.
//! - `metadata_ddl.applied_token` is 0.
//! - `topology_log`, `routing_log`, and `catalog_entries_applied` are empty.
//!
//! A read error fails boot: running with a partial view of leases or drains
//! admits lease acquires that the cluster has fenced.

use std::sync::RwLock;

use nodedb_cluster::MetadataCache;

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// Replace the drains, pending DDL records, and DDL preparation owner in
/// `shared` with the persisted rows. Runs in every deployment mode: a single
/// node writes drain rows too.
pub fn seed_host_tables(shared: &SharedState) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let drains = catalog.load_descriptor_drains()?;
    let pending = catalog.load_pending_ddl()?;
    let owner = catalog.load_ddl_owner()?;
    shared.lease_drain.clear();
    shared.pending_ddl.clear();
    for drain in drains {
        shared.lease_drain.install_start(
            drain.descriptor_id,
            drain.owner,
            drain.up_to_version,
            drain.expires_at,
            drain.proposer_node_id,
        );
    }
    for record in pending {
        shared
            .pending_ddl
            .insert(record.token, record.objects, record.proposed_at);
    }
    *shared
        .metadata_ddl
        .owner
        .lock()
        .unwrap_or_else(|p| p.into_inner()) =
        owner.map(
            |(token, node_id)| crate::control::metadata_proposer::DdlPrepareOwner {
                token,
                node_id,
                acquired_at: std::time::Instant::now(),
            },
        );
    Ok(())
}

/// Replace the leases and cluster version in the metadata group's `cache`
/// with the persisted rows, and raise `last_applied_hlc` to the highest
/// persisted lease or drain expiry.
pub fn seed_metadata_cache(
    cache: &RwLock<MetadataCache>,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let leases = catalog.load_descriptor_leases()?;
    let drains = catalog.load_descriptor_drains()?;
    let cluster_version = catalog.load_cluster_version()?;

    let mut cache = cache.write().unwrap_or_else(|p| p.into_inner());
    cache.leases.clear();
    let mut hlc_floor = cache.last_applied_hlc;
    for lease in leases {
        if lease.expires_at > hlc_floor {
            hlc_floor = lease.expires_at;
        }
        cache
            .leases
            .insert((lease.descriptor_id.clone(), lease.node_id), lease);
    }
    for drain in &drains {
        if drain.expires_at > hlc_floor {
            hlc_floor = drain.expires_at;
        }
    }
    cache.last_applied_hlc = hlc_floor;
    cache.cluster_version = cluster_version.unwrap_or(0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::{
        DescriptorId, DescriptorKind, DescriptorLease, DrainOwner, MetadataApplier, MetadataEntry,
        PendingDdlObject, encode_entry,
    };
    use nodedb_types::Hlc;

    use crate::control::catalog_entry;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::lease::DrainEntry;
    use crate::control::security::catalog::StoredCollection;

    use super::super::MetadataCommitApplier;
    use super::super::test_fixture::applier_with_shared_at;
    use super::{seed_host_tables, seed_metadata_cache};

    async fn apply(applier: &MetadataCommitApplier, index: u64, entry: &MetadataEntry) {
        assert_eq!(
            applier
                .apply(&[(index, encode_entry(entry).unwrap())])
                .await,
            index,
            "entry {index} must apply"
        );
    }

    fn orders() -> DescriptorId {
        DescriptorId::new(0, 7, DescriptorKind::Collection, "orders")
    }

    fn pending_object() -> PendingDdlObject {
        let stored = StoredCollection::new(7, "pending_orders", "tester");
        PendingDdlObject::Create {
            entry: Box::new(MetadataEntry::CatalogDdl {
                payload: catalog_entry::encode(&CatalogEntry::PutCollection(Box::new(stored)))
                    .unwrap(),
            }),
        }
    }

    /// Every durable host table round-trips: apply in one session, reopen the
    /// catalog, seed a fresh session, and read the same state back.
    #[tokio::test(flavor = "multi_thread")]
    async fn applied_host_state_is_seeded_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let lease = DescriptorLease {
            descriptor_id: orders(),
            version: 3,
            node_id: 2,
            expires_at: Hlc::new(70, 0),
        };
        let moving = DrainOwner::MoveTenant {
            tenant_id: 7,
            source_db_id: 0,
        };
        // Built once: `StoredCollection::new` stamps `created_at` from the
        // clock, so two builds differ when they straddle a second.
        let pending = pending_object();
        {
            let (applier, _state) = applier_with_shared_at(dir.path(), "first.wal");
            apply(
                &applier,
                1,
                &MetadataEntry::DescriptorLeaseGrant(lease.clone()),
            )
            .await;
            apply(
                &applier,
                2,
                &MetadataEntry::ClusterVersionBump { from: 0, to: 3 },
            )
            .await;
            apply(
                &applier,
                3,
                &MetadataEntry::DdlPrepareAcquire {
                    token: 42,
                    node_id: 2,
                },
            )
            .await;
            // A pending propose reserves only under the lease owner's token.
            apply(
                &applier,
                4,
                &MetadataEntry::DdlPendingPropose {
                    token: 42,
                    objects: vec![pending.clone()],
                    proposed_at: Hlc::new(60, 0),
                },
            )
            .await;
            for (index, owner, up_to) in [(5, DrainOwner::Ddl, 4), (6, moving.clone(), 6)] {
                apply(
                    &applier,
                    index,
                    &MetadataEntry::DescriptorDrainStart {
                        descriptor_id: orders(),
                        up_to_version: up_to,
                        expires_at: Hlc::new(90, 0),
                        proposer_node_id: 2,
                        owner,
                    },
                )
                .await;
            }
            apply(
                &applier,
                7,
                &MetadataEntry::DescriptorDrainEnd {
                    descriptor_id: orders(),
                    owner: moving,
                },
            )
            .await;
        }

        let (applier, state) = applier_with_shared_at(dir.path(), "second.wal");
        seed_host_tables(&state).unwrap();
        seed_metadata_cache(&state.metadata_cache, state.credentials.catalog()).unwrap();
        {
            let cache = state.metadata_cache.read().unwrap();
            assert_eq!(cache.leases.get(&(orders(), 2)), Some(&lease));
            assert_eq!(cache.cluster_version, 3);
            assert_eq!(
                cache.last_applied_hlc,
                Hlc::new(90, 0),
                "the floor is the highest seeded lease or drain expiry"
            );
            assert!(cache.topology_log.is_empty() && cache.routing_log.is_empty());
        }
        assert_eq!(
            state.lease_drain.snapshot(),
            vec![(
                orders(),
                DrainOwner::Ddl,
                DrainEntry {
                    up_to_version: 4,
                    expires_at: Hlc::new(90, 0),
                    proposer_node_id: 2,
                }
            )],
            "an ended owner's drain is not seeded"
        );
        let record = state.pending_ddl.get(42).expect("pending record seeded");
        assert_eq!(record.objects, vec![pending]);
        assert_eq!(record.proposed_at, Hlc::new(60, 0));
        assert_eq!(
            state
                .metadata_ddl
                .owner
                .lock()
                .unwrap()
                .map(|owner| (owner.token, owner.node_id)),
            Some((42, 2))
        );
        assert_eq!(
            state
                .metadata_ddl
                .applied_token
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );

        // Releases remove the rows the grants wrote.
        apply(
            &applier,
            1,
            &MetadataEntry::DescriptorLeaseRelease {
                node_id: 2,
                descriptor_ids: vec![orders()],
            },
        )
        .await;
        apply(&applier, 2, &MetadataEntry::DdlPrepareRelease { token: 42 }).await;
        let catalog = state.credentials.catalog();
        assert!(catalog.load_descriptor_leases().unwrap().is_empty());
        assert_eq!(catalog.load_ddl_owner().unwrap(), None);
    }
}

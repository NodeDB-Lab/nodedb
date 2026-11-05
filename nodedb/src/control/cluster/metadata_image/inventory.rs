// SPDX-License-Identifier: BUSL-1.1

//! The catalog objects a metadata image install reconciles, read before and
//! after the replicated tables are replaced.

use std::collections::{HashMap, HashSet};

use nodedb_types::{DatabaseId, StoredVectorIndexParams};

use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::security::catalog::{
    SequenceState, StoredCollection, StoredContinuousAggregate, StoredSequence, SystemCatalog,
};
use crate::types::TenantId;

/// `(database, tenant, name)` of a catalog object.
pub(super) type ObjectKey = (u64, u64, String);

/// Catalog objects whose change the Data Plane or a live registry must see.
#[derive(Default)]
pub(super) struct Inventory {
    pub(super) collections: HashMap<ObjectKey, StoredCollection>,
    pub(super) arrays: HashMap<ObjectKey, ArrayCatalogEntry>,
    /// `(database, tenant, collection, field)` → encoded params.
    pub(super) vector_params: HashMap<(u64, u64, String, String), Vec<u8>>,
    /// Aggregate key → encoded definition.
    pub(super) continuous_aggregates: HashMap<ObjectKey, Vec<u8>>,
    /// Collection key → the version its CRDT history was last compacted to.
    pub(super) compaction_points: HashMap<ObjectKey, String>,
    /// Sequence key → encoded definition and counter state.
    pub(super) sequences: HashMap<ObjectKey, (Vec<u8>, Option<Vec<u8>>)>,
    pub(super) synonym_groups: HashSet<ObjectKey>,
    pub(super) topics: HashSet<ObjectKey>,
    pub(super) change_streams: HashSet<ObjectKey>,
    pub(super) tenants: HashSet<u64>,
    pub(super) database_quotas: HashSet<DatabaseId>,
    pub(super) tenant_quotas: HashSet<(DatabaseId, TenantId)>,
    /// SPKI → expiry (epoch-ms) of every live enrollment pre-authorization.
    pub(super) preauthorizations: HashMap<[u8; 32], u64>,
}

fn encode<T: zerompk::ToMessagePack>(value: &T, what: &str) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Internal {
        detail: format!("metadata image inventory: encode {what}: {e}"),
    })
}

/// Wall clock in epoch-ms, for pre-authorization expiry.
pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(u64::MAX)
}

impl Inventory {
    /// The `(database, tenant)` pairs a `MOVE TENANT` emptied between `self` and
    /// `after`.
    ///
    /// A cutover the image carries leaves no entry to apply, so the move shows as
    /// an object that left its database while its tenant holds the same name in
    /// another one. A drop in that window with the same name elsewhere reads the
    /// same way, and costs that tenant's sessions in the database a reconnect.
    pub(super) fn moved_tenant_scopes(&self, after: &Inventory) -> HashSet<(u64, u64)> {
        let names_after: HashSet<(u64, &str)> = after
            .collections
            .keys()
            .chain(after.arrays.keys())
            .map(|(_, tenant, name)| (*tenant, name.as_str()))
            .collect();
        self.collections
            .keys()
            .chain(self.arrays.keys())
            .filter(|key| {
                !after.collections.contains_key(*key)
                    && !after.arrays.contains_key(*key)
                    && names_after.contains(&(key.1, key.2.as_str()))
            })
            .map(|(database, tenant, _)| (*database, *tenant))
            .collect()
    }

    /// Read the inventory from `catalog`.
    pub(super) fn read(catalog: &SystemCatalog) -> crate::Result<Self> {
        let collections = catalog
            .load_all_collections_across_databases()?
            .into_iter()
            .map(|c| ((c.database_id.as_u64(), c.tenant_id, c.name.clone()), c))
            .collect();
        let arrays = catalog
            .load_all_arrays()?
            .into_iter()
            .map(|a| {
                (
                    (
                        a.array_id.database_id.as_u64(),
                        a.array_id.tenant_id.as_u64(),
                        a.name.clone(),
                    ),
                    a,
                )
            })
            .collect();
        let mut vector_params = HashMap::new();
        for p in catalog.list_all_vector_index_params()? {
            let bytes = encode::<StoredVectorIndexParams>(&p, "vector index params")?;
            vector_params.insert(
                (p.database_id, p.tenant_id, p.collection, p.field_name),
                bytes,
            );
        }
        let mut continuous_aggregates = HashMap::new();
        for a in catalog.load_all_continuous_aggregates()? {
            let bytes = encode::<StoredContinuousAggregate>(&a, "continuous aggregate")?;
            continuous_aggregates.insert((a.database_id, a.tenant_id, a.name), bytes);
        }
        let compaction_points = catalog
            .load_compaction_points()?
            .into_iter()
            .map(|p| {
                (
                    (p.database_id, p.tenant_id, p.collection),
                    p.target_version_json,
                )
            })
            .collect();
        let mut sequences = HashMap::new();
        for def in catalog.load_all_sequences()? {
            let state = catalog
                .get_sequence_state(def.database_id, def.tenant_id, &def.name)?
                .map(|state| encode::<SequenceState>(&state, "sequence state"))
                .transpose()?;
            let bytes = encode::<StoredSequence>(&def, "sequence")?;
            sequences.insert((def.database_id, def.tenant_id, def.name), (bytes, state));
        }
        let synonym_groups = catalog
            .load_all_synonym_groups()?
            .into_iter()
            .map(|g| (g.database_id, g.tenant_id, g.name))
            .collect();
        let topics = catalog
            .load_all_ep_topics()?
            .into_iter()
            .map(|t| (t.database_id.as_u64(), t.tenant_id, t.name))
            .collect();
        let change_streams = catalog
            .load_all_change_streams()?
            .into_iter()
            .map(|s| (s.database_id.as_u64(), s.tenant_id, s.name))
            .collect();
        let tenants = catalog
            .load_all_tenants()?
            .into_iter()
            .map(|t| t.tenant_id)
            .collect();
        let database_quotas = catalog
            .list_database_quotas_lossy()?
            .0
            .into_iter()
            .map(|(db, _)| db)
            .collect();
        let tenant_quotas = catalog
            .list_all_tenant_quotas_lossy()?
            .0
            .into_iter()
            .map(|(db, tenant, _)| (db, tenant))
            .collect();
        let preauthorizations = catalog
            .list_enrollment_preauthorizations(now_ms())?
            .into_iter()
            .collect();
        Ok(Self {
            collections,
            arrays,
            vector_params,
            continuous_aggregates,
            compaction_points,
            sequences,
            synonym_groups,
            topics,
            change_streams,
            tenants,
            database_quotas,
            tenant_quotas,
            preauthorizations,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory_of(objects: &[(u64, u64, &str)]) -> Inventory {
        let mut inventory = Inventory::default();
        for (database, tenant, name) in objects {
            inventory.collections.insert(
                (*database, *tenant, (*name).to_string()),
                StoredCollection::stamped_for_test(*tenant, name, "owner"),
            );
        }
        inventory
    }

    #[test]
    fn an_object_that_changed_database_marks_its_source_scope() {
        let before = inventory_of(&[(70, 5, "orders"), (70, 6, "users"), (72, 5, "kept")]);
        let after = inventory_of(&[(71, 5, "orders"), (70, 6, "users"), (72, 5, "kept")]);

        let scopes = before.moved_tenant_scopes(&after);

        assert_eq!(scopes, HashSet::from([(70, 5)]));
    }

    #[test]
    fn a_plain_drop_marks_no_scope() {
        let before = inventory_of(&[(70, 5, "orders")]);
        let after = inventory_of(&[]);

        assert!(before.moved_tenant_scopes(&after).is_empty());
    }
}

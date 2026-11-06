// SPDX-License-Identifier: BUSL-1.1

//! The catalog entries that drop one database and every object in it.
//!
//! `DROP DATABASE` proposes the whole plan as one metadata commit. Each entry
//! is the replicated delete the object's own `DROP` uses, so every node runs
//! the same apply and post-apply for it: registry eviction, owner and grant
//! cleanup, and Data Plane reclaim for a collection or an array. A restart
//! replays the commit as one unit.

use std::collections::HashSet;

use nodedb_types::{DatabaseId, Hlc};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::permission::parse_scoped_target;

/// Build the teardown of `db_id` from the catalog as it stands.
///
/// `DeleteDatabase` comes last, after every entry of
/// [`plan_database_objects_teardown`].
pub fn plan_database_teardown(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
) -> crate::Result<Vec<CatalogEntry>> {
    let mut plan = plan_database_objects_teardown(catalog, db_id)?;
    for grant in catalog.list_database_grants(db_id)? {
        plan.push(CatalogEntry::DeleteDatabaseGrant {
            db_id: db_id.as_u64(),
            user_id: grant.user_id,
            privilege: grant.privilege,
        });
    }
    plan.push(CatalogEntry::DeleteDatabase {
        db_id: db_id.as_u64(),
    });
    Ok(plan)
}

/// The deletes of every object inside `db_id`, keeping the database's own row
/// and its grants. `REBOOTSTRAP` uses this: a mirror's grants are this cluster's.
///
/// Dependents come before the objects they name, and collections come after
/// every object defined over them. Fenced deletes carry an unstamped target:
/// the proposer freezes it.
pub fn plan_database_objects_teardown(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
) -> crate::Result<Vec<CatalogEntry>> {
    let db = db_id.as_u64();
    let mut plan = Vec::new();

    for group in catalog
        .load_all_consumer_groups()?
        .into_iter()
        .filter(|g| g.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteConsumerGroup {
            database_id: db,
            tenant_id: group.tenant_id,
            stream_name: group.stream_name,
            name: group.name,
            target_hlc: Hlc::ZERO,
        });
    }
    for stream in catalog
        .load_all_change_streams()?
        .into_iter()
        .filter(|s| s.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteChangeStream {
            database_id: db,
            tenant_id: stream.tenant_id,
            name: stream.name,
            target_hlc: Hlc::ZERO,
        });
    }
    for topic in catalog
        .load_all_ep_topics()?
        .into_iter()
        .filter(|t| t.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteTopicWithConsumerGroups {
            database_id: db,
            tenant_id: topic.tenant_id,
            name: topic.name,
            target_hlc: Hlc::ZERO,
        });
    }
    for trigger in catalog.load_triggers_for_database(db_id)? {
        plan.push(CatalogEntry::DeleteTrigger {
            database_id: db_id,
            tenant_id: trigger.tenant_id,
            name: trigger.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    for schedule in catalog
        .load_all_schedules()?
        .into_iter()
        .filter(|s| s.database_id == db)
    {
        plan.push(CatalogEntry::DeleteSchedule {
            database_id: db_id,
            tenant_id: schedule.tenant_id,
            name: schedule.name,
        });
    }
    for view in catalog.load_streaming_mvs_for_database(db_id)? {
        plan.push(CatalogEntry::DeleteStreamingMaterializedView {
            database_id: db,
            tenant_id: view.tenant_id,
            name: view.name,
        });
    }
    for aggregate in catalog.list_continuous_aggregates_in_database(db)? {
        plan.push(CatalogEntry::DeleteContinuousAggregate {
            database_id: db,
            tenant_id: aggregate.tenant_id,
            name: aggregate.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    // A materialized view's delete also purges its same-name target
    // collection, so the collection pass below skips those targets.
    let mut view_targets = HashSet::new();
    for view in catalog.list_materialized_views_in_database(db)? {
        view_targets.insert((view.tenant_id, view.name.clone()));
        plan.push(CatalogEntry::DeleteMaterializedView {
            database_id: db,
            tenant_id: view.tenant_id,
            name: view.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    for rule in catalog.load_alert_rules_in_database(db)? {
        plan.push(CatalogEntry::DeleteAlertRule {
            database_id: db,
            tenant_id: rule.tenant_id,
            name: rule.name,
        });
    }
    for policy in catalog.load_retention_policies_in_database(db)? {
        plan.push(CatalogEntry::DeleteRetentionPolicy {
            database_id: db,
            tenant_id: policy.tenant_id,
            name: policy.name,
            collection: policy.collection,
        });
    }
    for policy in catalog
        .load_all_rls_policies()?
        .into_iter()
        .filter(|p| p.database_id == db)
    {
        plan.push(CatalogEntry::DeleteRlsPolicy {
            tenant_id: policy.tenant_id,
            collection: policy.collection,
            name: policy.name,
        });
    }
    for function in catalog
        .load_all_functions()?
        .into_iter()
        .filter(|f| f.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteFunction {
            database_id: db_id,
            tenant_id: function.tenant_id,
            name: function.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    for procedure in catalog
        .load_all_procedures()?
        .into_iter()
        .filter(|p| p.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteProcedure {
            database_id: db_id,
            tenant_id: procedure.tenant_id,
            name: procedure.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    for sequence in catalog.load_sequences_in_database(db)? {
        plan.push(CatalogEntry::DeleteSequence {
            database_id: db,
            tenant_id: sequence.tenant_id,
            name: sequence.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    // The purge removes each collection's indexes, vector parameters and
    // models, surrogates, redaction policies, owner row, and engine storage.
    for collection in catalog
        .load_all_collections(db_id)?
        .into_iter()
        .filter(|c| !view_targets.contains(&(c.tenant_id, c.name.clone())))
    {
        plan.push(CatalogEntry::PurgeCollection {
            database_id: db,
            tenant_id: collection.tenant_id,
            name: collection.name,
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        });
    }
    // Each array delete drops the cell store on every core of every node.
    for array in catalog
        .load_all_arrays()?
        .into_iter()
        .filter(|a| a.array_id.database_id == db_id)
    {
        plan.push(CatalogEntry::DeleteArray {
            database_id: db,
            tenant_id: array.array_id.tenant_id.as_u64(),
            name: array.name,
            target_hlc: Hlc::ZERO,
            moved_to: None,
        });
    }
    for group in catalog.load_synonym_groups_in_database(db)? {
        plan.push(CatalogEntry::DeleteSynonymGroup {
            database_id: db,
            tenant_id: group.tenant_id,
            name: group.name,
            target_hlc: Hlc::ZERO,
        });
    }
    for custom_type in catalog.load_custom_types_in_database(db)? {
        plan.push(CatalogEntry::DeleteCustomType {
            database_id: db,
            tenant_id: custom_type.tenant_id,
            name: custom_type.name,
        });
    }
    // Grants on the database's collections, functions, and procedures.
    for grant in catalog.load_all_permissions()?.into_iter().filter(|grant| {
        parse_scoped_target(&grant.target).is_some_and(|target| target.database_id == db)
    }) {
        plan.push(CatalogEntry::DeletePermission {
            target: grant.target,
            grantee: grant.grantee,
            permission: grant.permission,
        });
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn collection(db: DatabaseId, tenant: u64, name: &str) -> StoredCollection {
        let mut stored = StoredCollection::stamped_for_test(tenant, name, "admin");
        stored.database_id = db;
        stored
    }

    /// Every collection of the database, active or soft-deleted, is purged
    /// through the plan, the database goes last, and other databases are
    /// untouched.
    #[test]
    fn plan_purges_every_collection_then_drops_the_database() {
        let (_dir, catalog) = open_catalog();
        let dropped = DatabaseId::new(1024);
        let kept = DatabaseId::new(1025);
        catalog
            .put_collection(dropped, &collection(dropped, 1, "orders"))
            .unwrap();
        let mut inactive = collection(dropped, 2, "archive");
        inactive.is_active = false;
        catalog.put_collection(dropped, &inactive).unwrap();
        catalog
            .put_collection(kept, &collection(kept, 1, "orders"))
            .unwrap();

        let plan = plan_database_teardown(&catalog, dropped).unwrap();

        let mut purged: Vec<(u64, u64, String)> = plan
            .iter()
            .filter_map(|entry| match entry {
                CatalogEntry::PurgeCollection {
                    database_id,
                    tenant_id,
                    name,
                    ..
                } => Some((*database_id, *tenant_id, name.clone())),
                _ => None,
            })
            .collect();
        purged.sort();
        assert_eq!(
            purged,
            vec![
                (1024, 1, "orders".to_string()),
                (1024, 2, "archive".to_string())
            ]
        );
        assert!(matches!(
            plan.last(),
            Some(CatalogEntry::DeleteDatabase { db_id: 1024 })
        ));
    }

    /// Grants on the dropped database's objects are revoked. A grant on the
    /// same-name collection of another database stays.
    #[test]
    fn plan_revokes_grants_of_the_database_only() {
        use crate::control::security::catalog::StoredPermission;
        use crate::control::security::permission::collection_target;
        use crate::types::TenantId;

        let (_dir, catalog) = open_catalog();
        let dropped = DatabaseId::new(1024);
        let kept = DatabaseId::new(1025);
        for db in [dropped, kept] {
            catalog
                .put_permission(&StoredPermission {
                    target: collection_target(db, TenantId::new(1), "orders"),
                    grantee: "user:bob".into(),
                    permission: "read".into(),
                    granted_by: "admin".into(),
                    granted_at: 0,
                })
                .unwrap();
        }

        let plan = plan_database_teardown(&catalog, dropped).unwrap();
        let revoked: Vec<&str> = plan
            .iter()
            .filter_map(|entry| match entry {
                CatalogEntry::DeletePermission { target, .. } => Some(target.as_str()),
                _ => None,
            })
            .collect();
        let expected = collection_target(dropped, TenantId::new(1), "orders");
        assert_eq!(revoked, vec![expected.as_str()]);
    }

    /// Every array of the database is dropped through the plan, ahead of the
    /// database. An array of another database stays.
    #[test]
    fn plan_drops_every_array_of_the_database() {
        use crate::control::array_catalog::ArrayCatalogEntry;
        use nodedb_array::types::ArrayId;
        use nodedb_types::TenantId;

        let (_dir, catalog) = open_catalog();
        let dropped = DatabaseId::new(1024);
        let kept = DatabaseId::new(1025);
        for (db, tenant) in [(dropped, 1), (dropped, 2), (kept, 1)] {
            catalog
                .put_array(&ArrayCatalogEntry {
                    array_id: ArrayId::in_database(TenantId::new(tenant), db, "grid"),
                    name: "grid".to_string(),
                    schema_msgpack: vec![0x90],
                    schema_hash: 7,
                    created_at_ms: 0,
                    prefix_bits: 8,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                    modification_hlc: Hlc::ZERO,
                    incarnation: nodedb_types::Hlc::ZERO,
                })
                .unwrap();
        }

        let plan = plan_database_teardown(&catalog, dropped).unwrap();

        let mut arrays: Vec<(u64, u64, &str, bool)> = plan
            .iter()
            .filter_map(|entry| match entry {
                CatalogEntry::DeleteArray {
                    database_id,
                    tenant_id,
                    name,
                    moved_to,
                    ..
                } => Some((*database_id, *tenant_id, name.as_str(), moved_to.is_some())),
                _ => None,
            })
            .collect();
        arrays.sort();
        assert_eq!(
            arrays,
            vec![(1024, 1, "grid", false), (1024, 2, "grid", false)]
        );
        let last_array = plan
            .iter()
            .rposition(|entry| matches!(entry, CatalogEntry::DeleteArray { .. }))
            .unwrap();
        assert!(matches!(
            plan.last(),
            Some(CatalogEntry::DeleteDatabase { db_id: 1024 })
        ));
        assert!(last_array < plan.len() - 1);
    }
}

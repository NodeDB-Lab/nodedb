// SPDX-License-Identifier: BUSL-1.1

//! Bring this node's Data Plane in line with a metadata image.
//!
//! An install skips every entry the image covers, and with them the Data
//! Plane effects their post-apply made on a node that applied them. This pass
//! reproduces those effects from the difference between the catalog before
//! and after the install:
//! - `PurgeCollection`, and the source side of `MoveTenantCutover`: a
//!   collection the image no longer holds has its storage reclaimed.
//! - `PutCollection` / `PutCollectionIfAbsent`: a new incarnation's storage
//!   is cleared first, then every active collection is registered.
//! - `PutArray` / `DeleteArray`: a dropped array is dropped on every core, a
//!   moved one is rekeyed, a new or changed one is opened, and the array
//!   mirror is rebuilt from the catalog.
//! - `PutVectorIndexParams` / `DeleteVectorIndexParams`: changed parameters
//!   are installed, removed ones dropped.
//! - `PutContinuousAggregate` / `DeleteContinuousAggregate`: changed
//!   aggregates are registered, removed ones unregistered.
//! - `PutSynonymGroup` / `DeleteSynonymGroup`: every group is installed,
//!   removed ones deleted.
//! - `PutSpatialRefSys` / `DropSpatialRefSys`: the catalog's user-defined
//!   CRS set is sent to every core.
//! - `DeleteMaterializedView`: a view target is a collection, reclaimed by
//!   the collection rule.
//! - `DeleteTopicWithConsumerGroups`: a removed topic's node-local messages
//!   and CDC buffer go.
//! - `DeleteChangeStream`: a removed stream's CDC buffer goes.
//! - `CompactHistory`: a collection whose compaction point moved owes a
//!   compaction to the image's point. The owed row is written, then every
//!   core compacts. A failed compaction stays owed for the retry worker.

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::catalog_entry::post_apply::{
    ContinuousAggregateRegisterFailure, array, clear_before_recreate, compact_async,
    drop_array_on_every_core, install_custom_crs_set, install_synonym_group,
    install_vector_index_params, open_array_on_every_core, reclaim_collection_storage,
    register_continuous_aggregate_on_every_core, remove_synonym_group, remove_vector_index_params,
    unregister_continuous_aggregate,
};
use crate::control::security::catalog::StoredPendingHistoryCompaction;
use crate::control::state::SharedState;

use super::inventory::{Inventory, ObjectKey};

/// Reproduce on this node's Data Plane every effect the skipped entries
/// make.
pub(super) async fn reconcile_data_plane(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    reconcile_collections(shared, before, after).await?;
    reconcile_compactions(shared, before, after).await?;
    reconcile_arrays(shared, before, after).await?;
    reconcile_vector_params(shared, before, after).await?;
    reconcile_continuous_aggregates(shared, before, after).await?;
    reconcile_synonym_groups(shared, before, after).await?;
    // The set is whole and small, so every install sends it.
    install_custom_crs_set(shared, "image").await?;
    reconcile_event_buffers(shared, before, after)?;
    invalidate_plans(shared, before, after);
    Ok(())
}

/// Owe and run a compaction for every collection whose compaction point the
/// image moved. Each point comes from a committed `CompactHistory` the
/// install skipped.
async fn reconcile_compactions(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    let owed = owed_compactions(&before.compaction_points, &after.compaction_points);
    let catalog = shared.credentials.catalog();
    for row in &owed {
        catalog.enqueue_pending_history_compaction(row)?;
    }
    for row in owed {
        if let Err(error) = compact_async(
            row.database_id,
            row.tenant_id,
            &row.collection,
            &row.target_version_json,
            shared,
        )
        .await
        {
            tracing::warn!(
                collection = %row.collection,
                tenant = row.tenant_id,
                error = %error,
                "metadata snapshot install: history compaction still owed; the retry worker owns it"
            );
        }
    }
    Ok(())
}

/// The owed compaction of every collection whose point in `after` differs
/// from the one in `before`.
fn owed_compactions(
    before: &HashMap<ObjectKey, String>,
    after: &HashMap<ObjectKey, String>,
) -> Vec<StoredPendingHistoryCompaction> {
    after
        .iter()
        .filter(|(key, target)| before.get(*key) != Some(*target))
        .map(
            |((database_id, tenant_id, collection), target)| StoredPendingHistoryCompaction {
                database_id: *database_id,
                tenant_id: *tenant_id,
                collection: collection.clone(),
                target_version_json: target.clone(),
                last_error: String::new(),
                attempts: 0,
            },
        )
        .collect()
}

/// Drop this node's CDC buffer of every change stream the image removed,
/// and the messages and buffer of every removed topic.
fn reconcile_event_buffers(
    shared: &SharedState,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    for (db, tenant, name) in before.change_streams.difference(&after.change_streams) {
        shared
            .cdc_router
            .remove_buffer(nodedb_types::DatabaseId::new(*db), *tenant, name);
    }
    for (db, tenant, name) in before.topics.difference(&after.topics) {
        shared
            .credentials
            .catalog()
            .delete_ep_topic_with_consumer_groups_unchecked(
                nodedb_types::DatabaseId::new(*db),
                *tenant,
                name,
            )?;
        crate::control::catalog_entry::post_apply::topic::delete_with_consumer_groups(
            *db, *tenant, name, shared,
        );
    }
    Ok(())
}

async fn reconcile_collections(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    for (db, tenant, name) in before.collections.keys() {
        if after
            .collections
            .contains_key(&(*db, *tenant, name.clone()))
        {
            continue;
        }
        let purge_lsn = shared.wal.next_lsn().as_u64();
        match reclaim_collection_storage(shared, *db, *tenant, name, purge_lsn, false).await {
            Ok(()) => {}
            Err(failure) if failure.retry_queued => tracing::warn!(
                collection = %name,
                tenant = *tenant,
                error = %failure.error,
                "metadata snapshot install: reclaim failed; the pending-reclaim worker owns the retry"
            ),
            Err(failure) => return Err(failure.error),
        }
    }
    for (key, stored) in &after.collections {
        if !stored.is_active {
            continue;
        }
        let same_incarnation = before
            .collections
            .get(key)
            .is_some_and(|old| old.created_at == stored.created_at);
        if !same_incarnation {
            clear_before_recreate(shared, key.0, key.1, &key.2).await?;
        }
    }
    crate::bootstrap::schema_rehydrate::rehydrate_schema_registry(shared)
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("metadata snapshot install: register collections: {e}"),
        })
}

/// The target database of a move of the array `entry` that the catalog no
/// longer holds at `key`.
///
/// A move keeps the array's incarnation, the stamp of the `PutArray` that
/// created it, and changes only its database. So the target is the one array
/// the install added with the same tenant, name, and incarnation in another
/// database. A same-named array with another incarnation is an unrelated
/// array, never a move. An unstamped incarnation names no array, so it never
/// matches.
fn move_target(
    key: &ObjectKey,
    entry: &ArrayCatalogEntry,
    before: &HashMap<ObjectKey, ArrayCatalogEntry>,
    after: &HashMap<ObjectKey, ArrayCatalogEntry>,
) -> Option<u64> {
    if entry.incarnation == nodedb_types::Hlc::ZERO {
        return None;
    }
    let mut targets = after.iter().filter(|((db, tenant, name), added)| {
        added.incarnation == entry.incarnation
            && *tenant == key.1
            && *name == key.2
            && *db != key.0
            && !before.contains_key(&(*db, *tenant, name.clone()))
    });
    let target = targets.next()?;
    targets.next().is_none().then_some(target.0.0)
}

async fn reconcile_arrays(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    let mut moved_in: Vec<ObjectKey> = Vec::new();
    for (key, entry) in &before.arrays {
        if after.arrays.contains_key(key) {
            continue;
        }
        let target = move_target(key, entry, &before.arrays, &after.arrays);
        if let Some(to) = target {
            moved_in.push((to, key.1, key.2.clone()));
        }
        drop_array_on_every_core(shared, key.0, key.1, &key.2, target).await?;
    }

    // The Data Plane opens an array from the mirror, so the mirror is the
    // catalog's before any open.
    let mirror = crate::control::array_catalog::persist::load_all(shared.credentials.catalog())?;
    *shared
        .array_catalog
        .write()
        .unwrap_or_else(|p| p.into_inner()) = mirror;
    for entry in after.arrays.values() {
        array::put_sync(entry, shared);
    }

    for (key, entry) in &after.arrays {
        if moved_in.contains(key) {
            continue;
        }
        let unchanged = before
            .arrays
            .get(key)
            .is_some_and(|old| old.schema_hash == entry.schema_hash);
        if !unchanged {
            open_array_on_every_core(shared, entry).await?;
        }
    }
    Ok(())
}

async fn reconcile_vector_params(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    for (key, _) in before
        .vector_params
        .iter()
        .filter(|(k, _)| !after.vector_params.contains_key(*k))
    {
        remove_vector_index_params(
            key.0,
            key.1,
            key.2.clone(),
            key.3.clone(),
            Arc::clone(shared),
        )
        .await;
    }
    for (key, bytes) in &after.vector_params {
        if before.vector_params.get(key) == Some(bytes) {
            continue;
        }
        let params = zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Internal {
            detail: format!("metadata snapshot install: decode vector index params: {e}"),
        })?;
        install_vector_index_params(params, Arc::clone(shared)).await;
    }
    Ok(())
}

async fn reconcile_continuous_aggregates(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    for (db, tenant, name) in before.continuous_aggregates.keys() {
        if !after
            .continuous_aggregates
            .contains_key(&(*db, *tenant, name.clone()))
        {
            unregister_continuous_aggregate(*db, *tenant, name.clone(), Arc::clone(shared)).await;
        }
    }
    for (key, bytes) in &after.continuous_aggregates {
        if before.continuous_aggregates.get(key) == Some(bytes) {
            continue;
        }
        let stored: crate::control::security::catalog::StoredContinuousAggregate =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Internal {
                detail: format!("metadata snapshot install: decode continuous aggregate: {e}"),
            })?;
        register_continuous_aggregate_on_every_core(
            shared,
            stored.tenant_id,
            &stored.name,
            &stored.def_bytes,
        )
        .await
        .map_err(|failure: ContinuousAggregateRegisterFailure| failure.error)?;
    }
    Ok(())
}

async fn reconcile_synonym_groups(
    shared: &Arc<SharedState>,
    before: &Inventory,
    after: &Inventory,
) -> crate::Result<()> {
    for (db, tenant, name) in before.synonym_groups.difference(&after.synonym_groups) {
        remove_synonym_group(*db, *tenant, name.clone(), shared).await;
    }
    for group in shared.credentials.catalog().load_all_synonym_groups()? {
        install_synonym_group(group, shared).await;
    }
    Ok(())
}

/// Drop every cached plan that names a collection whose descriptor changed.
fn invalidate_plans(shared: &SharedState, before: &Inventory, after: &Inventory) {
    let Some(invalidator) = shared.gateway_invalidator.get() else {
        return;
    };
    for (key, old) in &before.collections {
        let version = after
            .collections
            .get(key)
            .map_or(u64::MAX, |new| new.descriptor_version);
        if version != old.descriptor_version {
            invalidator.invalidate(&key.2, version);
        }
    }
    for (key, new) in &after.collections {
        if !before.collections.contains_key(key) {
            invalidator.invalidate(&key.2, new.descriptor_version);
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;
    use nodedb_types::{DatabaseId, Hlc, TenantId};

    use super::*;

    const TENANT: u64 = 3;

    fn array(db: u64, name: &str, incarnation: Hlc) -> (ObjectKey, ArrayCatalogEntry) {
        let entry = ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(TENANT), DatabaseId::new(db), name),
            name: name.to_string(),
            schema_msgpack: Vec::new(),
            schema_hash: 7,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: incarnation,
            incarnation,
        };
        ((db, TENANT, name.to_string()), entry)
    }

    #[test]
    fn a_moved_compaction_point_is_owed_and_a_held_one_is_not() {
        let key = |name: &str| (2, TENANT, name.to_string());
        let before = HashMap::from([
            (key("held"), "{\"1\":4}".to_string()),
            (key("behind"), "{\"1\":4}".to_string()),
        ]);
        let after = HashMap::from([
            (key("held"), "{\"1\":4}".to_string()),
            (key("behind"), "{\"1\":9}".to_string()),
            (key("new"), "{\"1\":2}".to_string()),
        ]);
        let mut owed: Vec<(String, String)> = owed_compactions(&before, &after)
            .into_iter()
            .map(|row| (row.collection, row.target_version_json))
            .collect();
        owed.sort();
        assert_eq!(
            owed,
            vec![
                ("behind".to_string(), "{\"1\":9}".to_string()),
                ("new".to_string(), "{\"1\":2}".to_string()),
            ]
        );
    }

    #[test]
    fn a_move_keeps_the_incarnation() {
        let (key, entry) = array(1, "grid", Hlc::new(10, 0));
        let before = HashMap::from([(key.clone(), entry.clone())]);
        let after = HashMap::from([array(2, "grid", Hlc::new(10, 0))]);
        assert_eq!(move_target(&key, &entry, &before, &after), Some(2));
    }

    #[test]
    fn a_same_named_array_in_another_database_is_not_a_move() {
        let (key, entry) = array(1, "grid", Hlc::new(10, 0));
        let before = HashMap::from([(key.clone(), entry.clone())]);
        let after = HashMap::from([array(2, "grid", Hlc::new(11, 0))]);
        assert_eq!(move_target(&key, &entry, &before, &after), None);
    }

    #[test]
    fn an_unstamped_array_is_never_a_move() {
        let (key, entry) = array(1, "grid", Hlc::ZERO);
        let before = HashMap::from([(key.clone(), entry.clone())]);
        let after = HashMap::from([array(2, "grid", Hlc::ZERO)]);
        assert_eq!(move_target(&key, &entry, &before, &after), None);
    }
}

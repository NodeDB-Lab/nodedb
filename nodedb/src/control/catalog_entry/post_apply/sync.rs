// SPDX-License-Identifier: BUSL-1.1

//! Synchronous in-memory cache updates for each [`CatalogEntry`].
//!
//! Runs **inline** on the raft applier thread, BEFORE the metadata
//! applier bumps `AppliedIndexWatcher`. Once `applied_index = N`,
//! readers are guaranteed to see every sync side effect of every
//! entry up to N — no tokio spawn race.
//!
//! Previously `sync` and `async` were combined into a single
//! `tokio::spawn`, so a freshly-applied `PutUser` could bump the
//! watcher while its `install_replicated_user` task was still queued
//! on the scheduler. Tests that waited on `applied_index` and then
//! immediately polled `credentials.get_user` would flake whenever
//! the scheduler ran them in that order. Keeping this function
//! **sync** and inline avoids that race by construction.

use std::sync::Arc;

use super::gateway_invalidation::invalidate_gateway_cache_for_entry;
use super::{
    alert_rule, api_key, auth_user, change_stream, collection, consumer_group,
    continuous_aggregate, custom_type, database, function, materialized_view, owner, permission,
    procedure, quota, redaction, retention_policy, rls, role, schedule, scope_grant, scope_quota,
    sequence, streaming_materialized_view, synonym_group, tenant, topic, trigger, user,
};
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Run every **synchronous** post-apply side effect inline. Must be
/// called from the metadata applier BEFORE the watcher bump so
/// readers of the applied index see every in-memory cache update
/// that entry triggered. Best-effort per variant: the whole thing
/// is infallible today (all typed functions log on failure and
/// return).
pub fn apply_post_apply_side_effects_sync(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    // Gateway plan-cache invalidation: on any descriptor mutation, evict
    // stale cached plans that reference the changed descriptor.
    // This is a single, unconditional call per DDL commit — negligible overhead.
    invalidate_gateway_cache_for_entry(entry, shared);

    match entry {
        CatalogEntry::PutCollection(stored) => {
            // Owner record install is sync; Data Plane register is
            // the async part, handled by `spawn_post_apply_async_side_effects`.
            collection::put_owner_sync(stored, Arc::clone(shared));
            collection::queue_tree_def_sync(stored, shared);
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            // Install owner from the CANONICAL catalog collection, not the
            // carried entry: a no-op re-announce must not overwrite the
            // pre-existing owner. Post-apply the collection always exists,
            // so the read-back is Some; the carried entry is only a
            // best-effort fallback if the redb write silently failed.
            // Owner record install is sync; Data Plane register is
            // the async part, handled by `spawn_post_apply_async_side_effects`.
            let canonical = shared
                .credentials
                .catalog()
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
            let canonical = canonical.as_ref().unwrap_or(&**stored);
            collection::put_owner_sync(canonical, Arc::clone(shared));
            collection::queue_tree_def_sync(canonical, shared);
        }
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            collection::deactivate(*tenant_id, name.clone(), Arc::clone(shared));
            collection::queue_tree_def_removal_sync(*database_id, *tenant_id, name, shared);
        }
        CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
        } => {
            collection::purge_sync(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
            collection::queue_tree_def_removal_sync(*database_id, *tenant_id, name, shared);
        }
        CatalogEntry::PutSequence(stored) => {
            sequence::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSequence {
            database_id,
            tenant_id,
            name,
        } => {
            sequence::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSequenceState(state) => {
            sequence::put_state((**state).clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTrigger(stored) => {
            trigger::put((**stored).clone(), shared);
        }
        CatalogEntry::DeleteTrigger {
            database_id,
            tenant_id,
            name,
        } => {
            trigger::delete(*database_id, *tenant_id, name.clone(), shared);
        }
        CatalogEntry::PutFunction(stored) => {
            function::put((**stored).clone(), shared);
        }
        CatalogEntry::DeleteFunction {
            database_id,
            tenant_id,
            name,
        } => {
            function::delete(*database_id, *tenant_id, name.clone(), shared);
        }
        CatalogEntry::PutProcedure(stored) => {
            procedure::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteProcedure {
            database_id,
            tenant_id,
            name,
        } => {
            procedure::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSchedule(stored) => {
            schedule::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSchedule {
            database_id,
            tenant_id,
            name,
        } => {
            schedule::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutChangeStream(stored) => {
            change_stream::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteChangeStream {
            database_id,
            tenant_id,
            name,
        } => {
            change_stream::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutUser(stored) => {
            user::put(
                (**stored).clone(),
                Arc::clone(shared),
                Some(crate::control::security::buses::SessionInvalidationReason::RoleAltered),
            );
        }
        CatalogEntry::DropUser { username } => {
            user::drop_user(username.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutRole(stored) => {
            role::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteRole { name } => {
            role::delete(name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutApiKey(stored) => {
            api_key::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::RevokeApiKey { key_id } => {
            api_key::revoke(key_id.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutAuthUser(stored) => {
            auth_user::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::PutMaterializedView(stored) => {
            materialized_view::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteMaterializedView {
            database_id,
            tenant_id,
            name,
        } => {
            materialized_view::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutStreamingMaterializedView(definition) => {
            streaming_materialized_view::put((**definition).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteStreamingMaterializedView {
            database_id,
            tenant_id,
            name,
        } => {
            streaming_materialized_view::delete(
                *database_id,
                *tenant_id,
                name.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutContinuousAggregate(stored) => {
            continuous_aggregate::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
        } => {
            continuous_aggregate::delete(
                *database_id,
                *tenant_id,
                name.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutTenant(stored) => {
            tenant::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTenantWithAdmin { tenant, admin } => {
            tenant::put_with_admin((**tenant).clone(), (**admin).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteTenant { tenant_id } => {
            tenant::delete(*tenant_id, Arc::clone(shared));
        }
        CatalogEntry::PutRlsPolicy(stored) => {
            rls::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection,
            name,
        } => {
            rls::delete(
                *tenant_id,
                collection.clone(),
                name.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutRedactionPolicy(stored) => {
            redaction::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteRedactionPolicy {
            tenant_id,
            collection,
            for_role,
        } => {
            redaction::delete(
                *tenant_id,
                collection.clone(),
                for_role.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutPermission(stored) => {
            permission::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeletePermission {
            target,
            grantee,
            permission: perm,
        } => {
            permission::delete(
                target.clone(),
                grantee.clone(),
                perm.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutScopeGrant(stored) => {
            scope_grant::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteScopeGrant {
            scope_name,
            grantee_type,
            grantee_id,
        } => {
            scope_grant::delete(
                scope_name.clone(),
                grantee_type.clone(),
                grantee_id.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutIndexRecord(_) | CatalogEntry::DeleteIndexRecord { .. } => {
            // no-op: the index registry has no in-memory mirror — every
            // reader (SHOW INDEXES, DROP INDEX, collection teardown) goes
            // to the catalog, which `apply` already wrote on this node.
        }
        CatalogEntry::PutOwner(stored) => {
            owner::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteOwner {
            object_type,
            database_id,
            tenant_id,
            object_name,
        } => {
            owner::delete(
                object_type.clone(),
                *database_id,
                *tenant_id,
                object_name.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutSynonymGroup(stored) => {
            synonym_group::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSynonymGroup {
            database_id,
            tenant_id,
            name,
        } => {
            synonym_group::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutCustomType(stored) => {
            custom_type::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteCustomType {
            database_id,
            tenant_id,
            name,
        } => {
            custom_type::delete(*database_id, *tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutDatabase(stored) => {
            database::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteDatabase { db_id } => {
            database::delete(*db_id, Arc::clone(shared));
        }
        CatalogEntry::PutDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => {
            database::put_grant(*db_id, *user_id, privilege.clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => {
            database::delete_grant(*db_id, *user_id, privilege.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutOidcProvider(_) | CatalogEntry::DeleteOidcProvider { .. } => {
            // No in-memory cache to update yet; the OIDC verify path reads from
            // catalog on each request. A runtime cache can be added when needed.
        }
        CatalogEntry::CloneDatabase {
            target_descriptor, ..
        } => {
            // Sync side effect: register the target database in the in-memory
            // database registry so subsequent DDL within the same session can
            // resolve it by name without waiting for a read-round-trip to redb.
            database::put((**target_descriptor).clone(), Arc::clone(shared));
        }
        CatalogEntry::RecordWalTombstone { .. } => {
            // WAL replay barrier only; no in-memory cache to refresh.
        }
        CatalogEntry::PutDatabaseQuota { db_id, record } => {
            quota::put_database(DatabaseId::new(*db_id), record, shared);
        }
        CatalogEntry::DeleteDatabaseQuota { db_id } => {
            quota::delete_database(DatabaseId::new(*db_id), shared);
        }
        CatalogEntry::PutTenantQuota {
            db_id,
            tenant_id,
            record,
        } => {
            quota::put_tenant(
                DatabaseId::new(*db_id),
                TenantId::new(*tenant_id),
                record,
                shared,
            );
        }
        CatalogEntry::DeleteTenantQuota { db_id, tenant_id } => {
            quota::delete_tenant(DatabaseId::new(*db_id), TenantId::new(*tenant_id), shared);
        }
        CatalogEntry::PutScopeQuota(stored) => {
            scope_quota::put(stored, shared);
        }
        CatalogEntry::DeleteScopeQuota { scope_name } => {
            scope_quota::delete(scope_name, shared);
        }
        CatalogEntry::PutRetentionPolicy(def) => {
            retention_policy::put(def, shared);
        }
        CatalogEntry::DeleteRetentionPolicy {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            retention_policy::delete(*database_id, *tenant_id, name, shared);
        }
        CatalogEntry::PutAlertRule(def) => {
            alert_rule::put(def, shared);
        }
        CatalogEntry::DeleteAlertRule {
            database_id,
            tenant_id,
            name,
        } => {
            alert_rule::delete(*database_id, *tenant_id, name, shared);
        }
        CatalogEntry::CreateTopicIfAbsent(def) => {
            topic::create_if_absent(def, shared);
        }
        CatalogEntry::DeleteTopicWithConsumerGroups {
            database_id,
            tenant_id,
            name,
        } => {
            topic::delete_with_consumer_groups(*database_id, *tenant_id, name, shared);
        }
        CatalogEntry::PutConsumerGroupIfAbsent(def) => {
            consumer_group::put_if_absent(def, shared);
        }
        CatalogEntry::DeleteConsumerGroup {
            database_id,
            tenant_id,
            stream_name,
            name,
        } => {
            consumer_group::delete(*database_id, *tenant_id, stream_name, name, shared);
        }
        CatalogEntry::MigrateConsumerGroupStream { def, legacy_stream } => {
            consumer_group::migrate_stream(def, legacy_stream, shared);
        }
        CatalogEntry::PutCheckpoint(_)
        | CatalogEntry::DeleteCheckpoint { .. }
        | CatalogEntry::CompactHistory { .. } => {
            // no-op: checkpoints have no in-memory mirror — every reader
            // (SHOW VERSIONS, AT VERSION, COMPACT HISTORY) goes to the
            // catalog, which `apply` already wrote on this node. The oplog
            // compaction runs from the async post-apply path.
        }
        CatalogEntry::PutVectorModel(_)
        | CatalogEntry::DeleteVectorModel { .. }
        | CatalogEntry::PutVectorIndexParams(_)
        | CatalogEntry::DeleteVectorIndexParams { .. } => {
            // no-op: the Control Plane keeps no mirror of either table. Every
            // reader goes to the catalog, and the Data Plane takes its copy
            // from the boot seed.
        }
        CatalogEntry::PutColumnStats(_) => {
            // no-op: column statistics have no in-memory mirror. The planner
            // reads them from the catalog, which `apply` already wrote.
        }
        CatalogEntry::MoveTenantCutover {
            tenant_id,
            source_db_id,
            target_db_id,
            collections,
        } => {
            tenant::move_cutover_sync(
                *tenant_id,
                *source_db_id,
                *target_db_id,
                collections,
                Arc::clone(shared),
            );
        }
    }
}

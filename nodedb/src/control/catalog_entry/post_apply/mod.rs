//! Post-apply side effects for a [`CatalogEntry`] — dispatched by
//! DDL family.
//!
//! Split into two phases so readers of `applied_index` observe a
//! consistent view:
//!
//! - [`apply_post_apply_side_effects_sync`] runs the synchronous
//!   in-memory cache updates (install_replicated_user,
//!   install_replicated_role, etc.) **inline** on the raft applier
//!   thread, BEFORE the metadata applier bumps the
//!   `AppliedIndexWatcher`. Once `applied_index = N`, readers are
//!   guaranteed to see every sync side-effect of every entry up to
//!   N — no tokio spawn race.
//! - [`spawn_post_apply_async_side_effects`] spawns a tokio task for
//!   the genuinely async work — today that is only Data Plane
//!   dispatches for `PutCollection`. Readers of the Data Plane
//!   register state still race with this, but no test relies on
//!   that synchronisation.
//!
//! Previously both were combined into a single `tokio::spawn`, so
//! a freshly-applied `PutUser` could bump the watcher while its
//! `install_replicated_user` task was still queued on the
//! scheduler. Tests that waited on `applied_index` and then
//! immediately polled `credentials.get_user` would flake whenever
//! the scheduler ran them in that order.

pub mod api_key;
pub mod change_stream;
pub mod collection;
pub mod function;
pub mod materialized_view;
pub mod owner;
pub mod permission;
pub mod procedure;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod sequence;
pub mod tenant;
pub mod trigger;
pub mod user;

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

/// Run every **synchronous** post-apply side effect inline. Must be
/// called from the metadata applier BEFORE the watcher bump so
/// readers of the applied index see every in-memory cache update
/// that entry triggered. Best-effort per variant: the whole thing
/// is infallible today (all typed functions log on failure and
/// return).
pub fn apply_post_apply_side_effects_sync(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            // Owner record install is sync; Data Plane register is
            // the async part, handled by `spawn_post_apply_async_side_effects`.
            collection::put_owner_sync(stored, Arc::clone(shared));
        }
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            collection::deactivate(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSequence(stored) => {
            sequence::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            sequence::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSequenceState(state) => {
            sequence::put_state((**state).clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTrigger(stored) => {
            trigger::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteTrigger { tenant_id, name } => {
            trigger::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutFunction(stored) => {
            function::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteFunction { tenant_id, name } => {
            function::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutProcedure(stored) => {
            procedure::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteProcedure { tenant_id, name } => {
            procedure::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSchedule(stored) => {
            schedule::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSchedule { tenant_id, name } => {
            schedule::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutChangeStream(stored) => {
            change_stream::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteChangeStream { tenant_id, name } => {
            change_stream::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutUser(stored) => {
            user::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeactivateUser { username } => {
            user::deactivate(username.clone(), Arc::clone(shared));
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
        CatalogEntry::PutMaterializedView(stored) => {
            materialized_view::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            materialized_view::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTenant(stored) => {
            tenant::put((**stored).clone(), Arc::clone(shared));
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
        CatalogEntry::PutOwner(stored) => {
            owner::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteOwner {
            object_type,
            tenant_id,
            object_name,
        } => {
            owner::delete(
                object_type.clone(),
                *tenant_id,
                object_name.clone(),
                Arc::clone(shared),
            );
        }
    }
}

/// Spawn the async post-apply side effects of `entry`. Today this is
/// limited to Data Plane dispatches for `PutCollection` (the only
/// genuinely `.await`-carrying variant). Best-effort: failures log
/// and drop.
pub fn spawn_post_apply_async_side_effects(entry: CatalogEntry, shared: Arc<SharedState>) {
    if let CatalogEntry::PutCollection(stored) = entry {
        tokio::spawn(async move {
            collection::put_async(*stored, shared).await;
        });
    }
}

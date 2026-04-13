//! Asynchronous post-apply side effects for a [`CatalogEntry`] —
//! dispatched by DDL family.
//!
//! The top-level [`spawn_post_apply_side_effects`] is one
//! `tokio::spawn` containing an exhaustive match that routes each
//! variant to a typed function in a per-family sibling file.
//! Adding a new variant forces this file to grow by one line and
//! the corresponding family file by one function — never grows
//! unboundedly.

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

/// Spawn the post-apply side effects of `entry`. Best-effort: any
/// failure inside the spawned task logs a warning but does not
/// unwind the raft apply path.
pub fn spawn_post_apply_side_effects(entry: CatalogEntry, shared: Arc<SharedState>) {
    tokio::spawn(async move {
        match entry {
            CatalogEntry::PutCollection(stored) => {
                collection::put(*stored, shared).await;
            }
            CatalogEntry::DeactivateCollection { tenant_id, name } => {
                collection::deactivate(tenant_id, name, shared);
            }
            CatalogEntry::PutSequence(stored) => {
                sequence::put(*stored, shared);
            }
            CatalogEntry::DeleteSequence { tenant_id, name } => {
                sequence::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutSequenceState(state) => {
                sequence::put_state(*state, shared);
            }
            CatalogEntry::PutTrigger(stored) => {
                trigger::put(*stored, shared);
            }
            CatalogEntry::DeleteTrigger { tenant_id, name } => {
                trigger::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutFunction(stored) => {
                function::put(*stored, shared);
            }
            CatalogEntry::DeleteFunction { tenant_id, name } => {
                function::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutProcedure(stored) => {
                procedure::put(*stored, shared);
            }
            CatalogEntry::DeleteProcedure { tenant_id, name } => {
                procedure::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutSchedule(stored) => {
                schedule::put(*stored, shared);
            }
            CatalogEntry::DeleteSchedule { tenant_id, name } => {
                schedule::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutChangeStream(stored) => {
                change_stream::put(*stored, shared);
            }
            CatalogEntry::DeleteChangeStream { tenant_id, name } => {
                change_stream::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutUser(stored) => {
                user::put(*stored, shared);
            }
            CatalogEntry::DeactivateUser { username } => {
                user::deactivate(username, shared);
            }
            CatalogEntry::PutRole(stored) => {
                role::put(*stored, shared);
            }
            CatalogEntry::DeleteRole { name } => {
                role::delete(name, shared);
            }
            CatalogEntry::PutApiKey(stored) => {
                api_key::put(*stored, shared);
            }
            CatalogEntry::RevokeApiKey { key_id } => {
                api_key::revoke(key_id, shared);
            }
            CatalogEntry::PutMaterializedView(stored) => {
                materialized_view::put(*stored, shared);
            }
            CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
                materialized_view::delete(tenant_id, name, shared);
            }
            CatalogEntry::PutTenant(stored) => {
                tenant::put(*stored, shared);
            }
            CatalogEntry::DeleteTenant { tenant_id } => {
                tenant::delete(tenant_id, shared);
            }
            CatalogEntry::PutRlsPolicy(stored) => {
                rls::put(*stored, shared);
            }
            CatalogEntry::DeleteRlsPolicy {
                tenant_id,
                collection,
                name,
            } => {
                rls::delete(tenant_id, collection, name, shared);
            }
            CatalogEntry::PutPermission(stored) => {
                permission::put(*stored, shared);
            }
            CatalogEntry::DeletePermission {
                target,
                grantee,
                permission: perm,
            } => {
                permission::delete(target, grantee, perm, shared);
            }
            CatalogEntry::PutOwner(stored) => {
                owner::put(*stored, shared);
            }
            CatalogEntry::DeleteOwner {
                object_type,
                tenant_id,
                object_name,
            } => {
                owner::delete(object_type, tenant_id, object_name, shared);
            }
        }
    });
}

//! Synchronous host-side application of a [`CatalogEntry`] to
//! `SystemCatalog` redb — dispatched by DDL family.
//!
//! The top-level [`apply_to`] is an exhaustive match that routes
//! each variant to a typed function in a per-family sibling file.
//! Adding a new variant forces this file to grow by one line (the
//! match arm) and the corresponding family file by one function —
//! never grows unboundedly.

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

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

/// Apply `entry` to `catalog`. Best-effort: per-variant errors are
/// logged + swallowed inside the family handlers so a single write
/// failure doesn't stall the raft apply path. Startup replay will
/// re-run the entry if needed.
pub fn apply_to(entry: &CatalogEntry, catalog: &SystemCatalog) {
    match entry {
        CatalogEntry::PutCollection(stored) => collection::put(stored, catalog),
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            collection::deactivate(*tenant_id, name, catalog)
        }
        CatalogEntry::PutSequence(stored) => sequence::put(stored, catalog),
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            sequence::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutSequenceState(state) => sequence::put_state(state, catalog),
        CatalogEntry::PutTrigger(stored) => trigger::put(stored, catalog),
        CatalogEntry::DeleteTrigger { tenant_id, name } => {
            trigger::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutFunction(stored) => function::put(stored, catalog),
        CatalogEntry::DeleteFunction { tenant_id, name } => {
            function::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutProcedure(stored) => procedure::put(stored, catalog),
        CatalogEntry::DeleteProcedure { tenant_id, name } => {
            procedure::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutSchedule(stored) => schedule::put(stored, catalog),
        CatalogEntry::DeleteSchedule { tenant_id, name } => {
            schedule::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutChangeStream(stored) => change_stream::put(stored, catalog),
        CatalogEntry::DeleteChangeStream { tenant_id, name } => {
            change_stream::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutUser(stored) => user::put(stored, catalog),
        CatalogEntry::DeactivateUser { username } => user::deactivate(username, catalog),
        CatalogEntry::PutRole(stored) => role::put(stored, catalog),
        CatalogEntry::DeleteRole { name } => role::delete(name, catalog),
        CatalogEntry::PutApiKey(stored) => api_key::put(stored, catalog),
        CatalogEntry::RevokeApiKey { key_id } => api_key::revoke(key_id, catalog),
        CatalogEntry::PutMaterializedView(stored) => materialized_view::put(stored, catalog),
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            materialized_view::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutTenant(stored) => tenant::put(stored, catalog),
        CatalogEntry::DeleteTenant { tenant_id } => tenant::delete(*tenant_id, catalog),
        CatalogEntry::PutRlsPolicy(stored) => rls::put(stored, catalog),
        CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection,
            name,
        } => rls::delete(*tenant_id, collection, name, catalog),
        CatalogEntry::PutPermission(stored) => permission::put(stored, catalog),
        CatalogEntry::DeletePermission {
            target,
            grantee,
            permission: perm,
        } => permission::delete(target, grantee, perm, catalog),
        CatalogEntry::PutOwner(stored) => owner::put(stored, catalog),
        CatalogEntry::DeleteOwner {
            object_type,
            tenant_id,
            object_name,
        } => owner::delete(object_type, *tenant_id, object_name, catalog),
    }
}

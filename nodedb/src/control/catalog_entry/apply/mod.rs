// SPDX-License-Identifier: BUSL-1.1

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
pub mod continuous_aggregate;
pub mod custom_type;
pub mod database;
pub mod function;
pub mod local;
pub mod materialized_view;
pub mod oidc_provider;
pub mod owner;
pub mod permission;
pub mod procedure;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod sequence;
pub mod synonym_group;
pub mod tenant;
pub mod trigger;
pub mod user;
pub mod wal_tombstone;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

/// Apply `entry` to `catalog`. Best-effort: per-variant errors are
/// logged + swallowed inside the family handlers so a single write
/// failure doesn't stall the raft apply path. Startup replay will
/// re-run the entry if needed.
///
/// Debug builds run the full referential-integrity verifier after
/// every apply and panic on any violation. This catches the
/// "forgot-to-write-the-owner-row" class of bug on the first DDL a
/// developer runs instead of deferring to the next restart, so
/// reviewers don't need to rely on a user report to surface
/// half-finished sync work. Release builds skip the check.
pub fn apply_to(entry: &CatalogEntry, catalog: &SystemCatalog) -> bool {
    let applied = match entry {
        CatalogEntry::PutTenantWithAdmin { tenant, admin } => {
            tenant::put_with_admin(tenant, admin, catalog)
        }
        _ => {
            apply_to_inner(entry, catalog);
            true
        }
    };
    if !applied {
        return false;
    }
    #[cfg(debug_assertions)]
    {
        // Narrow to OrphanRow — the "half-finished sync" class this
        // check exists to catch (a primary row written without its
        // owner row, or vice versa). DanglingReference is test-fixture
        // hygiene (e.g. a test owner with no StoredUser backing) and
        // legitimate startup state — leave those to the full
        // startup-time verifier.
        use crate::control::cluster::recovery_check::divergence::DivergenceKind;
        let orphans: Vec<_> =
            crate::control::cluster::recovery_check::integrity::verify_redb_integrity(catalog)
                .into_iter()
                .filter(|d| matches!(d.kind, DivergenceKind::OrphanRow { .. }))
                .collect();
        assert!(
            orphans.is_empty(),
            "catalog_entry::apply_to({}) left the catalog in a state \
             that fails verify_redb_integrity — every parent-replicated \
             Put* variant must write both the primary row and the \
             StoredOwner row. Orphan violations: {:?}",
            entry.kind(),
            orphans,
        );
    }
    true
}

fn apply_to_inner(entry: &CatalogEntry, catalog: &SystemCatalog) {
    match entry {
        CatalogEntry::PutCollection(stored) => collection::put(stored, catalog),
        CatalogEntry::PutCollectionIfAbsent(stored) => collection::put_if_absent(stored, catalog),
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            collection::deactivate(*tenant_id, name, catalog)
        }
        CatalogEntry::PurgeCollection { tenant_id, name } => {
            // Raft apply runs symmetrically on every node and must not
            // abort on a per-node catalog storage hiccup — log and
            // continue, exactly as before. The swallow is now an
            // explicit choice at the call site rather than hidden
            // inside `purge`; the interactive re-CREATE caller makes
            // the opposite choice and propagates.
            if let Err(e) = collection::purge(*tenant_id, name, catalog) {
                tracing::warn!(
                    collection = %name,
                    tenant = tenant_id,
                    error = %e,
                    "catalog_entry: purge_collection apply failed"
                );
            }
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
        CatalogEntry::DropUser { username } => user::delete(username, catalog),
        CatalogEntry::PutRole(stored) => role::put(stored, catalog),
        CatalogEntry::DeleteRole { name } => role::delete(name, catalog),
        CatalogEntry::PutApiKey(stored) => api_key::put(stored, catalog),
        CatalogEntry::RevokeApiKey { key_id } => api_key::revoke(key_id, catalog),
        CatalogEntry::PutMaterializedView(stored) => materialized_view::put(stored, catalog),
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            materialized_view::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutContinuousAggregate(stored) => continuous_aggregate::put(stored, catalog),
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
        } => continuous_aggregate::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutTenant(stored) => tenant::put(stored, catalog),
        // Applied by `apply_to` so its commit outcome can suppress post-apply.
        CatalogEntry::PutTenantWithAdmin { .. } => {}
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
        CatalogEntry::PutSynonymGroup(stored) => synonym_group::put(stored, catalog),
        CatalogEntry::DeleteSynonymGroup { tenant_id, name } => {
            synonym_group::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutCustomType(stored) => custom_type::put(stored, catalog),
        CatalogEntry::DeleteCustomType { tenant_id, name } => {
            custom_type::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutDatabase(descriptor) => database::put(descriptor, catalog),
        CatalogEntry::DeleteDatabase { db_id } => database::delete(*db_id, catalog),
        CatalogEntry::PutDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => database::put_grant(*db_id, *user_id, privilege, catalog),
        CatalogEntry::DeleteDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => database::delete_grant(*db_id, *user_id, privilege, catalog),
        CatalogEntry::CloneDatabase {
            target_descriptor,
            source_db_id,
        } => database::clone_apply(target_descriptor, *source_db_id, catalog),
        CatalogEntry::PutOidcProvider(provider) => oidc_provider::put(provider, catalog),
        CatalogEntry::DeleteOidcProvider { name } => oidc_provider::delete(name, catalog),
        CatalogEntry::RecordWalTombstone {
            tenant_id,
            collection,
            purge_lsn,
        } => wal_tombstone::record(*tenant_id, collection, *purge_lsn, catalog),
        CatalogEntry::MoveTenantCutover {
            tenant_id,
            source_db_id,
            target_db_id,
            collections,
        } => tenant::move_cutover(
            *tenant_id,
            *source_db_id,
            *target_db_id,
            collections,
            catalog,
        ),
    }
}

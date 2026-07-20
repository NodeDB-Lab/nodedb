// SPDX-License-Identifier: BUSL-1.1

//! Verifier coverage: `verify_redb_integrity` reports `OrphanRow`
//! divergences symmetrically for every parent-replicated DDL type,
//! not just `Collection`. Plus a compile-time exhaustive classification
//! of every `CatalogEntry` variant so a new type cannot land without
//! declaring its integrity-check status here.

mod catalog_integrity_helpers;

use catalog_integrity_helpers::*;
use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::security::catalog::auth_types::StoredOwner;

#[test]
fn verify_redb_integrity_flags_orphan_function() {
    let (_dir, catalog) = make_catalog();
    catalog.put_function(&make_function("f1")).unwrap();
    assert!(
        find_orphan(&catalog, "function").is_some(),
        "verify_redb_integrity must report OrphanRow(function) when a \
         StoredFunction exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_procedure() {
    let (_dir, catalog) = make_catalog();
    catalog.put_procedure(&make_procedure("p1")).unwrap();
    assert!(
        find_orphan(&catalog, "procedure").is_some(),
        "verify_redb_integrity must report OrphanRow(procedure) when a \
         StoredProcedure exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_trigger() {
    let (_dir, catalog) = make_catalog();
    catalog
        .put_collection(
            nodedb_types::DatabaseId::DEFAULT,
            &make_collection("orders"),
        )
        .unwrap();
    catalog
        .put_owner(&StoredOwner {
            database_id: 0,
            object_type: "collection".into(),
            object_name: "orders".into(),
            tenant_id: TENANT,
            owner_username: ADMIN.into(),
        })
        .unwrap();
    catalog.put_trigger(&make_trigger("t1", "orders")).unwrap();
    assert!(
        find_orphan(&catalog, "trigger").is_some(),
        "verify_redb_integrity must report OrphanRow(trigger) when a \
         StoredTrigger exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_materialized_view() {
    let (_dir, catalog) = make_catalog();
    catalog.put_materialized_view(&make_mv("mv1")).unwrap();
    assert!(
        find_orphan(&catalog, "materialized_view").is_some(),
        "verify_redb_integrity must report OrphanRow(materialized_view) \
         when a StoredMaterializedView exists without a matching \
         StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_sequence() {
    let (_dir, catalog) = make_catalog();
    catalog.put_sequence(&make_sequence("s1")).unwrap();
    assert!(
        find_orphan(&catalog, "sequence").is_some(),
        "verify_redb_integrity must report OrphanRow(sequence) when a \
         StoredSequence exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_schedule() {
    let (_dir, catalog) = make_catalog();
    catalog.put_schedule(&make_schedule("sch1")).unwrap();
    assert!(
        find_orphan(&catalog, "schedule").is_some(),
        "verify_redb_integrity must report OrphanRow(schedule) when a \
         ScheduleDef exists without a matching StoredOwner row"
    );
}

#[test]
fn verify_redb_integrity_flags_orphan_change_stream() {
    let (_dir, catalog) = make_catalog();
    catalog.put_change_stream(&make_stream("cs1")).unwrap();
    assert!(
        find_orphan(&catalog, "change_stream").is_some(),
        "verify_redb_integrity must report OrphanRow(change_stream) \
         when a ChangeStreamDef exists without a matching StoredOwner row"
    );
}

/// On a freshly-bootstrapped catalog (clean data dir, no DDL yet) the
/// startup integrity walk must complete with no divergences. The walk
/// opens every `_system.*` table it cross-checks; a table missing from
/// the bootstrap registry now surfaces as a `TableLoadError` divergence
/// (it used to be a swallowed `tracing::error!`), so an empty divergence
/// list is positive proof that every walked table was bootstrapped —
/// and the list stays in lockstep with the walker automatically as new
/// tables are added to it.
///
/// Regression: `_system.continuous_aggregates` was omitted from the
/// bootstrap init list. Its loader returned `Err("table does not
/// exist")`, the walker logged an `ERROR` on every clean boot, and the
/// integrity check still reported "clean". "No CAs registered" is the
/// legitimate state for a fresh server — it must read back as an empty
/// set, not an error.
#[test]
fn fresh_catalog_loads_every_walked_table() {
    let (_dir, catalog) = make_catalog();

    let divergences = verify_redb_integrity(&catalog);
    assert!(
        divergences.is_empty(),
        "fresh catalog must pass the integrity walk with no divergences, got: {divergences:?}"
    );

    // Named guard for the exact table that was missing: it must load
    // and read back empty on a fresh server.
    let caggs = catalog
        .load_all_continuous_aggregates()
        .expect("continuous_aggregates must load on a fresh catalog");
    assert!(
        caggs.is_empty(),
        "a fresh catalog has no continuous aggregates registered"
    );
}

/// Exhaustive classification of every `CatalogEntry` variant for the
/// parent-owner invariant. Adding a new variant to `CatalogEntry` forces
/// this match to grow by one arm; reviewers decide `ParentReplicated`
/// (applier must call `owner::put_parent_owner`) or `Exempt`
/// (standalone objects, registry-only entries).
///
/// This function is never called at runtime — its value is purely the
/// compile-time exhaustiveness check.
#[allow(dead_code)]
enum VariantClass {
    ParentReplicated,
    Exempt,
}

#[allow(dead_code, clippy::match_same_arms)]
fn classify(entry: &CatalogEntry) -> VariantClass {
    match entry {
        CatalogEntry::PutCollection(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutCollectionIfAbsent(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutFunction(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutProcedure(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutTrigger(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutMaterializedView(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutSequence(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutSchedule(_) => VariantClass::ParentReplicated,
        CatalogEntry::PutChangeStream(_) => VariantClass::ParentReplicated,

        CatalogEntry::DeactivateCollection { .. } => VariantClass::ParentReplicated,
        CatalogEntry::PurgeCollection { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteFunction { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteProcedure { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteTrigger { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteMaterializedView { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteSequence { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteSchedule { .. } => VariantClass::ParentReplicated,
        CatalogEntry::DeleteChangeStream { .. } => VariantClass::ParentReplicated,

        // Continuous aggregates carry a parent-replicated definition row
        // plus an owner row (apply path calls `put_parent_owner` /
        // `delete_parent_owner`).
        CatalogEntry::PutContinuousAggregate(_) => VariantClass::ParentReplicated,
        CatalogEntry::DeleteContinuousAggregate { .. } => VariantClass::ParentReplicated,

        CatalogEntry::PutOwner(_) => VariantClass::Exempt,
        CatalogEntry::DeleteOwner { .. } => VariantClass::Exempt,

        CatalogEntry::PutSequenceState(_) => VariantClass::Exempt,
        CatalogEntry::PutUser(_) => VariantClass::Exempt,
        CatalogEntry::DropUser { .. } => VariantClass::Exempt,
        CatalogEntry::PutRole(_) => VariantClass::Exempt,
        CatalogEntry::DeleteRole { .. } => VariantClass::Exempt,
        CatalogEntry::PutApiKey(_) => VariantClass::Exempt,
        CatalogEntry::RevokeApiKey { .. } => VariantClass::Exempt,
        CatalogEntry::PutPermission(_) => VariantClass::Exempt,
        CatalogEntry::DeletePermission { .. } => VariantClass::Exempt,
        CatalogEntry::PutTenant(_) => VariantClass::Exempt,
        CatalogEntry::PutTenantWithAdmin { .. } => VariantClass::Exempt,
        CatalogEntry::DeleteTenant { .. } => VariantClass::Exempt,
        CatalogEntry::PutRlsPolicy(_) => VariantClass::Exempt,
        CatalogEntry::DeleteRlsPolicy { .. } => VariantClass::Exempt,

        CatalogEntry::PutSynonymGroup(_) => VariantClass::Exempt,
        CatalogEntry::DeleteSynonymGroup { .. } => VariantClass::Exempt,

        // Custom types are registry-only objects (no owner row required).
        CatalogEntry::PutCustomType(_) => VariantClass::Exempt,
        CatalogEntry::DeleteCustomType { .. } => VariantClass::Exempt,

        // WAL replay barrier — replicated for replay correctness, but it is
        // not a parent object and carries no per-object owner row.
        CatalogEntry::RecordWalTombstone { .. } => VariantClass::Exempt,

        // Database descriptors and grants are catalog-level objects with
        // no per-object owner row — they are exempt from the parent-replicated
        // ownership invariant.
        CatalogEntry::PutDatabase(_) => VariantClass::Exempt,
        CatalogEntry::DeleteDatabase { .. } => VariantClass::Exempt,
        CatalogEntry::PutDatabaseGrant { .. } => VariantClass::Exempt,
        CatalogEntry::DeleteDatabaseGrant { .. } => VariantClass::Exempt,
        // Clone creates a new database descriptor; no per-object owner row needed.
        CatalogEntry::CloneDatabase { .. } => VariantClass::Exempt,
        // Move tenant cutover re-keys collections; no ownership object is created.
        CatalogEntry::MoveTenantCutover { .. } => VariantClass::Exempt,

        // OIDC providers are cluster-level identity-provider config; no
        // per-object owner row required.
        CatalogEntry::PutOidcProvider(_) => VariantClass::Exempt,
        CatalogEntry::DeleteOidcProvider { .. } => VariantClass::Exempt,
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Applier contract: for every parent-replicated `Put<T>` variant, the
//! synchronous `apply_to` path MUST write a matching `StoredOwner` row
//! to redb. If it does not, the next restart's integrity check aborts
//! boot with an `OrphanRow` divergence.

mod catalog_integrity_helpers;

use catalog_integrity_helpers::*;
use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::security::catalog::auth_types::StoredOwner;

#[test]
fn apply_put_collection_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutCollection(Box::new(make_collection("orders")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "collection", "orders"),
        "PutCollection apply must write a StoredOwner row to redb; \
         missing row causes verify_redb_integrity to abort startup \
         with an OrphanRow(collection) divergence"
    );
}

#[test]
fn apply_put_function_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutFunction(Box::new(make_function("normalize_email")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "function", "normalize_email"),
        "PutFunction apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_procedure_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutProcedure(Box::new(make_procedure("purge_old")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "procedure", "purge_old"),
        "PutProcedure apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_trigger_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    // Write the parent collection first so Check 4 (trigger →
    // collection) doesn't also fire — this test is about the
    // owner-row gap only.
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

    let entry = CatalogEntry::PutTrigger(Box::new(make_trigger("send_email", "orders")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "trigger", "send_email"),
        "PutTrigger apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_materialized_view_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutMaterializedView(Box::new(make_mv("orders_summary")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "materialized_view", "orders_summary"),
        "PutMaterializedView apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_sequence_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSequence(Box::new(make_sequence("orders_seq")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "sequence", "orders_seq"),
        "PutSequence apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_schedule_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutSchedule(Box::new(make_schedule("nightly")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "schedule", "nightly"),
        "PutSchedule apply must write a StoredOwner row to redb"
    );
}

#[test]
fn apply_put_change_stream_writes_owner_row_to_redb() {
    let (_dir, catalog) = make_catalog();
    let entry = CatalogEntry::PutChangeStream(Box::new(make_stream("orders_cdc")));
    apply_to(&entry, &catalog);
    assert!(
        owner_row_present(&catalog, "change_stream", "orders_cdc"),
        "PutChangeStream apply must write a StoredOwner row to redb"
    );
}

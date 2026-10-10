// SPDX-License-Identifier: BUSL-1.1

//! A committed record never folds a cross-shard sum target on the source's
//! core.
//!
//! A cross-shard balance travels on an `ApplyBalanceDelta` task homed on the
//! target's vShard. An INSERT says so by its deferral list. A DELETE says so
//! by leaving its join value unresolved. The install folds every row of one
//! collection against one merged table. So an INSERT and a DELETE of one
//! account in one transaction must leave that account out of the table.
//! Otherwise the DELETE row folds the account here as well, and the shipped
//! task moves it a second time.

use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget,
};
use nodedb_types::{
    CollectionKey, DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, TenantId,
};

use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::engine::document::store::CollectionConfig;

const DB: u64 = 0;
const TID: u64 = 1;
/// The sum's source collection.
const SOURCE: &str = "local_charges";
/// A target on another vShard than [`SOURCE`].
const TARGET: &str = "remote_balances";
const ACCOUNT: &str = "a1";
const TARGET_SURROGATE: Surrogate = Surrogate(4242);
/// The stored row the transaction deletes.
const DELETED: Surrogate = Surrogate(1);
/// The row the transaction inserts.
const INSERTED: Surrogate = Surrogate(2);

/// A core where [`SOURCE`] sums `amount` into [`TARGET`]'s `balance`, with
/// the target row at 10 and one source row of 10 for the account.
fn seeded_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
        CollectionConfig::new(TARGET),
    );
    let mut source = CollectionConfig::new(SOURCE);
    source.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: TARGET.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        declared_primary_key: None,
    }];
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
        source,
    );
    // The target row lives in this core's store, so a fold on this core is
    // visible as a moved balance rather than a refusal.
    put_row(
        &mut core,
        TARGET,
        TARGET_SURROGATE,
        &serde_json::json!({"id": ACCOUNT, "balance": "10"}),
    );
    put_row(&mut core, SOURCE, DELETED, &entry(10));
    (core, Box::new((req, resp)))
}

fn entry(amount: i64) -> serde_json::Value {
    serde_json::json!({"account_id": ACCOUNT, "amount": amount})
}

fn put_row(core: &mut CoreLoop, collection: &str, surrogate: Surrogate, doc: &serde_json::Value) {
    core.sparse
        .put(
            DB,
            TID,
            collection,
            &StorageKey::for_surrogate(surrogate),
            &doc_format::encode_to_msgpack(doc),
        )
        .expect("seed row");
}

fn row(core: &CoreLoop, collection: &str, surrogate: Surrogate) -> Option<serde_json::Value> {
    let stored = core
        .sparse
        .get(DB, TID, collection, &StorageKey::for_surrogate(surrogate))
        .expect("read row")?;
    doc_format::decode_document(&stored).ok()
}

/// The source slice of `BEGIN; INSERT ...; DELETE ...; COMMIT` as the
/// Control Plane plans it. The INSERT keeps its resolution and defers the
/// target. The DELETE's balance was settled from its stored row, so its join
/// value is gone from its resolution.
fn source_slice() -> Vec<PhysicalPlan> {
    let collection = QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE);
    vec![
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: collection.clone(),
            document_id: "e1".to_string(),
            value: doc_format::encode_to_msgpack(&entry(4)),
            if_absent: false,
            surrogate: INSERTED,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: vec![ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
            deferred_sum_targets: vec![TARGET.to_string()],
        }),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id: "e0".to_string(),
            surrogate: Some(DELETED),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    ]
}

/// The premise: the target is cross-shard, so its balance moves only on its
/// own task.
#[test]
fn the_fixture_target_is_cross_shard() {
    assert!(
        !crate::query::sum_target_is_co_resident(
            CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE),
            TARGET,
        ),
        "'{TARGET}' must not share '{SOURCE}''s vShard"
    );
}

/// The source slice of an INSERT and a DELETE on one account installs both
/// rows and leaves the cross-shard balance to the shipped tasks.
#[test]
fn a_delete_next_to_an_insert_of_its_account_folds_no_cross_shard_balance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _ends) = seeded_core(dir.path());

    let response = core.commit_plans_for_test(&make_default_task(), TID, &source_slice(), 30);

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert!(row(&core, SOURCE, DELETED).is_none(), "e0 is deleted");
    assert!(row(&core, SOURCE, INSERTED).is_some(), "e1 is inserted");
    assert_eq!(
        row(&core, TARGET, TARGET_SURROGATE)
            .as_ref()
            .and_then(|doc| doc.get("balance"))
            .and_then(|balance| balance.as_str()),
        Some("10"),
        "the source install moves no cross-shard balance; the shipped tasks move it once"
    );
}

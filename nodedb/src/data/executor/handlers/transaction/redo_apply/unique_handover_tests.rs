// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE on a committed redo record is judged on the record's post-state.
//!
//! The install writes row by row. A handover inside one record lands in
//! either write order, and a record whose post-state gives one value two
//! owners writes nothing.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, TenantId, Value,
};

use super::entry::CommittedRedo;
use super::test_commit::doc_put_sub_record;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::engine::document::store::{CollectionConfig, DocumentEngine, IndexPath};
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

const TID: u64 = 1;
const COLL: &str = "codes";

/// A core whose `codes` collection declares a UNIQUE index on `code`, with
/// its bridge ends kept alive.
fn unique_core(
    dir: &std::path::Path,
) -> (CoreLoop, Box<dyn std::any::Any>, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    let mut config = CollectionConfig::new(COLL);
    config.index_paths.push(IndexPath {
        unique: true,
        ..IndexPath::new("code")
    });
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
        config,
    );
    (core, Box::new(req), Box::new(resp))
}

fn body(code: &str) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert("code".to_string(), Value::String(code.to_string()));
    // A document body is a standard MessagePack map, as the write path stores
    // it. `zerompk::to_msgpack_vec` on `Value` writes the tagged form, which
    // no document decoder reads.
    nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode body")
}

fn put(surrogate: u32, code: &str) -> RedoSubRecord {
    doc_put_sub_record(COLL, &format!("r{surrogate}"), &body(code), surrogate)
}

fn install(core: &mut CoreLoop, lsn: u64, ops: Vec<RedoSubRecord>) -> Response {
    let redo = RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    }
    .to_bytes()
    .expect("encode redo");
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(lsn));
    core.execute_apply_transaction_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &[COLL.to_string()],
            sum_targets: &[],
        },
    )
}

/// The surrogates the unique index names for `code`.
fn owners(core: &CoreLoop, code: &str) -> Vec<u32> {
    let mut owners: Vec<u32> = DocumentEngine::new(&core.sparse, 0, TID)
        .index_lookup(COLL, "code", code, None)
        .expect("index lookup")
        .iter()
        .map(|key| key.surrogate().as_u32())
        .collect();
    owners.sort_unstable();
    owners
}

fn assert_ok(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

fn assert_unique_violation(response: &Response) {
    assert_eq!(response.status, Status::Error);
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ),
        "expected a unique violation, got {:?}",
        response.error_code
    );
}

fn seeded(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = unique_core(dir);
    assert_ok(&install(&mut core, 10, vec![put(1, "A"), put(2, "B")]));
    (core, req, resp)
}

#[test]
fn a_swap_in_one_record_installs_in_either_write_order() {
    for row_1_first in [true, false] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = seeded(dir.path());
        // The swap's sub-records are built after the seed, so their stamps
        // sit above the seed's, as a later commit's stamps do.
        let ops = if row_1_first {
            vec![put(1, "B"), put(2, "A")]
        } else {
            vec![put(2, "A"), put(1, "B")]
        };
        assert_ok(&install(&mut core, 20, ops));
        assert_eq!(owners(&core, "A"), vec![2]);
        assert_eq!(owners(&core, "B"), vec![1]);
    }
}

#[test]
fn a_claim_written_before_the_release_installs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = seeded(dir.path());
    assert_ok(&install(&mut core, 20, vec![put(3, "A"), put(1, "X")]));
    assert_eq!(owners(&core, "A"), vec![3]);
    assert_eq!(owners(&core, "X"), vec![1]);
}

#[test]
fn two_rows_of_one_record_claiming_one_value_write_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = seeded(dir.path());
    assert_unique_violation(&install(&mut core, 20, vec![put(3, "Z"), put(4, "Z")]));
    assert!(
        owners(&core, "Z").is_empty(),
        "the refused record writes no row"
    );
}

#[test]
fn a_value_an_untouched_row_holds_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = seeded(dir.path());
    assert_unique_violation(&install(&mut core, 20, vec![put(3, "B")]));
    assert_eq!(owners(&core, "B"), vec![2]);
}

/// A statement staged inside a transaction sees the rows earlier statements
/// released: row 1's delete frees `A` for the insert after it.
#[test]
fn a_staged_insert_claims_a_value_the_transaction_released() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = seeded(dir.path());
    let collection = QualifiedCollection::new(DatabaseId::DEFAULT, COLL);
    let plans = [
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: collection.clone(),
            document_id: "r1".to_string(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id: "r3".to_string(),
            value: body("A"),
            if_absent: false,
            surrogate: Surrogate::new(3),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }),
    ];
    let task = make_default_task();
    assert_ok(&core.commit_plans_for_test(&task, TID, &plans, 30));
    assert_eq!(owners(&core, "A"), vec![3]);
    let row1 = core
        .sparse
        .get(0, TID, COLL, &StorageKey::for_surrogate(Surrogate::new(1)))
        .expect("read row");
    assert!(row1.is_none(), "row 1 is deleted");
}

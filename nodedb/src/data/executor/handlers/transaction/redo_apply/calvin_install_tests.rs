// SPDX-License-Identifier: BUSL-1.1

//! A committed Calvin slice installs from its stamped redo entry: the
//! `ApplyTransactionRedo` plan with a `CalvinInstall`.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, CalvinResolved, DocumentOp, MetaOp, PhysicalPlan, RedoOrigin,
    RedoSumTargets, ReturningColumns, ReturningSpec,
};
use nodedb_types::{QualifiedCollection, StorageKey, Surrogate};

use super::calvin_fold_tests::{TARGET, TID, balance, calvin_redo, core_with_sum, sum_targets};
use super::test_commit::failing_install_sub_record;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
use crate::data::executor::handlers::control::calvin::test_support::{
    bulk_delete_plan, point_insert_plan, seed_row,
};
use crate::data::executor::handlers::control::calvin_reply::CalvinReply;
use crate::data::executor::handlers::control::calvin_synthetic_txn_id;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::types::{DatabaseId, Lsn, TenantId};
use crate::wal::RedoRecord;

const ORDERS: &str = "orders";
/// The vShard `make_default_task` homes to.
const VSHARD: u32 = 0;

fn doc_body(field: &str, value: &str) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert(
        field.to_string(),
        nodedb_types::Value::String(value.to_string()),
    );
    nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).expect("encode body")
}

/// An insert of row `surrogate` that asks for its row back.
fn returning_insert(document_id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ORDERS),
        document_id: document_id.to_string(),
        value: doc_body("a", "returned"),
        if_absent: false,
        surrogate: Surrogate::new(surrogate),
        returning: Some(ReturningSpec {
            columns: ReturningColumns::Star,
        }),
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

/// Stage `plans` as the leader does at `(1, 0)` and resolve them.
fn stage_and_resolve(core: &mut CoreLoop, plans: &[PhysicalPlan]) -> CalvinResolved {
    let task = make_default_task();
    let ctx = CalvinExecCtx {
        epoch: 1,
        position: 0,
        epoch_system_ms: 0,
    };
    let staged =
        core.execute_calvin_execute_static(&task, ctx, &TenantId::new(TID), plans, &[], &[]);
    assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
    let resolved = core.execute_calvin_resolve(&task, 1, 0);
    assert_eq!(resolved.status, Status::Ok, "{:?}", resolved.error_code);
    zerompk::from_msgpack(resolved.payload.as_bytes()).expect("decode resolved answer")
}

/// The stamped install of `redo` as the slice at `(epoch, 0)`.
fn install_plan(
    redo: Vec<u8>,
    collections: Vec<String>,
    sum_targets: Vec<RedoSumTargets>,
    epoch: u64,
    reply: CalvinReplySpec,
) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo,
        collections,
        sum_targets,
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch,
            position: 0,
            epoch_system_ms: 0,
            reply,
            user_write: true,
        }),
    })
}

/// Run `plan` on `core` with its record at `lsn`, through the plan
/// dispatcher.
fn run_at(core: &mut CoreLoop, plan: &PhysicalPlan, lsn: u64) -> Response {
    let mut task: ExecutionTask = make_default_task();
    task.wal_lsn = Some(Lsn::new(lsn));
    core.execute_plan(&task, plan)
}

fn base_row(core: &CoreLoop, surrogate: u32) -> Option<Vec<u8>> {
    core.sparse
        .get(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            ORDERS,
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read base row")
}

fn holds(payload: &[u8], needle: &[u8]) -> bool {
    payload.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn a_calvin_install_consumes_its_staged_entry_and_renders_the_reply() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let resolved = stage_and_resolve(&mut core, &[returning_insert("o9", 9)]);
    let synthetic = calvin_synthetic_txn_id(1, 0, VSHARD).expect("synthetic id");
    assert!(core.calvin.commit_pending.contains_key(&(1, 0, VSHARD)));
    assert!(core.txn_overlays.contains_key(&synthetic));

    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let installed = run_at(&mut core, &plan, 42);

    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    assert!(installed.error_code.is_none(), "{:?}", installed.error_code);
    assert!(
        !core.calvin.commit_pending.contains_key(&(1, 0, VSHARD)),
        "the install consumes the staged entry"
    );
    assert!(
        !core.txn_overlays.contains_key(&synthetic),
        "the install drops the synthetic overlay"
    );
    assert!(base_row(&core, 9).is_some(), "the install writes the row");
    assert!(
        holds(installed.payload.as_bytes(), b"returned"),
        "the reply carries the RETURNING row"
    );
}

#[test]
fn a_calvin_install_without_a_staged_entry_renders_the_same_reply() {
    let leader_dir = tempfile::tempdir().expect("tempdir");
    let (mut leader, _ltx, _lrx) = make_core_with_dir(leader_dir.path());
    let resolved = stage_and_resolve(&mut leader, &[returning_insert("o9", 9)]);
    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let on_leader = run_at(&mut leader, &plan, 42);
    assert_eq!(on_leader.status, Status::Ok, "{:?}", on_leader.error_code);

    // A replica that staged nothing installs the same entry.
    let replica_dir = tempfile::tempdir().expect("tempdir");
    let (mut replica, _rtx, _rrx) = make_core_with_dir(replica_dir.path());
    assert!(replica.calvin.commit_pending.is_empty());
    let on_replica = run_at(&mut replica, &plan, 42);

    assert_eq!(on_replica.status, Status::Ok, "{:?}", on_replica.error_code);
    assert!(
        on_replica.error_code.is_none(),
        "{:?}",
        on_replica.error_code
    );
    assert!(
        base_row(&replica, 9).is_some(),
        "the replica writes the row"
    );
    assert!(holds(on_replica.payload.as_bytes(), b"returned"));
    assert_eq!(
        on_replica.payload.as_bytes(),
        on_leader.payload.as_bytes(),
        "every replica answers the same reply"
    );
}

#[test]
fn a_calvin_install_keeps_fold_rows_in_the_write_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _ends) = core_with_sum(dir.path());
    let plan = install_plan(
        calvin_redo(),
        vec![super::calvin_fold_tests::SOURCE.to_string()],
        sum_targets(),
        3,
        CalvinReplySpec::Count(Vec::new()),
    );

    let installed = run_at(&mut core, &plan, 70);

    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    assert_eq!(balance(&core).as_deref(), Some("125"));
    assert!(
        installed
            .write_set
            .iter()
            .any(|entry| entry.collection.as_deref() == Some(TARGET)),
        "the folded target row stays in the write set for the funnel to journal: {:?}",
        installed.write_set
    );
}

/// The install applies the resolved record, not a replay of the staged
/// plans. A row that joins the bulk delete's predicate after the stage
/// stays, and the predicted row goes.
#[test]
fn a_calvin_install_applies_the_resolved_record_not_a_replay_of_the_plans() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    seed_row(&mut core, ORDERS, 1);
    let resolved = stage_and_resolve(&mut core, &[bulk_delete_plan(ORDERS, Some(vec![1]))]);
    seed_row(&mut core, ORDERS, 2);

    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let installed = run_at(&mut core, &plan, 41);

    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    assert!(base_row(&core, 1).is_none());
    assert!(base_row(&core, 2).is_some());
}

/// The install notes the index values of every document row at the
/// record's LSN.
#[test]
fn a_calvin_install_notes_index_values_at_the_redo_lsn() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut config = CollectionConfig::new(ORDERS);
    config.index_paths.push(IndexPath::new("a"));
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), ORDERS.to_string()),
        config,
    );
    let resolved = stage_and_resolve(&mut core, &[point_insert_plan(ORDERS, "o1", 7)]);

    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let installed = run_at(&mut core, &plan, 43);

    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    assert_eq!(
        core.write_index.index_values.value_version(
            crate::data::executor::core_loop::index_value_versions::IndexDimRef {
                vshard: crate::types::VShardId::new(0),
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(TID),
                collection: ORDERS,
                field: "a",
            },
            "1"
        ),
        Some(crate::data::executor::core_loop::write_index::tests::local(
            43
        )),
        "the install records the row's index value at the redo's version"
    );
}

/// An install whose record cannot decode answers an error and leaves
/// nothing of the record in base.
#[test]
fn a_calvin_install_whose_record_cannot_decode_answers_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let resolved = stage_and_resolve(&mut core, &[point_insert_plan(ORDERS, "o1", 7)]);

    let plan = install_plan(
        vec![0xff, 0x00],
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let installed = run_at(&mut core, &plan, 44);

    assert_eq!(installed.status, Status::Error);
    assert!(!matches!(
        installed.error_code.as_deref(),
        Some(ErrorCode::OllpRetryRequired)
    ));
    assert!(base_row(&core, 7).is_none());
}

/// A refused install keeps the staged entry and its overlay, so a later
/// install of the record consumes them.
#[test]
fn a_refused_calvin_install_keeps_the_staged_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let resolved = stage_and_resolve(&mut core, &[point_insert_plan(ORDERS, "o1", 7)]);
    let mut failing = RedoRecord::from_bytes(&resolved.redo).expect("decode redo");
    failing.ops.push(failing_install_sub_record());
    let failing = failing.to_bytes().expect("encode redo");
    let synthetic = calvin_synthetic_txn_id(1, 0, VSHARD).expect("synthetic id");

    let refused_plan = install_plan(
        failing,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply.clone(),
    );
    let refused = run_at(&mut core, &refused_plan, 45);

    assert!(
        matches!(
            refused.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "{:?}",
        refused.error_code
    );
    assert!(core.calvin.commit_pending.contains_key(&(1, 0, VSHARD)));
    assert!(core.txn_overlays.contains_key(&synthetic));
    assert!(base_row(&core, 7).is_none());

    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        resolved.reply,
    );
    let installed = run_at(&mut core, &plan, 46);

    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    assert!(base_row(&core, 7).is_some());
    assert!(!core.calvin.commit_pending.contains_key(&(1, 0, VSHARD)));
    assert!(!core.txn_overlays.contains_key(&synthetic));
}

/// A reply that fails to render after a successful install leaves the
/// install `Ok` and carries the render error for the statement.
#[test]
fn a_reply_render_error_leaves_the_calvin_install_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let resolved = stage_and_resolve(&mut core, &[point_insert_plan(ORDERS, "o1", 7)]);
    let unrenderable = CalvinReplySpec::from(&CalvinReply::unrenderable_for_test(ORDERS));

    let plan = install_plan(
        resolved.redo,
        vec![ORDERS.to_string()],
        Vec::new(),
        1,
        unrenderable,
    );
    let installed = run_at(&mut core, &plan, 47);

    assert_eq!(installed.status, Status::Ok);
    assert!(
        matches!(
            installed.error_code.as_deref(),
            Some(ErrorCode::Internal { .. })
        ),
        "{:?}",
        installed.error_code
    );
    assert!(base_row(&core, 7).is_some());
    assert!(!core.calvin.commit_pending.contains_key(&(1, 0, VSHARD)));
}

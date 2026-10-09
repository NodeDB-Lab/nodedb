// SPDX-License-Identifier: BUSL-1.1

//! A committed Calvin slice installs from its stamped redo entry: the
//! `ApplyTransactionRedo` plan with a `CalvinInstall`.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, CalvinResolved, DocumentOp, MetaOp, PhysicalPlan, RedoOrigin,
    RedoSumTargets, ReturningColumns, ReturningSpec,
};
use nodedb_types::{QualifiedCollection, StorageKey, Surrogate};

use super::calvin_fold_tests::{TARGET, TID, balance, calvin_redo, core_with_sum, sum_targets};
use crate::bridge::envelope::{Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
use crate::data::executor::handlers::control::calvin_synthetic_txn_id;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn, TenantId};

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

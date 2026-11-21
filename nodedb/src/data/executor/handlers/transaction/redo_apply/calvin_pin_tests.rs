// SPDX-License-Identifier: BUSL-1.1

//! A Calvin stage reads its collection configs under the slice's pins.
//!
//! The proposer saw `tickets` schemaless with no type guard. A replica
//! applied a later typeguard VALUE `lowered = LOWER(status)`. The stage
//! request carries the proposer's pins, so the replica stages and installs
//! the row the proposer planned.

use std::sync::Arc;

use nodedb_physical::physical_plan::{
    CalvinInstall, DocumentOp, MetaOp, PhysicalPlan, RedoOrigin, RedoPins, written_collections,
};
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId, Value};

use super::install_pin_tests::{
    COLL, assert_ok, core_with, document, field, guarded_wire, pinned, register, schemaless_wire,
};
use super::judged_tests::pins;
use super::stream_validate::tests::{Hold, TID, hold, plain_core};
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_request;
use crate::data::executor::handlers::control::calvin::{CalvinExecCtx, CalvinSnapshot};
use crate::data::executor::handlers::control::calvin_synthetic_txn_id;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

/// The epoch every stage here runs at, at position 0.
const EPOCH: u64 = 1;
/// The vShard `make_request` homes to.
const VSHARD: u32 = 0;
/// The LSN the slice's redo installs at.
const LSN: u64 = 10;

/// An insert of row `t1` with `status = OPEN`.
fn insert() -> PhysicalPlan {
    let object = [("status".to_string(), Value::String("OPEN".into()))]
        .into_iter()
        .collect();
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
        document_id: "t1".into(),
        value: nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body"),
        if_absent: false,
        surrogate: Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// The static stage task of `plans`, carrying `pins` as the scheduler sends
/// them.
fn stage_task(plans: Vec<PhysicalPlan>, pins: Option<&RedoPins>) -> ExecutionTask {
    let mut request = make_request(PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
        epoch: EPOCH,
        position: 0,
        tenant_id: TenantId::new(TID),
        plans,
        epoch_system_ms: 0,
        versioned_reads: Vec::new(),
        body_plans: Vec::new(),
        snapshot_ordinal: None,
    }));
    request.config_pins = pins.cloned().map(Arc::new);
    ExecutionTask::new(request)
}

/// Stage `insert` on `core` through the plan dispatcher, with `pins`.
fn stage(core: &mut CoreLoop, pins: Option<&RedoPins>) -> Response {
    core.execute(&stage_task(vec![insert()], pins))
}

/// The body of row `t1` the stage left in the slice's overlay. `None` when
/// nothing staged.
fn staged_body(core: &CoreLoop) -> Option<Vec<u8>> {
    let txn_id = calvin_synthetic_txn_id(EPOCH, 0, VSHARD).expect("synthetic id");
    let coll_key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string());
    match core
        .staged_row(txn_id, &coll_key, 1)
        .expect("read staged row")
    {
        Some(Staged::Put(body)) => Some(body),
        Some(Staged::Tombstone) => panic!("the stage put row t1"),
        None => None,
    }
}

/// The row `t1` the stage left in the slice's overlay, decoded.
fn staged(core: &CoreLoop) -> Option<Value> {
    staged_body(core).map(|body| {
        crate::data::executor::doc_format::decode_document_value(&body).expect("decode staged row")
    })
}

/// The staged body bytes of row `t1`.
fn staged_bytes(core: &CoreLoop) -> Vec<u8> {
    staged_body(core).expect("the stage put row t1")
}

/// Resolve the staged slice under `pins` and install its redo at `LSN`,
/// pinned with `pins`, as every replica installs the slice's entry.
fn resolve_and_install(core: &mut CoreLoop, pins: &RedoPins) -> Response {
    let task = stage_task(vec![insert()], Some(pins));
    let held = core.begin_pinned_apply(pins).expect("open the view");
    let resolved = core.calvin_resolve_whole(&task, EPOCH, 0);
    core.restore_judging(held);
    let answer = resolved.expect("resolve the staged slice");
    let plans = [insert()];
    let install = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: answer.redo,
        collections: written_collections(&plans),
        sum_targets: Vec::new(),
        origin: RedoOrigin::of_plans(&plans),
        calvin: Some(CalvinInstall {
            epoch: EPOCH,
            position: 0,
            epoch_system_ms: 0,
            reply: answer.reply,
            user_write: true,
            mirror: false,
        }),
        reads: Vec::new(),
        snapshot_ordinal: None,
        pins: pins.clone(),
    });
    // The install opens its own view from the entry's pins.
    let mut request = task.request.clone();
    request.config_pins = None;
    request.wal_lsn = Some(Lsn::new(LSN));
    core.execute_plan(&ExecutionTask::new(request), &install)
}

/// A replica whose current config adds a typeguard VALUE stages and
/// installs the proposer's row under the slice's pins: no `lowered` field.
/// Without pins the same stage derives `lowered`, so the pins decide it.
#[test]
fn a_pinned_stage_derives_the_row_under_the_proposer_config() {
    let pins = pinned(&schemaless_wire());

    let mut replica = core_with(&guarded_wire());
    assert_ok(&stage(&mut replica.core, Some(&pins)));
    let row = staged(&replica.core).expect("the stage put t1");
    assert_eq!(field(&row, "status"), Some(&Value::String("OPEN".into())));
    assert_eq!(field(&row, "lowered"), None);
    assert_ok(&resolve_and_install(&mut replica.core, &pins));
    let installed = document(&replica.core, 1);
    assert_eq!(field(&installed, "lowered"), None);

    let mut unpinned = core_with(&guarded_wire());
    assert_ok(&stage(&mut unpinned.core, None));
    let row = staged(&unpinned.core).expect("the stage put t1");
    assert_eq!(field(&row, "lowered"), Some(&Value::String("open".into())));
}

/// The bytes `core` stages for `insert` under `pins`, beside how many
/// configs the view decoded while it was open.
fn stage_under_view(core: &mut CoreLoop, pins: &RedoPins) -> (Vec<u8>, usize) {
    let task = stage_task(vec![insert()], Some(pins));
    let held = core.begin_pinned_apply(pins).expect("open the view");
    let response = core.execute_calvin_execute_static(
        &task,
        CalvinExecCtx {
            epoch: EPOCH,
            position: 0,
            epoch_system_ms: 0,
        },
        &TenantId::new(TID),
        &[insert()],
        CalvinSnapshot::NONE,
        &[],
    );
    let decoded = core
        .apply_scope
        .judged
        .as_ref()
        .map_or(0, |view| view.decoded_configs() + view.installed_configs());
    core.restore_judging(held);
    assert_ok(&response);
    (staged_bytes(core), decoded)
}

/// A core whose current config of `tickets` is the schemaless wire with
/// digest 0, which no pin matches.
fn undigested_core() -> Hold {
    let mut held = hold(plain_core);
    register(&mut held.core, &schemaless_wire(), 0);
    held
}

/// A pin whose digest equals the core's config reads the current config
/// and decodes nothing. A core with a 0 digest decodes the same pin. Both
/// stage the same bytes.
#[test]
fn a_matching_digest_takes_the_fast_path_and_stages_the_decoded_row() {
    let pins = pinned(&schemaless_wire());

    let mut matching = core_with(&schemaless_wire());
    let (fast, fast_decoded) = stage_under_view(&mut matching.core, &pins);
    assert_eq!(fast_decoded, 0, "a matching digest decodes no pin");

    let mut other = undigested_core();
    let (slow, slow_decoded) = stage_under_view(&mut other.core, &pins);
    assert!(slow_decoded > 0, "a 0 digest decodes the pin");

    assert_eq!(fast, slow);
}

/// A stage whose pins hold no pin of the collection it writes is refused
/// before it stages anything, on every replica alike.
#[test]
fn a_stage_whose_pins_lack_a_written_config_is_refused() {
    let unpinned = pins(Vec::new());
    for wire in [schemaless_wire(), guarded_wire()] {
        let mut held = core_with(&wire);
        let response = stage(&mut held.core, Some(&unpinned));
        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedPrevalidation { reason })
                    if reason.contains("carries no config pin of")
            ),
            "{:?}",
            response.error_code
        );
        assert_eq!(staged(&held.core), None, "a refused stage stages nothing");
    }
}

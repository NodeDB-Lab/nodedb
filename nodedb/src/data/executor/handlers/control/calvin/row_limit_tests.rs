// SPDX-License-Identifier: BUSL-1.1

//! A Calvin write whose row image is over the row image limit refuses at
//! stage, so its slice votes abort with SQLSTATE 54000 and nothing stages.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, PhysicalPlan};
use nodedb_types::{
    DatabaseId, MAX_ROW_IMAGE_BYTES, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId, Value,
};

use super::shared::CalvinExecCtx;
use super::shared::test_support::make_task;
use super::snapshot_vote::CalvinSnapshot;
use crate::bridge::envelope::{ErrorCode, Response, StageVote, Status};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;

const EPOCH: u64 = 41;

fn stage(core: &mut CoreLoop, plans: &[PhysicalPlan]) -> Response {
    core.execute_calvin_execute_static(
        &make_task(),
        CalvinExecCtx {
            epoch: EPOCH,
            position: 0,
            epoch_system_ms: 0,
        },
        &TenantId::new(1),
        plans,
        CalvinSnapshot::NONE,
        &[],
    )
}

fn large_insert(len: usize) -> PhysicalPlan {
    let mut object = HashMap::new();
    object.insert("note".to_string(), Value::String("x".repeat(len)));
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
        document_id: "o1".into(),
        value: nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode"),
        if_absent: false,
        surrogate: Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

fn large_edge(len: usize) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "people"),
        src_id: "a".into(),
        label: "knows".into(),
        dst_id: "b".into(),
        properties: vec![0x80; len],
        src_surrogate: Surrogate::new(1),
        dst_surrogate: Surrogate::new(2),
        valid_from_ms: None,
        valid_until_ms: None,
    })
}

fn large_kv_put(len: usize) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
        key: b"k".to_vec(),
        value: vec![0u8; len],
        ttl_ms: 0,
        surrogate: Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

/// The slice refuses with SQLSTATE 54000, votes abort, and keeps nothing
/// staged.
fn assert_aborts_at_the_limit(core: &CoreLoop, response: &Response) {
    assert_eq!(response.status, Status::Error, "{response:?}");
    let code = response.error_code.as_deref().expect("error code");
    assert!(
        matches!(code, ErrorCode::ProgramLimitExceeded { .. }),
        "{code:?}"
    );
    assert_eq!(error_code_to_sqlstate(code).1, "54000");
    assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
    let vshard = make_task().request.vshard_id.as_u32();
    assert!(!core.calvin.commit_pending.contains_key(&(EPOCH, 0, vshard)));
    let synthetic = calvin_synthetic_txn_id(EPOCH, 0, vshard).expect("synthetic id");
    assert!(!core.txn_overlays.contains_key(&synthetic));
}

#[test]
fn a_calvin_document_write_over_the_limit_votes_abort() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let response = stage(&mut core, &[large_insert(MAX_ROW_IMAGE_BYTES + 1)]);
    assert_aborts_at_the_limit(&core, &response);
}

#[test]
fn a_calvin_edge_with_properties_over_the_limit_votes_abort() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let response = stage(&mut core, &[large_edge(MAX_ROW_IMAGE_BYTES + 1)]);
    assert_aborts_at_the_limit(&core, &response);
}

#[test]
fn a_calvin_kv_write_over_the_limit_votes_abort() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    // One key byte makes the row one byte over the limit.
    let response = stage(&mut core, &[large_kv_put(MAX_ROW_IMAGE_BYTES)]);
    assert_aborts_at_the_limit(&core, &response);
}

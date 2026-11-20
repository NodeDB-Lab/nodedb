// SPDX-License-Identifier: BUSL-1.1

//! A plain replicated document write derives its rows and judges its rules
//! under its proposer's pins.
//!
//! Two cores hold different current configs of `tickets`. One holds the
//! proposer's config. The other applied a later DDL: a typeguard VALUE, or
//! a GENERATED column `lowered = LOWER(status)`. Each runs the same task,
//! pinned with the proposer's config, and both store the same bytes.

use std::sync::Arc;

use nodedb_physical::physical_plan::{
    DocConfigWire, DocumentOp, PhysicalPlan, RedoPins, ResolvedSumTarget, RowSource, UpdateValue,
};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, SystemTimeScope, Value,
};

use super::install_pin_tests::{
    COLL, assert_ok, core_with, document, field, generated_wire, guarded_wire, pinned,
    schemaless_wire, stored, strict_v1, strict_wire,
};
use super::stream_validate::tests::TID;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_request;
use crate::data::executor::task::ExecutionTask;

/// Each proposer config, beside the config a replica holds after a later
/// DDL.
fn config_pairs() -> [(DocConfigWire, DocConfigWire); 2] {
    [
        (schemaless_wire(), guarded_wire()),
        (strict_wire(strict_v1()), generated_wire()),
    ]
}

pub(super) fn collection() -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, COLL)
}

fn body(status: &str) -> Vec<u8> {
    let object = [("status".to_string(), Value::String(status.into()))]
        .into_iter()
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body")
}

fn point_put(status: &str) -> DocumentOp {
    DocumentOp::PointPut {
        collection: collection(),
        document_id: "t1".into(),
        value: body(status),
        surrogate: Surrogate::new(1),
        pk_bytes: b"t1".to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    }
}

fn batch_insert(status: &str) -> DocumentOp {
    DocumentOp::BatchInsert {
        collection: collection(),
        documents: vec![("t1".into(), body(status))],
        if_absent: false,
        surrogates: vec![Surrogate::new(1)],
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    }
}

fn upsert(status: &str) -> DocumentOp {
    DocumentOp::Upsert {
        collection: collection(),
        document_id: "t1".into(),
        value: body(status),
        on_conflict_updates: Vec::new(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

/// The assignment `status = <status>`.
fn status_literal(status: &str) -> UpdateValue {
    UpdateValue::Literal(
        nodedb_types::value_to_msgpack(&Value::String(status.into())).expect("encode literal"),
    )
}

pub(super) fn point_update(status: &str) -> DocumentOp {
    DocumentOp::PointUpdate {
        collection: collection(),
        document_id: "t1".into(),
        surrogate: Some(Surrogate::new(1)),
        pk_bytes: b"t1".to_vec(),
        updates: vec![("status".into(), status_literal(status))],
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    }
}

/// Run `op` on `core` as a replicated write that carries `pins`.
pub(super) fn run(core: &mut CoreLoop, op: DocumentOp, pins: Option<&RedoPins>) -> Response {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.config_pins = pins.cloned().map(Arc::new);
    core.execute(&ExecutionTask::new(request))
}

/// The bytes row `t1` holds on a proposer core and on a later-DDL core
/// after each runs `ops` in order, every one carrying `pins`.
fn stored_on_both(
    proposer: &DocConfigWire,
    other: &DocConfigWire,
    ops: &[DocumentOp],
    pins: Option<&RedoPins>,
) -> (Vec<u8>, Vec<u8>) {
    let mut on_proposer = core_with(proposer);
    let mut on_other = core_with(other);
    for op in ops {
        assert_ok(&run(&mut on_proposer.core, op.clone(), pins));
        assert_ok(&run(&mut on_other.core, op.clone(), pins));
    }
    (stored(&on_proposer.core, 1), stored(&on_other.core, 1))
}

/// Assert both cores store the same row for `ops` pinned with the
/// proposer's config, and that the unpinned run of the same `ops` does
/// not: so the pins are what makes the rows equal.
fn assert_pins_fix_the_row(ops: &[DocumentOp]) {
    for (proposer, other) in config_pairs() {
        let pins = pinned(&proposer);
        let (expected, replica) = stored_on_both(&proposer, &other, ops, Some(&pins));
        assert_eq!(replica, expected, "{other:?}");
        let (expected, replica) = stored_on_both(&proposer, &other, ops, None);
        assert_ne!(replica, expected, "an unpinned run derives under {other:?}");
    }
}

#[test]
fn a_pinned_point_put_stores_the_proposer_row_on_every_replica() {
    assert_pins_fix_the_row(&[point_put("OPEN")]);
}

#[test]
fn a_pinned_batch_insert_stores_the_proposer_row_on_every_replica() {
    assert_pins_fix_the_row(&[batch_insert("OPEN")]);
}

#[test]
fn a_pinned_upsert_stores_the_proposer_row_on_every_replica() {
    assert_pins_fix_the_row(&[upsert("OPEN")]);
}

pub(super) fn bulk_update(status: &str) -> DocumentOp {
    DocumentOp::BulkUpdate {
        collection: collection(),
        filters: Vec::new(),
        updates: vec![("status".into(), status_literal(status))],
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
        system_time: SystemTimeScope::Current,
    }
}

/// An `UPDATE ... FROM` whose shipped source row joins `t1` on `status`.
pub(super) fn update_from_join(from: &str, status: &str) -> DocumentOp {
    let source = serde_json::json!({ "status": from });
    DocumentOp::UpdateFromJoin {
        target_collection: collection(),
        source: RowSource::whole_collection(QualifiedCollection::new(
            DatabaseId::DEFAULT,
            "changes",
        )),
        source_alias: "s".into(),
        target_join_col: "status".into(),
        source_join_col: "status".into(),
        updates: vec![("status".into(), status_literal(status))],
        target_filters: Vec::new(),
        returning: None,
        source_rows: Some(vec![crate::data::executor::doc_format::encode_to_msgpack(
            &source,
        )]),
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
        target_system_time: SystemTimeScope::Current,
    }
}

/// Assert both cores store the same row once `t1` is put and then
/// updated by `update`, every write pinned with the proposer's config. The
/// guarded replica derives no `lowered` field of its own.
fn assert_pinned_update_matches(update: DocumentOp) {
    let ops = [point_put("OPEN"), update];
    for (proposer, other) in config_pairs() {
        let pins = pinned(&proposer);
        let (expected, replica) = stored_on_both(&proposer, &other, &ops, Some(&pins));
        assert_eq!(replica, expected, "{other:?}");
    }
    let pins = pinned(&schemaless_wire());
    let mut on_guarded = core_with(&guarded_wire());
    for op in ops {
        assert_ok(&run(&mut on_guarded.core, op, Some(&pins)));
    }
    let row = document(&on_guarded.core, 1);
    assert_eq!(field(&row, "status"), Some(&Value::String("CLOSED".into())));
    assert_eq!(field(&row, "lowered"), None);
}

#[test]
fn a_pinned_point_update_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(point_update("CLOSED"));
}

#[test]
fn a_pinned_bulk_update_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(bulk_update("CLOSED"));
}

#[test]
fn a_pinned_update_from_join_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(update_from_join("OPEN", "CLOSED"));
}

/// An upsert of `t1` whose conflict branch sets `status`.
pub(super) fn upsert_on_conflict(status: &str) -> DocumentOp {
    DocumentOp::Upsert {
        collection: collection(),
        document_id: "t1".into(),
        value: body("OPEN"),
        on_conflict_updates: vec![("status".into(), status_literal(status))],
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

/// A one-row upsert page of `t1` whose conflict branch sets `status`.
pub(super) fn batch_upsert_on_conflict(status: &str) -> DocumentOp {
    DocumentOp::BatchUpsert {
        collection: collection(),
        documents: vec![("t1".into(), body("OPEN"))],
        surrogates: vec![Surrogate::new(1)],
        on_conflict_updates: vec![("status".into(), status_literal(status))],
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

/// A MERGE whose shipped source row matches `t1` on `status` and whose
/// MATCHED arm sets `status`.
pub(super) fn merge_matched(from: &str, status: &str) -> DocumentOp {
    use nodedb_physical::physical_plan::document::merge_types::{
        MergeActionOp, MergeClauseKind, MergeClauseOp,
    };
    let source = serde_json::json!({ "status": from });
    DocumentOp::Merge {
        target_collection: collection(),
        source: RowSource::whole_collection(QualifiedCollection::new(
            DatabaseId::DEFAULT,
            "changes",
        )),
        source_alias: "s".into(),
        target_join_col: "status".into(),
        source_join_col: "status".into(),
        clauses: vec![MergeClauseOp {
            kind: MergeClauseKind::Matched,
            extra_predicate: Vec::new(),
            action: MergeActionOp::Update {
                updates: vec![("status".into(), status_literal(status))],
            },
        }],
        returning: None,
        resolved_inserts: Some(Vec::new()),
        resolved_insert_identities: Vec::new(),
        source_rows: Some(vec![crate::data::executor::doc_format::encode_to_msgpack(
            &source,
        )]),
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
        target_system_time: SystemTimeScope::Current,
    }
}

#[test]
fn a_pinned_upsert_conflict_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(upsert_on_conflict("CLOSED"));
}

#[test]
fn a_pinned_batch_upsert_conflict_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(batch_upsert_on_conflict("CLOSED"));
}

#[test]
fn a_pinned_merge_update_stores_the_proposer_row_on_every_replica() {
    assert_pinned_update_matches(merge_matched("OPEN", "CLOSED"));
}

/// Whether `core` holds no row `t1`.
fn holds_no_row(core: &CoreLoop) -> bool {
    core.sparse
        .get(0, TID, COLL, &StorageKey::for_surrogate(Surrogate::new(1)))
        .expect("read row")
        .is_none()
}

/// Assert `pins` refuse a point put before it stores anything, and leave
/// no judged view open.
fn assert_refused(op: DocumentOp, pins: &RedoPins) {
    let mut held = core_with(&guarded_wire());
    let response = run(&mut held.core, op, Some(pins));
    assert_eq!(response.status, Status::Error);
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ),
        "{:?}",
        response.error_code
    );
    assert!(holds_no_row(&held.core));
    assert!(held.core.apply_scope.judged.is_none());
    assert!(!held.core.apply_scope.judging);
}

#[test]
fn a_pin_whose_digest_does_not_match_refuses_the_write() {
    let mut pins = pinned(&schemaless_wire());
    let mut pin = (*pins.configs[0]).clone();
    pin.digest = pin.digest.wrapping_add(1);
    pins.configs[0] = Arc::new(pin);
    assert_refused(point_put("OPEN"), &pins);
}

#[test]
fn a_pin_whose_bytes_do_not_decode_refuses_the_write() {
    let mut pins = pinned(&schemaless_wire());
    let mut pin = (*pins.configs[0]).clone();
    pin.wire = vec![0xc1].into();
    pin.digest = nodedb_physical::physical_plan::pin_digest(pin.wire.as_slice());
    pins.configs[0] = Arc::new(pin);
    assert_refused(point_put("OPEN"), &pins);
}

#[test]
fn an_unpinned_written_collection_refuses_the_write() {
    let pins = pinned(&schemaless_wire());
    let mut op = point_put("OPEN");
    if let DocumentOp::PointPut { collection, .. } = &mut op {
        *collection = QualifiedCollection::new(DatabaseId::DEFAULT, "ghost");
    }
    let mut held = core_with(&guarded_wire());
    let response = run(&mut held.core, op, Some(&pins));
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ),
        "{:?}",
        response.error_code
    );
}

#[test]
fn an_unpinned_sum_target_refuses_the_write() {
    let pins = pinned(&schemaless_wire());
    let mut op = point_put("OPEN");
    if let DocumentOp::PointPut {
        resolved_sum_targets,
        ..
    } = &mut op
    {
        resolved_sum_targets.push(ResolvedSumTarget::new("accounts", "a1", Surrogate::new(9)));
    }
    assert_refused(op, &pins);
}

#[test]
fn a_write_with_no_crs_pin_is_refused() {
    let pins = RedoPins {
        crs: None,
        ..pinned(&schemaless_wire())
    };
    assert_refused(point_put("OPEN"), &pins);
}

/// The view a pinned write opens drops when its task ends.
#[test]
fn a_pinned_write_leaves_no_judged_view_open() {
    let mut held = core_with(&guarded_wire());
    let pins = pinned(&schemaless_wire());
    assert_ok(&run(&mut held.core, point_put("OPEN"), Some(&pins)));
    assert!(held.core.apply_scope.judged.is_none());
    assert!(!held.core.apply_scope.judging);
}

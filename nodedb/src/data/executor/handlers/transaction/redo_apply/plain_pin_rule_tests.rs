// SPDX-License-Identifier: BUSL-1.1

//! A plain replicated document write judges its rules under its proposer's
//! pins.
//!
//! The proposer saw `tickets` with no rule. The replica applied a later DDL
//! that adds one: append-only, BALANCED, or a UNIQUE index. Both cores run
//! the same writes pinned with the proposer's config and reach the same
//! verdict. The unpinned run on the replica refuses, so the pins decide.

use nodedb_physical::physical_plan::{
    BalancedDef, DocConfigWire, DocumentOp, RedoPins, RegisteredIndex, RegisteredIndexState,
};
use nodedb_types::{RlsWriteCheck, Surrogate, SystemTimeScope, Value};

use super::install_pin_tests::{assert_ok, core_with, pinned, schemaless_wire};
use super::plain_pin_tests::{collection, run};
use super::stream_validate::tests::Hold;
use crate::bridge::envelope::{ErrorCode, Response, Status};

fn body(fields: &[(&str, &str)]) -> Vec<u8> {
    let object = fields
        .iter()
        .map(|(name, value)| (name.to_string(), Value::String(value.to_string())))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body")
}

/// A put of row `t<surrogate>` holding `fields`.
fn put(surrogate: u32, fields: &[(&str, &str)]) -> DocumentOp {
    let document_id = format!("t{surrogate}");
    DocumentOp::PointPut {
        collection: collection(),
        pk_bytes: document_id.as_bytes().to_vec(),
        document_id,
        value: body(fields),
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    }
}

fn point_delete(surrogate: u32) -> DocumentOp {
    let document_id = format!("t{surrogate}");
    DocumentOp::PointDelete {
        collection: collection(),
        pk_bytes: document_id.as_bytes().to_vec(),
        document_id,
        surrogate: Some(Surrogate::new(surrogate)),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    }
}

fn bulk_delete() -> DocumentOp {
    DocumentOp::BulkDelete {
        collection: collection(),
        filters: Vec::new(),
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

/// The schemaless wire with a later append-only rule.
fn append_only_wire() -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.enforcement.append_only = true;
    wire
}

/// The schemaless wire with a later BALANCED rule over journal entries.
fn balanced_wire() -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.enforcement.balanced = Some(BalancedDef {
        group_key_column: "journal_id".into(),
        entry_type_column: "entry_type".into(),
        debit_value: "DEBIT".into(),
        credit_value: "CREDIT".into(),
        amount_column: "amount".into(),
    });
    wire
}

/// The schemaless wire with a later UNIQUE index on `status`.
fn unique_wire() -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.index_paths = vec![RegisteredIndex {
        name: "by_status".into(),
        path: "$.status".into(),
        unique: true,
        case_insensitive: false,
        state: RegisteredIndexState::Ready,
        predicate: None,
    }];
    wire
}

/// The response of each of `ops`, run in order on `held`, each pinned with
/// `pins`.
fn statuses(held: &mut Hold, ops: &[DocumentOp], pins: Option<&RedoPins>) -> Vec<Response> {
    ops.iter()
        .map(|op| run(&mut held.core, op.clone(), pins))
        .collect()
}

/// Assert the proposer core and the `later` core reach the same verdict on
/// `ops` pinned with the proposer's config, every one of them Ok. The
/// unpinned run on `later` refuses its last op with `refused`.
fn assert_one_verdict(later: &DocConfigWire, ops: &[DocumentOp], refused: fn(&ErrorCode) -> bool) {
    let proposer = schemaless_wire();
    let pins = pinned(&proposer);
    let mut on_proposer = core_with(&proposer);
    let mut on_later = core_with(later);
    for (expected, replica) in statuses(&mut on_proposer, ops, Some(&pins))
        .iter()
        .zip(statuses(&mut on_later, ops, Some(&pins)).iter())
    {
        assert_ok(expected);
        assert_ok(replica);
    }
    let mut unpinned = core_with(later);
    let verdicts = statuses(&mut unpinned, ops, None);
    let last = verdicts.last().expect("a last op");
    assert_eq!(last.status, Status::Error);
    assert!(
        last.error_code.as_deref().is_some_and(refused),
        "{:?}",
        last.error_code
    );
}

fn is_append_only(code: &ErrorCode) -> bool {
    matches!(code, ErrorCode::AppendOnlyViolation { .. })
}

fn is_balance(code: &ErrorCode) -> bool {
    matches!(code, ErrorCode::BalanceViolation { .. })
}

fn is_unique(code: &ErrorCode) -> bool {
    matches!(code, ErrorCode::RejectedConstraint { constraint, .. } if constraint == "unique")
}

#[test]
fn a_pinned_point_delete_judges_the_proposer_rules() {
    let ops = [put(1, &[("status", "OPEN")]), point_delete(1)];
    assert_one_verdict(&append_only_wire(), &ops, is_append_only);
}

#[test]
fn a_pinned_bulk_delete_judges_the_proposer_rules() {
    let ops = [put(1, &[("status", "OPEN")]), bulk_delete()];
    assert_one_verdict(&append_only_wire(), &ops, is_append_only);
}

#[test]
fn a_pinned_write_judges_the_proposer_balanced_rule() {
    let entry = [
        ("journal_id", "j1"),
        ("entry_type", "DEBIT"),
        ("amount", "5"),
    ];
    assert_one_verdict(&balanced_wire(), &[put(1, &entry)], is_balance);
}

#[test]
fn a_pinned_write_judges_the_proposer_unique_indexes() {
    let ops = [put(1, &[("status", "OPEN")]), put(2, &[("status", "OPEN")])];
    assert_one_verdict(&unique_wire(), &ops, is_unique);
}

/// A rewrite pinned at schema version 1 over a row stored at version 2 is
/// refused alike on two cores with different current configs: the
/// proposer's catalog was behind the data. Nothing is stored, and the
/// client retries with `40001`.
#[test]
fn a_rewrite_pinned_older_than_the_stored_row_is_refused_alike() {
    use super::install_pin_tests::{generated_wire, strict_v1, strict_v2, strict_wire};
    use super::plain_pin_tests::{
        batch_upsert_on_conflict, bulk_update, merge_matched, point_update, update_from_join,
        upsert_on_conflict,
    };

    let newer = pinned(&strict_wire(strict_v2("lowered")));
    let older = pinned(&strict_wire(strict_v1()));
    let rewrites = [
        point_update("CLOSED"),
        bulk_update("CLOSED"),
        update_from_join("OPEN", "CLOSED"),
        upsert_on_conflict("CLOSED"),
        batch_upsert_on_conflict("CLOSED"),
        merge_matched("OPEN", "CLOSED"),
    ];
    for rewrite in rewrites {
        for current in [strict_wire(strict_v2("lowered")), generated_wire()] {
            let mut held = core_with(&current);
            let row = put(1, &[("status", "OPEN"), ("lowered", "open")]);
            assert_ok(&run(&mut held.core, row, Some(&newer)));
            let before = super::install_pin_tests::stored(&held.core, 1);
            let response = run(&mut held.core, rewrite.clone(), Some(&older));
            assert_eq!(response.status, Status::Error, "{rewrite:?}");
            assert!(
                matches!(
                    response.error_code.as_deref(),
                    Some(ErrorCode::StalePinnedSchema { .. })
                ),
                "{rewrite:?}: {:?}",
                response.error_code
            );
            assert_eq!(
                super::install_pin_tests::stored(&held.core, 1),
                before,
                "{rewrite:?}"
            );
        }
    }
}

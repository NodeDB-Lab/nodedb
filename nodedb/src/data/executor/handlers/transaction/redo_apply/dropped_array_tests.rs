// SPDX-License-Identifier: BUSL-1.1

//! A committed redo record that names an array dropped on this node.
//!
//! The array has no catalog entry, so its sub-record is a skip: the record
//! still installs its document sub-records, and no error reaches the apply.

use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_physical::physical_plan::{PhysicalPlan, written_collections};
use nodedb_types::{Surrogate, TenantId};
use nodedb_wal::record::RecordType;

use super::entry::CommittedRedo;
use super::install_refusal_tests::document_insert;
use crate::bridge::envelope::Status;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::wal_replay::ArrayOpen;
use crate::engine::array::wal::{
    ArrayDeleteCell, ArrayDeletePayload, ArrayPutCell, ArrayPutPayload, encode_delete_with_version,
    encode_put_with_version,
};
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

const TID: u64 = 1;
const LSN: u64 = 300;

fn dropped_array() -> ArrayId {
    ArrayId::new(TenantId::new(TID), "dropped_grid")
}

fn array_put_sub_record() -> RedoSubRecord {
    let cells = vec![ArrayPutCell {
        coord: vec![CoordValue::Int64(3)],
        attrs: vec![CellValue::Int64(9)],
        surrogate: Surrogate::new(1),
        system_from_ms: 1,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
    }];
    RedoSubRecord {
        record_type: RecordType::ArrayPut as u32,
        payload: encode_put_with_version(&ArrayPutPayload {
            array_id: dropped_array(),
            vshard_id: 0,
            cells,
            provenance: None,
        })
        .expect("encode array put"),
    }
}

fn array_delete_sub_record() -> RedoSubRecord {
    let cells = vec![ArrayDeleteCell {
        coord: vec![CoordValue::Int64(3)],
        system_from_ms: 1,
        erasure: false,
    }];
    RedoSubRecord {
        record_type: RecordType::ArrayDelete as u32,
        payload: encode_delete_with_version(&ArrayDeletePayload {
            array_id: dropped_array(),
            vshard_id: 0,
            cells,
            provenance: None,
        })
        .expect("encode array delete"),
    }
}

/// Commit a document insert, append `extra` to its redo record, and apply the
/// record. Returns whether the apply committed, and the document row.
fn apply_with(extra: RedoSubRecord) -> (bool, Option<Vec<u8>>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let plans: Vec<PhysicalPlan> = vec![document_insert()];
    let redo = core
        .stage_and_resolve_for_test(&task, TID, &plans, LSN)
        .expect("resolve the document insert");
    let mut record = RedoRecord::from_bytes(&redo).expect("decode resolved redo");
    record.ops.push(extra);
    let redo = record.to_bytes().expect("encode redo");
    let mut request = task.request.clone();
    request.wal_lsn = Some(Lsn::new(LSN));
    let install_task = crate::data::executor::task::ExecutionTask::new(request);
    let collections = written_collections(&plans);
    let sum_targets = redo_sum_targets(&plans);
    let response = core.execute_apply_transaction_redo(
        &install_task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &collections,
            sum_targets: &sum_targets,
        },
    );
    let row = core
        .sparse
        .get(
            0,
            TID,
            "notes",
            &nodedb_types::StorageKey::for_surrogate(Surrogate::new(41)),
        )
        .expect("read row");
    (response.status == Status::Ok, row)
}

#[test]
fn a_dropped_array_put_is_skipped_and_the_document_row_applies() {
    let (committed, row) = apply_with(array_put_sub_record());
    assert!(committed, "the apply raises no error for the dropped array");
    assert!(row.is_some(), "the document sub-record installs its row");
}

#[test]
fn a_dropped_array_delete_is_skipped_and_the_document_row_applies() {
    let (committed, row) = apply_with(array_delete_sub_record());
    assert!(committed, "the apply raises no error for the dropped array");
    assert!(row.is_some(), "the document sub-record installs its row");
}

#[test]
fn outside_a_committed_redo_a_missing_entry_keeps_its_result() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp): (CoreLoop, _, _) = make_core_with_dir(dir.path());
    assert!(!core.skip_dropped_array_in_redo());
    assert!(matches!(
        core.ensure_array_open_for_replay(&dropped_array()),
        Ok(ArrayOpen::NoCatalogEntry)
    ));
    assert!(!core.committed_redo_failed());
}

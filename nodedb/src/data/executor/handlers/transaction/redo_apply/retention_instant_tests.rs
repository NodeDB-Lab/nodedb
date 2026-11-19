// SPDX-License-Identifier: BUSL-1.1

//! A committed delete is judged against retention at the instant its entry
//! carries, the system-time ordinal its leader assigned. Two replicas that
//! apply one entry reach one verdict, whatever their wall clocks read.

use nodedb_physical::physical_plan::document::{RetentionDuration, RetentionUnit};
use nodedb_types::{DatabaseId, TenantId};

use super::entry::CommittedRedo;
use super::test_commit::{doc_delete_sub_record, doc_put_sub_record};
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::engine::document::store::CollectionConfig;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

const TID: u64 = 1;
const COLL: &str = "held";
const SURROGATE: u32 = 5;
/// 2020-01-01T00:00:00Z, years before any wall clock this test runs at.
const CREATED_SECS: i64 = 1_577_836_800;
const HOUR_MS: i64 = 3_600_000;

/// A core whose `held` collection keeps a row an hour, holding one row
/// created at [`CREATED_SECS`].
fn replica(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.retention = Some(RetentionDuration {
        count: 1,
        unit: RetentionUnit::Hours,
    });
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
        config,
    );
    let body = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(
        std::collections::HashMap::from([(
            "created_at".to_string(),
            nodedb_types::Value::Integer(CREATED_SECS),
        )]),
    ))
    .expect("encode body");
    let seeded = install(
        &mut core,
        10,
        None,
        vec![doc_put_sub_record(COLL, "r5", &body, SURROGATE)],
    );
    assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
    (core, Box::new((req, resp)))
}

/// Install `ops` at `lsn`, as an entry whose leader assigned `ordinal`.
fn install(
    core: &mut CoreLoop,
    lsn: u64,
    ordinal: Option<i64>,
    ops: Vec<RedoSubRecord>,
) -> Response {
    let redo = RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
        pins: Default::default(),
    }
    .to_bytes()
    .expect("encode redo");
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(lsn));
    task.request.system_ordinal = ordinal;
    core.execute_apply_transaction_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &[COLL.to_string()],
            sum_targets: &[],
            pins: &Default::default(),
        },
    )
}

/// The verdict of the delete of the held row, applied at `at_ms`.
fn delete_at(core: &mut CoreLoop, at_ms: i64) -> Option<ErrorCode> {
    let response = install(
        core,
        11,
        Some(nodedb_types::ms_to_ordinal_upper(at_ms)),
        vec![doc_delete_sub_record(COLL, "r5", SURROGATE)],
    );
    response.error_code.as_deref().cloned()
}

/// The entry's leader stamped the delete half an hour after the row was
/// created, while replication delivered it years later by the wall clock.
/// Both replicas refuse it. An entry stamped two hours after the row was
/// created deletes it on both.
#[test]
fn two_replicas_judge_retention_at_the_entrys_instant() {
    let created_ms = CREATED_SECS * 1_000;
    for (at_ms, refused) in [
        (created_ms + HOUR_MS / 2, true),
        (created_ms + 2 * HOUR_MS, false),
    ] {
        let verdicts: Vec<Option<ErrorCode>> = (0..2)
            .map(|_| {
                let dir = tempfile::tempdir().expect("tempdir");
                let (mut core, _held) = replica(dir.path());
                delete_at(&mut core, at_ms)
            })
            .collect();
        assert_eq!(verdicts[0], verdicts[1], "the replicas disagree at {at_ms}");
        assert_eq!(
            matches!(verdicts[0], Some(ErrorCode::RetentionViolation { .. })),
            refused,
            "{:?} at {at_ms}",
            verdicts[0]
        );
        if !refused {
            assert_eq!(verdicts[0], None);
        }
    }
}

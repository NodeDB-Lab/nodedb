// SPDX-License-Identifier: BUSL-1.1

//! A batched install that fails in a late batch rolls back every batch it
//! wrote, through an undo log that spilled, and leaves each engine as it
//! was before the install.

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::types::Lsn;
use crate::wal::redo::replay_arm::RedoArm;
use crate::wal::{RedoRecord, RedoSubRecord};

use super::super::entry::CommittedRedo;
use super::super::passes::PassRefusal;
use super::super::stream_validate::tests::{
    Hold, LSN, TID, default_task, framed, hold, plain_core, record,
};
use super::super::stream_validate::{StreamLimits, StreamValidateInput};
use super::super::test_commit::failing_install_sub_record;
use super::StreamInstallInput;
use super::equivalence_tests::{
    BATCH_BYTES, FILL, FILL_EACH, FILL_FROM, METRICS, digest, fill, install_batched_at,
    install_whole, ts_layout, ts_line,
};
use super::rollback::run::{BatchedRollback, RollbackSlice};

/// A threshold every batch passes: the undo log spills after each batch.
const SPILL_EVERY_BATCH: usize = 1;

/// Reverse every write of a batched install that failed with `cause`, in
/// one call.
pub(super) fn roll_back_batched(
    core: &mut CoreLoop,
    rollback: BatchedRollback,
    cause: ErrorCode,
) -> PassRefusal {
    let mut run = core.begin_batched_rollback(rollback, cause);
    loop {
        match core.step_batched_rollback(run) {
            RollbackSlice::Checkpointed { run: next, .. } => run = next,
            RollbackSlice::Ended(refusal) => return refusal,
        }
    }
}

/// Validate `redo` as a stream in the held core's WAL, then install it
/// batch by batch, spilling past `spill_threshold`. `arm` receives the last
/// arm the install runs before the install starts, and what it returns
/// lives until the install answered.
fn install_streamed<G>(
    held: &mut Hold,
    redo: &RedoRecord,
    collections: &[String],
    spill_threshold: usize,
    arm: impl FnOnce(RedoArm) -> G,
) -> Response {
    let reference = held.wal.write(&framed(redo, &[], &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = default_task();
    let limits = StreamLimits {
        batch_bytes: BATCH_BYTES,
        spill_threshold,
    };
    let lsn = Lsn::new(LSN);
    let validated = held
        .core
        .validate_redo_stream(&StreamValidateInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            snapshot: None,
            judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
            lsn,
            limits,
        })
        .unwrap_or_else(|code| panic!("validate: {code:?}"));
    let last_arm = validated
        .active_arms
        .active()
        .last()
        .expect("an active arm");
    let _armed = arm(last_arm);
    held.core.install_redo_stream(
        StreamInstallInput {
            task: &task,
            tid: TID,
            marker: &marker,
            reference: &reference,
            origin: RedoOrigin::Commit,
            lsn,
            limits,
            permit: None,
            collections,
        },
        validated,
        &mut Vec::new(),
    )
}

/// The install answered that it rolled back, and the core serves on.
fn assert_rolled_back(core: &CoreLoop, response: &Response, case: &str) {
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "{case}: {:?}",
        response.error_code
    );
    assert!(!core.fail_stop.is_stopped(), "{case}: the core serves on");
}

#[cfg(feature = "failpoints")]
mod late_batch {
    use nodedb_physical::physical_plan::PhysicalPlan;

    use super::super::super::install_refusal_tests as refusal;
    use super::super::equivalence_tests::{resolved, spread};
    use super::*;
    use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;

    /// Every engine's record, spread over several batches, fails once the last
    /// arm wrote its last batch. Every batch of every arm rolls back from the
    /// spill files, and each engine reads as before the install.
    #[test]
    fn a_failure_in_a_late_batch_rolls_back_every_batch() {
        type Setup = fn(&mut CoreLoop);
        fn none(_: &mut CoreLoop) {}
        let cases: [(&str, PhysicalPlan, Setup); 7] = [
            ("document", refusal::document_insert(), none),
            ("kv", refusal::kv_predicate_delete(), refusal::seed_kv),
            ("vector", refusal::vector_direct_insert(), none),
            ("crdt", refusal::crdt_upsert(), none),
            ("spatial", refusal::spatial_insert(), none),
            ("array", refusal::array_put(), refusal::open_array),
            (
                "indexed",
                refusal::indexed_document_put(),
                refusal::declare_text,
            ),
        ];
        let mut records: Vec<(&str, RedoRecord, Vec<String>, Setup)> = Vec::new();
        for (case, plan, setup) in cases {
            let plans = std::slice::from_ref(&plan);
            assert!(redo_sum_targets(plans).is_empty(), "{case} folds nothing");
            let mut redo = resolved(plans, setup);
            redo.ops = spread(std::mem::take(&mut redo.ops));
            let mut collections = nodedb_physical::physical_plan::written_collections(plans);
            collections.push(FILL.to_string());
            records.push((case, redo, collections, setup));
        }
        records.push((
            "text",
            record(spread(vec![refusal::fts_index_sub_record()])),
            vec!["docs".to_string(), FILL.to_string()],
            refusal::declare_text,
        ));

        for (case, redo, collections, setup) in records {
            let mut held = hold(plain_core);
            setup(&mut held.core);
            let before = digest(&held.core);
            let response =
                install_streamed(&mut held, &redo, &collections, SPILL_EVERY_BATCH, |arm| {
                    nodedb_types::fail_point::FailGuard::fail(
                        &format!("redo_apply::batched::install_batch_fails::{arm:?}"),
                        "the last batch failed",
                    )
                });
            assert_rolled_back(&held.core, &response, case);
            let undo = held.core.redo_apply.last_rollback;
            assert!(
                undo.spill_lane > 2 * u64::from(FILL_EACH),
                "{case}: the rollback reverses every filler row and the engine's write, {undo:?}"
            );
            assert_eq!(
                undo.spill_lane_hot_bytes, 0,
                "{case}: every spill-lane entry came back from a spill file"
            );
            assert_eq!(digest(&held.core), before, "{case}: the state is as before");
        }
    }
}

/// Seed one row into the [`METRICS`] memtable.
fn seed_metrics(core: &mut CoreLoop) {
    let seed = record(vec![ts_line("metrics_tag,host=h0 value=1i 1000000000")])
        .to_bytes()
        .expect("encode seed");
    let mut task = default_task();
    task.wal_lsn = Some(Lsn::new(5));
    let response = core.execute_apply_transaction_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &seed,
            collections: &[METRICS.to_string()],
            sum_targets: &[],
            pins: &Default::default(),
        },
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

/// What the [`METRICS`] collection holds besides its partitions: the
/// memtable's resident bytes, its last-value cache size and its series.
fn metrics_state(core: &CoreLoop) -> MetricsState {
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), METRICS.to_string());
    MetricsState {
        memtable_bytes: core
            .columnar_memtables
            .get(&key)
            .map(|memtable| memtable.memory_bytes()),
        last_values: core.ts_last_value_caches.contains_key(&key),
        catalog: core.ts_series_catalogs.contains_key(&key),
        reservation: core
            .columnar_memtable_mem
            .get(&key)
            .map(nodedb_mem::ReservationToken::size),
        stamp: core.ts_replay.stamps.get(&key).cloned(),
    }
}

/// What [`metrics_state`] reads.
#[derive(Debug, PartialEq)]
struct MetricsState {
    memtable_bytes: Option<usize>,
    last_values: bool,
    catalog: bool,
    reservation: Option<usize>,
    stamp: Option<crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp>,
}

/// A `TRUNCATE` of [`METRICS`].
fn metrics_truncate() -> RedoSubRecord {
    RedoSubRecord {
        record_type: nodedb_wal::record::RecordType::TimeseriesTruncate as u32,
        payload: zerompk::to_msgpack_vec(&nodedb_types::columnar::ColumnarTruncateWalRecord {
            collection: METRICS.to_string(),
            system_from: None,
            erase_history: false,
        })
        .expect("encode truncate"),
    }
}

/// Ingests into [`METRICS`] across several batches, a truncate of it, one
/// ingest after the truncate, then a sub-record that fails in the
/// timeseries arm's last batch.
fn truncating_record() -> RedoRecord {
    let mut ops: Vec<RedoSubRecord> = fill(FILL_FROM, FILL_EACH);
    for index in 0..4u32 {
        ops.push(ts_line(&format!(
            "metrics_tag,host=h{index} value={index}i {}",
            (u64::from(index) + 2) * 1_000_000_000
        )));
        ops.extend(fill(FILL_FROM + FILL_EACH + index, 1));
    }
    ops.push(metrics_truncate());
    ops.push(ts_line("metrics_tag,host=h9 value=9i 9000000000"));
    ops.push(failing_install_sub_record());
    record(ops)
}

/// Install [`truncating_record`] whole and batched, each on a core `seed`
/// prepared. Both roll back: no partition the truncate's flush filed stays,
/// and the memtable, its reservation and the stamp read as before.
fn assert_truncate_flush_rolls_back(seed: fn(&mut CoreLoop), case: &str) {
    let redo = truncating_record();
    let collections = vec![
        METRICS.to_string(),
        "refusal_probe".to_string(),
        FILL.to_string(),
    ];
    for batched in [false, true] {
        let case = format!("{case}, batched {batched}");
        let mut held = hold(plain_core);
        seed(&mut held.core);
        let layout = ts_layout(&held.core);
        let state = metrics_state(&held.core);
        let before = digest(&held.core);
        let response = if batched {
            install_streamed(&mut held, &redo, &collections, SPILL_EVERY_BATCH, |_| ())
        } else {
            install_whole(&mut held.core, &redo, &[], &collections)
        };
        assert_rolled_back(&held.core, &response, &case);
        assert_eq!(ts_layout(&held.core), layout, "{case}: no partition stays");
        assert_eq!(metrics_state(&held.core), state, "{case}");
        assert_eq!(digest(&held.core), before, "{case}");
    }
}

/// The memtable held rows and a reservation before the install.
#[test]
fn a_rolled_back_truncate_takes_its_flush_back_with_a_prior_reservation() {
    assert_truncate_flush_rolls_back(seed_metrics, "prior reservation");
}

/// The collection had no memtable before the install.
#[test]
fn a_rolled_back_truncate_takes_its_flush_back_with_no_prior_reservation() {
    fn none(_: &mut CoreLoop) {}
    assert_truncate_flush_rolls_back(none, "no prior reservation");
}

/// Timeseries ingests into one collection across several batches keep one
/// memtable snapshot, the first, and the rollback puts the memtable back
/// exactly as it was.
#[test]
fn timeseries_snapshots_collapse_to_the_first_touch() {
    const INGESTS: u32 = 8;
    let mut ops: Vec<RedoSubRecord> = fill(FILL_FROM, FILL_EACH);
    for index in 0..INGESTS {
        ops.push(ts_line(&format!(
            "metrics_tag,host=h{index} value={index}i {}",
            (u64::from(index) + 2) * 1_000_000_000
        )));
        ops.extend(fill(FILL_FROM + FILL_EACH + index, 1));
    }
    // The timeseries arm runs after the document arm, and this sub-record
    // fails in its last batch.
    ops.push(failing_install_sub_record());
    let redo = record(ops);
    let collections = vec![
        METRICS.to_string(),
        "refusal_probe".to_string(),
        FILL.to_string(),
    ];

    let mut held = hold(plain_core);
    seed_metrics(&mut held.core);
    let layout = ts_layout(&held.core);
    let state = metrics_state(&held.core);
    let before = digest(&held.core);
    assert_eq!(layout, (Vec::new(), 1), "the seed sits in the memtable");

    let response = install_streamed(&mut held, &redo, &collections, SPILL_EVERY_BATCH, |_| ());
    assert_rolled_back(&held.core, &response, "timeseries");
    let undo = held.core.redo_apply.last_rollback;
    assert_eq!(
        undo.ram_lane, 1,
        "{INGESTS} ingests into one collection keep one snapshot: {undo:?}"
    );
    assert_eq!(
        ts_layout(&held.core),
        layout,
        "the memtable holds the seed only"
    );
    assert_eq!(metrics_state(&held.core), state);
    assert_eq!(digest(&held.core), before);
}

/// The batched install of a record that succeeds spills its undo log and
/// leaves the state the record writes.
#[test]
fn a_spilled_undo_log_of_a_successful_install_changes_nothing() {
    let redo = record(fill(FILL_FROM, 4 * FILL_EACH));
    let collections = vec![FILL.to_string()];
    let mut spilled = hold(plain_core);
    let response = install_batched_at(&mut spilled, &redo, &[], &collections, SPILL_EVERY_BATCH);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let mut kept = hold(plain_core);
    let response = install_batched_at(&mut kept, &redo, &[], &collections, usize::MAX);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(digest(&spilled.core), digest(&kept.core));
}

// SPDX-License-Identifier: BUSL-1.1

//! A batched install flushes its timeseries and array memtables between
//! batches, stamped with the part of the record each flush holds, so the
//! memtables stay under their budget. Restart replay applies each row of
//! such a record once, and a rollback takes every such flush back.

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::{DatabaseId, TenantId};
use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::enforcement::retention::judged_instant_ms;
use crate::data::executor::handlers::timeseries::flush_commit::REVERT_FILE;
use crate::data::executor::handlers::transaction::undo::TimerRestore;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

use super::super::stream_validate::tests::{
    LSN, StreamWal, TID, default_task, framed, hold, plain_core, record,
};
use super::super::stream_validate::{StreamLimits, StreamValidateInput};
use super::super::test_commit::failing_install_sub_record;
use super::StreamInstallInput;
use super::equivalence_tests::{
    FILL, FILL_EACH, FILL_FROM, METRICS, fill, install_whole, ts_layout,
};

/// Rows the large record writes, in [`OPS`] ingests.
const ROWS: u32 = 10_000;
const OPS: u32 = 100;
/// A batch bound that puts a few ingests in each batch.
const LARGE_BATCH_BYTES: usize = 16 * 1024;
/// A batch bound that puts a few ops in each batch of a small record.
const SMALL_BATCH_BYTES: usize = 256;

fn metrics_key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), METRICS.to_string())
}

/// One timeseries ingest of the rows `first..first + count` into
/// [`METRICS`], one second apart.
fn ts_rows(first: u32, count: u32) -> RedoSubRecord {
    let lines: Vec<String> = (first..first + count)
        .map(|row| {
            format!(
                "metrics_tag,host=h{} value={row}i {}",
                row % 4,
                (u64::from(row) + 1) * 1_000_000_000
            )
        })
        .collect();
    let payload = zerompk::to_msgpack_vec(&lines).expect("encode lines");
    RedoSubRecord {
        record_type: RecordType::TimeseriesBatch as u32,
        payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
            crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                collection: METRICS,
                payload: &payload,
                provenance: None,
                format: "ilp-msgpack",
                default_timestamp_ms: 0,
            },
        )
        .expect("encode ingest"),
    }
}

/// [`ROWS`] rows in [`OPS`] ingests.
fn large_record() -> RedoRecord {
    let each = ROWS / OPS;
    record((0..OPS).map(|op| ts_rows(op * each, each)).collect())
}

/// The resident bytes of a memtable that holds one ingest of the large
/// record: its columns at their first capacity.
fn base_memtable_bytes() -> usize {
    let mut scratch = hold(plain_core);
    let response = install_whole(
        &mut scratch.core,
        &record(vec![ts_rows(0, ROWS / OPS)]),
        &[],
        &[METRICS.to_string()],
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    scratch
        .core
        .columnar_memtables
        .get(&metrics_key())
        .map(|mt| mt.memory_bytes())
        .expect("the seed memtable")
}

/// A core over a caller-owned directory, its WAL and its bridge ends.
struct Node {
    core: CoreLoop,
    wal: StreamWal,
    _ends: Box<dyn std::any::Any>,
}

fn open(dir: &std::path::Path, budget: usize) -> Node {
    let (mut core, req, resp) = make_core_with_dir(dir);
    core.ts_tuning.memtable_budget_bytes = budget;
    core.load_ts_registries().expect("load registries");
    let wal = StreamWal::open(&core);
    Node {
        core,
        wal,
        _ends: Box::new((req, resp)),
    }
}

/// Write `redo` as a stream into the node's WAL, then validate and install
/// it at `batch_bytes`. Returns the answer and the marker's WAL record.
fn install_streamed(
    node: &mut Node,
    redo: &RedoRecord,
    collections: &[String],
    batch_bytes: usize,
) -> (Response, WalRecord) {
    let reference = node.wal.write(&framed(redo, &[], &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = default_task();
    let limits = StreamLimits {
        batch_bytes,
        spill_threshold: usize::MAX,
    };
    let lsn = Lsn::new(LSN);
    let validated = node
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
    let response = node.core.install_redo_stream(
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
    );
    let marker_record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: LSN,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DatabaseId::DEFAULT.as_u64(),
        payload: marker.to_bytes().expect("encode marker"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("marker record");
    (response, marker_record)
}

/// Rows in every partition and in the memtable, and the time span they
/// cover.
fn ts_totals(core: &CoreLoop) -> (u64, i64, i64) {
    let key = metrics_key();
    let mut rows = 0;
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    if let Some(registry) = core.ts_registries.get(&key) {
        for (_, entry) in registry.iter() {
            rows += entry.meta.row_count;
            min_ts = min_ts.min(entry.meta.min_ts);
            max_ts = max_ts.max(entry.meta.max_ts);
        }
    }
    if let Some(mt) = core.columnar_memtables.get(&key)
        && !mt.is_empty()
    {
        rows += mt.row_count();
        min_ts = min_ts.min(mt.min_ts());
        max_ts = max_ts.max(mt.max_ts());
    }
    (rows, min_ts, max_ts)
}

/// The partition directories on disk under the collection's directory.
fn partition_dirs(core: &CoreLoop) -> Vec<String> {
    let dir = core.ts_dir_of(&metrics_key());
    let mut dirs: Vec<String> = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                .filter(|name| name.starts_with("ts-") || name.starts_with("pending-"))
                .collect()
        })
        .unwrap_or_default();
    dirs.sort();
    dirs
}

/// Replay `marker` as restart replay does, on a node booted over `dir`.
fn restart(dir: &std::path::Path, budget: usize, marker: &WalRecord) -> Node {
    let mut node = open(dir, budget);
    node.core
        .floors
        .applied_prefix
        .seed_replayed_through(Lsn::new(LSN));
    node.core
        .replay_transaction_redo_wal(
            std::slice::from_ref(marker),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("replay");
    node
}

/// A record larger than the memtable budget installs batch by batch. Each
/// memtable that reaches the budget flushes after its batch, so no
/// memtable holds more than the budget and one batch, far below the
/// record whole. The rows read back as the whole-record install leaves
/// them.
#[test]
fn memtable_stays_under_budget_through_a_large_install() {
    let budget = base_memtable_bytes() + 1;
    let redo = large_record();
    let collections = vec![METRICS.to_string()];

    let mut whole = hold(plain_core);
    let response = install_whole(&mut whole.core, &redo, &[], &collections);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let whole_bytes = whole
        .core
        .columnar_memtables
        .get(&metrics_key())
        .map(|mt| mt.memory_bytes())
        .expect("the whole record's memtable");

    let dir = tempfile::tempdir().expect("tempdir");
    let mut node = open(dir.path(), budget);
    node.core.redo_apply.peak_ts_memtable_bytes = 0;
    let (response, _) = install_streamed(&mut node, &redo, &collections, LARGE_BATCH_BYTES);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);

    let peak = node.core.redo_apply.peak_ts_memtable_bytes;
    assert!(peak > 0, "the install measured its memtable");
    assert!(
        peak <= 2 * budget,
        "a memtable held {peak} bytes, over the budget {budget} and one batch"
    );
    assert!(
        peak < whole_bytes,
        "a memtable held {peak} bytes, as much as the whole record's {whole_bytes}"
    );
    let (partitions, _) = ts_layout(&node.core);
    assert!(
        partitions.len() >= 2,
        "the install flushed between its batches: {partitions:?}"
    );
    assert_eq!(ts_totals(&node.core), ts_totals(&whole.core));
    assert_eq!(ts_totals(&node.core).0, u64::from(ROWS));
}

/// A restart after a batched install whose flushes between batches hold
/// part of the record, and whose memtable held the rest: replay skips the
/// ops the partitions hold and applies the others, once, at every restart.
#[test]
fn restart_replay_of_a_partially_flushed_record_applies_each_row_once() {
    let budget = base_memtable_bytes() + 1;
    let redo = large_record();
    let collections = vec![METRICS.to_string()];
    let dir = tempfile::tempdir().expect("tempdir");
    let marker = {
        let mut node = open(dir.path(), budget);
        let (response, marker) =
            install_streamed(&mut node, &redo, &collections, LARGE_BATCH_BYTES);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let (partitions, memtable) = ts_layout(&node.core);
        assert!(
            !partitions.is_empty(),
            "the install flushed between batches"
        );
        assert!(memtable > 0, "the memtable holds the record's last rows");
        let stamp = node
            .core
            .ts_replay
            .stamps
            .get(&metrics_key())
            .expect("a stamp")
            .clone();
        assert!(!stamp.rows.skips(LSN), "no stamp names the record whole");
        assert!(
            !stamp.partial.is_empty(),
            "the partitions name the record in part"
        );
        marker
    };

    for restart_index in 0..2 {
        let node = restart(dir.path(), budget, &marker);
        assert_eq!(
            ts_totals(&node.core).0,
            u64::from(ROWS),
            "restart {restart_index}: every row of the record once"
        );
    }
}

/// A `TRUNCATE` of [`METRICS`].
fn truncate_metrics() -> RedoSubRecord {
    RedoSubRecord {
        record_type: RecordType::TimeseriesTruncate as u32,
        payload: zerompk::to_msgpack_vec(&nodedb_types::columnar::ColumnarTruncateWalRecord {
            collection: METRICS.to_string(),
            system_from: None,
            erase_history: false,
        })
        .expect("encode truncate"),
    }
}

/// Rows a current read sees: the memtable's and every partition no
/// truncate ended.
fn current_rows(core: &CoreLoop) -> u64 {
    let key = metrics_key();
    let partitions: u64 = core.ts_registries.get(&key).map_or(0, |registry| {
        registry
            .current_partitions()
            .map(|entry| entry.meta.row_count)
            .sum()
    });
    partitions
        + core
            .columnar_memtables
            .get(&key)
            .map_or(0, |mt| mt.row_count())
}

/// A truncate in a committed record ends the rows of the record's ops
/// before it. Its stamps name those ops, so restart replay skips them and
/// the truncate, applies the ops after it, and the ended rows stay ended.
#[test]
fn restart_replay_keeps_the_rows_a_truncate_in_the_record_ended() {
    let redo = record(vec![ts_rows(0, 5), truncate_metrics(), ts_rows(5, 3)]);
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mut node = open(dir.path(), usize::MAX);
        let response = install_whole(&mut node.core, &redo, &[], &[METRICS.to_string()]);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            current_rows(&node.core),
            3,
            "the truncate ended the first rows"
        );
    }
    let wal_record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: LSN,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DatabaseId::DEFAULT.as_u64(),
        payload: redo.to_bytes().expect("encode redo"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("redo record");
    for restart_index in 0..2 {
        let node = restart(dir.path(), usize::MAX, &wal_record);
        assert_eq!(
            current_rows(&node.core),
            3,
            "restart {restart_index}: the ended rows stay ended, the rows after the truncate \
             apply once"
        );
    }
}

/// The same record with a second truncate and more rows after it.
fn double_truncate_record() -> RedoRecord {
    record(vec![
        ts_rows(0, 5),
        truncate_metrics(),
        ts_rows(5, 3),
        truncate_metrics(),
        ts_rows(8, 2),
    ])
}

/// A crash after the record's first truncate took effect and before its
/// second: the durable state of an install of the record's first three
/// ops. Restart replay of the whole record skips the ops the first
/// truncate's stamp names, and applies the second truncate: only the rows
/// after it stay current.
#[test]
fn restart_replay_applies_a_second_truncate_after_the_first_took_effect() {
    let whole = double_truncate_record();
    let cut = record(vec![ts_rows(0, 5), truncate_metrics(), ts_rows(5, 3)]);
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mut node = open(dir.path(), usize::MAX);
        let response = install_whole(&mut node.core, &cut, &[], &[METRICS.to_string()]);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let stamp = node
            .core
            .ts_replay
            .stamps
            .get(&metrics_key())
            .expect("a stamp")
            .clone();
        assert!(!stamp.truncates.skips(LSN), "the truncate is named by op");
        assert!(stamp.partial.skips(LSN, 1) && !stamp.partial.skips(LSN, 2));
    }
    let wal_record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: LSN,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DatabaseId::DEFAULT.as_u64(),
        payload: whole.to_bytes().expect("encode redo"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("redo record");
    for restart_index in 0..2 {
        let node = restart(dir.path(), usize::MAX, &wal_record);
        assert_eq!(
            current_rows(&node.core),
            2,
            "restart {restart_index}: the second truncate ended the rows between the two"
        );
    }
}

/// A collection whose memtable holds rows from before the install, which
/// the install has not ingested into, flushes between batches: memory
/// pressure presses it. The flush records the collection's memtable
/// snapshot first, so the rollback puts every one of those rows back.
#[test]
fn a_pressed_collection_the_install_has_not_ingested_into_keeps_its_rows_on_rollback() {
    use std::collections::HashSet;

    use super::super::state::{RedoApplyPass, RedoApplyScope};
    use super::rollback::undo::SpillableUndo;
    use crate::data::executor::handlers::timeseries::{TsStreamAt, TsStreamLoads};
    use crate::types::partial_record::PartialRecord;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut node = open(dir.path(), usize::MAX);
    let response = install_whole(
        &mut node.core,
        &record(vec![ts_rows(0, 10)]),
        &[],
        &[METRICS.to_string()],
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(ts_layout(&node.core), (Vec::new(), 10));

    // A batched install whose record writes the collection in a later
    // batch, and is between two batches now.
    let mut loads = TsStreamLoads::default();
    node.core.add_ts_stream_op(
        &mut loads,
        &ts_rows(100, 1),
        TsStreamAt {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TID),
            vshard_id: 0,
        },
    );
    let mut scope = RedoApplyScope::new(RedoApplyPass::Install, Vec::new());
    scope.spillable = Some(SpillableUndo::default());
    node.core.redo_apply.scope = Some(scope);
    // Pressure the pre-batch measure did not see.
    node.core.ts_tuning.memtable_budget_bytes = 1;
    let flushed = node
        .core
        .flush_ts_install_batch(
            &loads,
            PartialRecord {
                lsn: LSN,
                ops_through: 1,
            },
            &mut HashSet::new(),
        )
        .unwrap_or_else(|error| panic!("flush: {error:?}"));
    assert_eq!(flushed, 1);
    assert_eq!(ts_layout(&node.core), (vec![10], 0), "the rows moved out");

    let scope = node
        .core
        .redo_apply
        .scope
        .take()
        .expect("the install scope");
    let undo = scope.spillable.expect("the batched log");
    let core = &mut node.core;
    let drained = super::rollback::undo::tests::drain_newest_first(undo, |index, _, entry| {
        core.apply_undo_entry(0, TID, index, entry, TimerRestore::KeepNewer)
    });
    assert!(drained.is_ok(), "every entry reversed");
    assert_eq!(
        ts_layout(&node.core),
        (Vec::new(), 10),
        "every row from before the install is back in the memtable"
    );
    assert!(partition_dirs(&node.core).is_empty());
}

/// A batched install that flushed its timeseries memtable after each
/// batch, then failed, takes every flush back: no partition directory,
/// revert manifest, stamp or memtable of the record stays.
#[test]
fn rollback_removes_install_partitions() {
    let mut ops: Vec<RedoSubRecord> = fill(FILL_FROM, FILL_EACH);
    for index in 0..6u32 {
        ops.push(ts_rows(index * 10, 10));
        ops.extend(fill(FILL_FROM + FILL_EACH + index, 1));
    }
    // The timeseries arm's last batch fails once the flushes before it ran.
    ops.push(failing_install_sub_record());
    let redo = record(ops);
    let collections = vec![
        METRICS.to_string(),
        "refusal_probe".to_string(),
        FILL.to_string(),
    ];
    let dir = tempfile::tempdir().expect("tempdir");
    let mut node = open(dir.path(), 1);
    let (response, _) = install_streamed(&mut node, &redo, &collections, SMALL_BATCH_BYTES);
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "{:?}",
        response.error_code
    );
    assert!(!node.core.fail_stop.is_stopped(), "the core serves on");
    let undo = node.core.redo_apply.last_rollback;
    assert_eq!(
        undo.ram_lane, 1,
        "one memtable snapshot stays in RAM, and every flush undo spills: {undo:?}"
    );
    assert!(
        node.core.ts_dir_of(&metrics_key()).exists(),
        "the install flushed the collection before it failed"
    );
    assert_eq!(ts_layout(&node.core), (Vec::new(), 0));
    assert!(
        partition_dirs(&node.core).is_empty(),
        "every partition the install flushed is gone: {:?}",
        partition_dirs(&node.core)
    );
    assert!(
        !node
            .core
            .ts_dir_of(&metrics_key())
            .join(REVERT_FILE)
            .exists(),
        "every revert finished"
    );
    assert!(node.core.ts_replay.stamps.get(&metrics_key()).is_none());
    assert!(
        node.core
            .columnar_memtable_mem
            .get(&metrics_key())
            .is_none()
    );
}

#[cfg(feature = "failpoints")]
mod array {
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};
    use nodedb_types::Surrogate;

    use super::super::super::install_refusal_tests::open_array;
    use super::super::equivalence_tests::resolved;
    use super::*;
    use crate::engine::array::wal::ArrayPutCell;

    fn grid() -> ArrayId {
        ArrayId::new(TenantId::new(TID), "grid")
    }

    /// A put of one cell at `x` into the array [`open_array`] opens.
    fn put(x: i64) -> PhysicalPlan {
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(x)],
            surrogate: Surrogate::new(u32::try_from(x + 1).expect("surrogate")),
            system_from_ms: 1,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        PhysicalPlan::Array(ArrayOp::Put {
            array_id: grid(),
            cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        })
    }

    /// The segment files in the array's directory.
    fn segment_files(core: &CoreLoop) -> Vec<String> {
        let store = core.array_engine.store(&grid()).expect("array store");
        let mut files: Vec<String> = std::fs::read_dir(store.root())
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
                    .filter(|name| name.ends_with(".ndas"))
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        files
    }

    /// An array install that flushed after each batch, then failed in its
    /// last batch, withdraws every segment it flushed: the manifest and
    /// the array's directory hold what they held before, and the memtable
    /// holds no cell of the record.
    #[test]
    fn rollback_removes_install_segments() {
        let plans: Vec<PhysicalPlan> = (0..6).map(put).collect();
        let mut redo = resolved(&plans, open_array);
        let mut ops = fill(FILL_FROM, FILL_EACH);
        for (index, op) in std::mem::take(&mut redo.ops).into_iter().enumerate() {
            ops.push(op);
            ops.extend(fill(FILL_FROM + FILL_EACH + index as u32, 1));
        }
        redo.ops = ops;
        let collections = vec!["grid".to_string(), FILL.to_string()];

        let dir = tempfile::tempdir().expect("tempdir");
        let mut node = open(dir.path(), usize::MAX);
        open_array(&mut node.core);
        node.core.array_engine.set_flush_cell_threshold(1);
        let manifest_before = {
            let store = node.core.array_engine.store(&grid()).expect("array store");
            (
                store.manifest().segments.len(),
                store.manifest().replay.clone(),
            )
        };
        let files_before = segment_files(&node.core);

        let _armed = nodedb_types::fail_point::FailGuard::fail(
            "redo_apply::batched::install_batch_fails::Array",
            "the last batch failed",
        );
        let (response, _) = install_streamed(&mut node, &redo, &collections, SMALL_BATCH_BYTES);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            response.error_code
        );
        assert!(!node.core.fail_stop.is_stopped(), "the core serves on");
        let store = node.core.array_engine.store(&grid()).expect("array store");
        assert_eq!(
            (
                store.manifest().segments.len(),
                store.manifest().replay.clone()
            ),
            manifest_before
        );
        assert!(store.manifest().partial.is_empty());
        assert!(store.memtable.is_empty(), "no cell of the record stays");
        assert_eq!(segment_files(&node.core), files_before);
    }
}

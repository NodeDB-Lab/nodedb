// SPDX-License-Identifier: BUSL-1.1

//! A flush whose rows span two intervals, interrupted at each step of its
//! commit, then reloaded and replayed, or rolled forward on the running core:
//! every row is stored exactly once.

use nodedb_physical::physical_plan::{TimeseriesOp, TimeseriesSchema};

use super::flush_buckets::{BucketFlush, stage_and_commit};
use super::flush_commit::{
    FlushManifest, FlushMove, MANIFEST_FILE, PENDING_PREFIX, commit_flush, pending_name,
};
use super::{TimeseriesApplyMode, TimeseriesIngestExec, TsResolveInput};
use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::engine::timeseries::interval_split::split_by_interval;
use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, TsDriftPolicy};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

const TENANT: u64 = 1;
const COLLECTION: &str = "metrics";
/// The WAL record the first flush holds.
const LSN: u64 = 1;
/// One row at 1 s, one at 1 h + 1 ms: two one-hour intervals.
const LINES: [&str; 2] = [
    "metrics value=1 1000000000",
    "metrics value=2 3600001000000",
];

fn key() -> (DatabaseId, TenantId, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(TENANT),
        COLLECTION.to_string(),
    )
}

fn task_at(lsn: u64) -> ExecutionTask {
    CoreLoop::replay_task(
        TenantId::new(TENANT),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            restart_identity: false,
        }),
        Some(Lsn::new(lsn)),
    )
}

/// Resolve and install `LINES` as the record at `lsn`, as its first apply and
/// as its restart replay alike.
fn apply_record(core: &mut CoreLoop, lsn: u64) {
    let lines: Vec<String> = LINES.iter().map(|line| (*line).to_string()).collect();
    let batch = core
        .resolve_ts_batch(
            &task_at(0),
            TsResolveInput {
                tid: TenantId::new(TENANT),
                collection: COLLECTION,
                lines: &lines,
                now_ms: 1_000,
                drift: TsDriftPolicy::Refuse,
                needs_images: false,
                base: None,
            },
        )
        .expect("resolve");
    let payload = batch.to_bytes().expect("encode batch");
    let mut task = task_at(lsn);
    task.request.admission = Admission::Exempt(ExemptReason::AlreadyOrdered);
    let response = core.execute_timeseries_ingest(TimeseriesIngestExec {
        task: &task,
        tid: TenantId::new(TENANT),
        collection: COLLECTION,
        payload: &payload,
        format: RESOLVED_INGEST_FORMAT,
        wal_lsn: Some(lsn),
        provenance: None,
        mode: TimeseriesApplyMode::Immediate,
        rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: &[],
    });
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

/// Declare a one-hour partition interval for the collection.
fn declare_hourly(core: &mut CoreLoop) {
    let mut config = CollectionConfig::new(COLLECTION);
    config.timeseries = Some(Box::new(TimeseriesSchema {
        time_key: "timestamp".into(),
        columns: Vec::new(),
        partition_interval: "1h".into(),
        last_value_cache: false,
    }));
    core.doc_configs.insert(key(), config);
}

fn flush(core: &mut CoreLoop) -> crate::Result<()> {
    core.flush_ts_collection(
        TenantId::new(TENANT),
        DatabaseId::DEFAULT,
        COLLECTION,
        1_000,
    )
}

/// Rows in the memtable plus rows in the registered partitions.
fn stored_rows(core: &CoreLoop) -> (u64, u64) {
    let memtable = core
        .columnar_memtables
        .get(&key())
        .map_or(0, |mt| mt.row_count());
    let partitions = core.ts_registries.get(&key()).map_or(0, |registry| {
        registry.iter().map(|(_, entry)| entry.meta.row_count).sum()
    });
    (memtable, partitions)
}

fn partition_count(core: &CoreLoop) -> usize {
    core.ts_registries
        .get(&key())
        .map_or(0, |registry| registry.partition_count())
}

fn segment_dir(core: &CoreLoop) -> std::path::PathBuf {
    super::paths::ts_collection_dir(
        &core.data_dir,
        DatabaseId::DEFAULT.as_u64(),
        TENANT,
        COLLECTION,
    )
}

/// Stage and commit a flush of the memtable without installing it: the
/// state an install failure right after the commit leaves.
fn commit_without_install(core: &CoreLoop) {
    let dir = segment_dir(core);
    std::fs::create_dir_all(&dir).expect("collection dir");
    let memtable = core.columnar_memtables.get(&key()).expect("memtable");
    let view = memtable.flush_view();
    let interval = core.ts_partition_interval(&key()).expect("interval");
    let buckets = split_by_interval(&view, &interval);
    assert_eq!(buckets.len(), 2);
    let stamp = core.ts_flush_stamp(&key()).expect("stamp");
    stage_and_commit(BucketFlush {
        segment_dir: &dir,
        view: &view,
        buckets: &buckets,
        stamp: &stamp,
        flush_wal_lsn: stamp.rows.highest(),
        kek: None,
    })
    .expect("stage and commit");
}

/// Whether any staged partition or manifest is left in the collection
/// directory.
fn leftovers(core: &CoreLoop) -> bool {
    let dir = segment_dir(core);
    dir.join(MANIFEST_FILE).exists()
        || std::fs::read_dir(&dir)
            .map(|entries| {
                entries.flatten().any(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with(PENDING_PREFIX))
                })
            })
            .unwrap_or(false)
}

/// The step a crash interrupts.
enum Crash {
    /// Both partitions staged, no manifest written.
    BeforeCommit,
    /// The manifest written, no partition renamed.
    AfterManifest,
    /// The manifest written, the first partition renamed.
    MidRename,
}

/// Flush the record, put the directory into the state `crash` leaves,
/// restart, reload and replay. Returns `(memtable rows, partition rows,
/// partition count)` after the replay.
fn crash_and_replay(crash: Crash) -> (u64, u64, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    declare_hourly(&mut core);
    flush(&mut core).expect("flush");
    let names: Vec<String> = core
        .ts_registries
        .get(&key())
        .map(|registry| {
            registry
                .iter()
                .map(|(_, entry)| entry.dir_name.clone())
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(names.len(), 2, "two intervals, two partitions");
    let segment_dir = segment_dir(&core);
    drop(core);

    let flush_id = format!("{LSN:016x}");
    let moves: Vec<FlushMove> = names
        .iter()
        .enumerate()
        .map(|(index, name)| FlushMove {
            pending: pending_name(&flush_id, index),
            installed: name.clone(),
        })
        .collect();
    let unrename = |movement: &FlushMove| {
        std::fs::rename(
            segment_dir.join(&movement.installed),
            segment_dir.join(&movement.pending),
        )
        .expect("put the partition back in staging");
    };
    match crash {
        Crash::BeforeCommit => moves.iter().for_each(unrename),
        Crash::AfterManifest => {
            moves.iter().for_each(unrename);
            commit_flush(&segment_dir, &FlushManifest { moves }).expect("manifest");
        }
        Crash::MidRename => {
            unrename(&moves[1]);
            commit_flush(&segment_dir, &FlushManifest { moves }).expect("manifest");
        }
    }

    let (mut restarted, _tx, _rx) = make_core_with_dir(dir.path());
    restarted.load_ts_registries().expect("load registries");
    apply_record(&mut restarted, LSN);
    let (memtable, partitions) = stored_rows(&restarted);
    (memtable, partitions, partition_count(&restarted))
}

#[test]
fn a_flush_spanning_two_intervals_writes_two_partitions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    declare_hourly(&mut core);
    flush(&mut core).expect("flush");
    let intervals: Vec<(i64, i64, u64)> = core
        .ts_registries
        .get(&key())
        .map(|registry| {
            registry
                .iter()
                .map(|(_, e)| (e.meta.min_ts, e.meta.max_ts, e.meta.interval_ms))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        intervals,
        vec![(1_000, 1_000, 3_600_000), (3_600_001, 3_600_001, 3_600_000)]
    );
    assert_eq!(stored_rows(&core), (0, 2));
    assert!(!leftovers(&core));
}

#[test]
fn a_crash_before_the_commit_replays_the_rows_once() {
    assert_eq!(crash_and_replay(Crash::BeforeCommit), (2, 0, 0));
}

#[test]
fn a_crash_after_the_manifest_installs_the_flush_and_skips_the_record() {
    assert_eq!(crash_and_replay(Crash::AfterManifest), (0, 2, 2));
}

#[test]
fn a_crash_mid_rename_installs_the_flush_and_skips_the_record() {
    assert_eq!(crash_and_replay(Crash::MidRename), (0, 2, 2));
}

/// An install that failed after the commit is rolled forward by the next
/// flush, which then flushes the rows ingested since. Every row is stored
/// once, on the running core and after a restart.
#[test]
fn a_flush_after_a_failed_install_rolls_it_forward() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    declare_hourly(&mut core);
    commit_without_install(&core);
    assert_eq!(
        stored_rows(&core),
        (2, 0),
        "the committed rows are still in memory"
    );

    // A record ingested after the commit appends behind the committed rows.
    apply_record(&mut core, 2);
    assert_eq!(stored_rows(&core), (4, 0));

    flush(&mut core).expect("the next flush rolls the commit forward, then flushes");
    assert_eq!(stored_rows(&core), (0, 4), "every row once, none in memory");
    assert_eq!(partition_count(&core), 4, "two partitions per flush");
    assert!(!leftovers(&core));
    drop(core);

    let (mut restarted, _tx, _rx) = make_core_with_dir(dir.path());
    restarted.load_ts_registries().expect("load registries");
    apply_record(&mut restarted, LSN);
    apply_record(&mut restarted, 2);
    assert_eq!(
        stored_rows(&restarted),
        (0, 4),
        "replay skips both records the partitions hold"
    );
}

/// The maintenance tick rolls a failed install forward on its own.
#[test]
fn the_maintenance_tick_rolls_a_failed_install_forward() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    declare_hourly(&mut core);
    commit_without_install(&core);
    core.roll_forward_ts_flushes(1_000);
    assert_eq!(stored_rows(&core), (0, 2));
    assert_eq!(partition_count(&core), 2);
    assert!(!leftovers(&core));
}

/// While the roll-forward itself fails, the collection takes no new flush and
/// keeps every row in memory.
#[test]
fn a_failing_roll_forward_refuses_the_next_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    declare_hourly(&mut core);
    commit_without_install(&core);
    // A staged partition the manifest names is gone: the install cannot finish.
    let staged = segment_dir(&core).join(pending_name(&format!("{LSN:016x}"), 1));
    std::fs::remove_dir_all(&staged).expect("remove a staged partition");

    assert!(flush(&mut core).is_err());
    assert_eq!(
        stored_rows(&core),
        (2, 0),
        "no row left memory, none stored twice"
    );
    assert!(segment_dir(&core).join(MANIFEST_FILE).exists());
}

/// A later flush cuts at the interval declared when it runs. The partitions
/// an earlier flush cut keep their interval.
#[test]
fn an_interval_change_governs_only_later_flushes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    apply_record(&mut core, LSN);
    // No declared interval: one partition per flush.
    flush(&mut core).expect("first flush");
    assert_eq!(partition_count(&core), 1);

    declare_hourly(&mut core);
    apply_record(&mut core, 2);
    flush(&mut core).expect("second flush");

    let mut intervals: Vec<u64> = core
        .ts_registries
        .get(&key())
        .map(|registry| registry.iter().map(|(_, e)| e.meta.interval_ms).collect())
        .unwrap_or_default();
    intervals.sort_unstable();
    assert_eq!(
        intervals,
        vec![0, 3_600_000, 3_600_000],
        "the first flush keeps its single cut; the second cuts hourly"
    );
    assert_eq!(stored_rows(&core), (0, 4));
}

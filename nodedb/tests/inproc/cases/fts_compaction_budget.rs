// SPDX-License-Identifier: BUSL-1.1

//! Verifies that FTS LSM level compaction is gated by the per-database
//! maintenance CPU budget.
//!
//! Two databases share one CoreLoop:
//!   - `db_cold` (cap = 25%) — under budget; FTS compaction must run.
//!   - `db_hot`  (cap = 1%)  — budget pre-exhausted; FTS compaction
//!     must be deferred without touching segments.
//!
//! Setup: both databases own one FTS-indexed collection with 9 synthetic
//! L0 segments (one above the default `max_segments_per_level` of 8).
//! Segments are written directly via `CoreLoop::fts_write_segment` because
//! Origin's normal indexing path writes to the POSTINGS table directly; the
//! LSM segment path is exercised by writing raw segment blobs and then
//! triggering the maintenance cycle.
//!
//! Assertions:
//!   - `stats.fts_compacted > 0` for db_cold (segments were merged).
//!   - Segment count at L0 for db_cold is < 9 after compaction.
//!   - `stats.fts_deferred > 0` for db_hot (budget gate fired).
//!   - Segment count at L0 for db_hot is unchanged (== 9).

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::maintenance::MaintenanceBudgetTracker;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_types::{DatabaseId, TenantId};

fn make_core() -> (
    CoreLoop,
    Producer<nodedb::bridge::dispatch::BridgeRequest>,
    Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel(64);
    let (resp_tx, resp_rx) = RingBuffer::channel(64);
    let core = CoreLoop::open(
        nodedb::data::executor::core_loop::CoreOpenParams::unencrypted(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            nodedb::data::executor::core_loop::test_governor(),
        ),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

/// Build a minimal valid FTS segment byte blob using the nodedb-fts writer.
fn make_test_segment() -> Vec<u8> {
    use nodedb_fts::block::CompactPosting;
    use nodedb_types::Surrogate;
    let mut postings = std::collections::HashMap::new();
    postings.insert(
        "hello".to_string(),
        vec![CompactPosting {
            doc_id: Surrogate(1),
            term_freq: 1,
            fieldnorm: 128,
            positions: vec![0],
        }],
    );
    nodedb_fts::lsm::segment::writer::flush_to_segment(postings)
        .expect("test segment build must succeed")
}

/// Return the number of L0 segments for `(database, tenant, collection)`.
///
/// Uses `nodedb_fts::lsm::compaction::parse_level` rather than a hardcoded
/// `"L0:"` prefix so changes to the segment-id format don't silently break
/// the test's level filter.
fn l0_count(core: &CoreLoop, db: DatabaseId, tenant: TenantId, collection: &str) -> usize {
    core.fts_list_segments(db.as_u64(), tenant, collection)
        .unwrap_or_default()
        .into_iter()
        .filter(|id| nodedb_fts::lsm::compaction::parse_level(id) == 0)
        .count()
}

/// Write `n` L0 segments for `(database, tenant, collection)`.
fn write_l0_segments(
    core: &CoreLoop,
    db: DatabaseId,
    tenant: TenantId,
    collection: &str,
    n: usize,
) {
    let segment_data = make_test_segment();
    for i in 0..n {
        let seg_id = nodedb_fts::lsm::compaction::segment_id(i as u64, 0);
        core.fts_write_segment(db.as_u64(), tenant, collection, &seg_id, &segment_data)
            .expect("fts_write_segment must succeed");
    }
}

/// Exhaust `db`'s maintenance budget by acquiring leases and sleeping.
///
/// Uses `try_acquire` directly on the tracker so no compaction work runs.
/// The cap is `maintenance_cpu_pct% of 60s`; each iteration sleeps 1ms and
/// records ~1ms of consumption. We allow 10× iterations to ensure the budget
/// is truly exhausted even on slow CI machines.
fn exhaust_budget(tracker: &Arc<MaintenanceBudgetTracker>, db: DatabaseId, cap_pct: u8) {
    let cap_secs = (cap_pct as f64 / 100.0) * 60.0;
    let max_iters = (cap_secs * 1000.0 * 10.0) as usize + 1000;
    for _ in 0..max_iters {
        match tracker.try_acquire(db, 0.0) {
            Some(lease) => {
                std::thread::sleep(Duration::from_millis(1));
                drop(lease);
            }
            None => return,
        }
    }
}

#[test]
fn fts_compaction_respects_maintenance_budget() {
    let (mut core, _req_tx, _resp_rx, _dir) = make_core();

    let db_cold = DatabaseId::new(200);
    let db_hot = DatabaseId::new(201);
    let tenant_cold = TenantId::new(200);
    let tenant_hot = TenantId::new(201);
    let collection = "articles";

    // Install a budget tracker.
    let tracker = Arc::new(MaintenanceBudgetTracker::new());
    // db_cold: 25% of 60s = 15s cap per minute — far from exhausted.
    tracker.set_cap(db_cold, 25);
    // db_hot: 1% of 60s = 0.6s cap — pre-exhausted below before compaction.
    tracker.set_cap(db_hot, 1);
    core.set_maintenance_budget(Arc::clone(&tracker));

    // Pre-exhaust db_hot's budget so the next try_acquire returns None.
    exhaust_budget(&tracker, db_hot, 1);

    // Verify db_hot is actually exhausted before inserting segments.
    assert!(
        tracker.try_acquire(db_hot, 0.0).is_none(),
        "db_hot budget must be exhausted before the compaction run"
    );

    // Write 9 L0 segments for each collection (one above the default limit of 8).
    let over_limit = 9usize;
    write_l0_segments(&core, db_cold, tenant_cold, collection, over_limit);
    write_l0_segments(&core, db_hot, tenant_hot, collection, over_limit);

    assert_eq!(
        l0_count(&core, db_cold, tenant_cold, collection),
        over_limit,
        "db_cold should start with {over_limit} L0 segments"
    );
    assert_eq!(
        l0_count(&core, db_hot, tenant_hot, collection),
        over_limit,
        "db_hot should start with {over_limit} L0 segments"
    );

    // Trigger non-forced maintenance compaction.
    let stats = core.run_compaction(false);

    // db_cold: under budget → FTS compaction must have run.
    assert!(
        stats.fts_compacted > 0,
        "fts_compacted must be > 0 when db_cold is under budget; got {}",
        stats.fts_compacted
    );
    let cold_l0_after = l0_count(&core, db_cold, tenant_cold, collection);
    assert!(
        cold_l0_after < over_limit,
        "db_cold L0 count must decrease after compaction (before={over_limit}, after={cold_l0_after})"
    );

    // db_hot: over budget → FTS compaction must have been deferred.
    assert!(
        stats.fts_deferred > 0,
        "fts_deferred must be > 0 when db_hot is over its maintenance budget; got {}",
        stats.fts_deferred
    );
    let hot_l0_after = l0_count(&core, db_hot, tenant_hot, collection);
    assert_eq!(
        hot_l0_after, over_limit,
        "db_hot L0 segments must be unchanged when deferred (expected={over_limit}, got={hot_l0_after})"
    );
    // Enumeration succeeded — the segments table read txn opened cleanly.
    assert!(
        !stats.fts_enumeration_failed,
        "fts_enumeration_failed must be false on a healthy backend"
    );
}

#[test]
fn fts_compaction_force_bypasses_budget() {
    let (mut core, _req_tx, _resp_rx, _dir) = make_core();

    let db_hot = DatabaseId::new(202);
    let tenant_hot = TenantId::new(202);
    let collection = "posts";

    let tracker = Arc::new(MaintenanceBudgetTracker::new());
    tracker.set_cap(db_hot, 1);
    core.set_maintenance_budget(Arc::clone(&tracker));

    // Pre-exhaust db_hot's budget.
    exhaust_budget(&tracker, db_hot, 1);
    assert!(
        tracker.try_acquire(db_hot, 0.0).is_none(),
        "db_hot budget must be exhausted before forced compaction run"
    );

    write_l0_segments(&core, db_hot, tenant_hot, collection, 9);
    assert_eq!(l0_count(&core, db_hot, tenant_hot, collection), 9);

    // Forced compaction must bypass the budget gate.
    let stats = core.run_compaction(true);

    assert!(
        stats.fts_compacted > 0,
        "forced compaction must bypass budget and compact FTS segments; fts_compacted={}",
        stats.fts_compacted
    );
    assert_eq!(
        stats.fts_deferred, 0,
        "forced compaction must not record deferrals"
    );
    assert!(
        !stats.fts_enumeration_failed,
        "fts_enumeration_failed must be false on a healthy backend"
    );
    let l0_after = l0_count(&core, db_hot, tenant_hot, collection);
    assert!(
        l0_after < 9,
        "forced compaction must reduce L0 count (before=9, after={l0_after})"
    );
}

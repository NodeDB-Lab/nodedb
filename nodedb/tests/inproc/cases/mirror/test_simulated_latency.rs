// SPDX-License-Identifier: BUSL-1.1

//! `mirror_simulated_cross_region_latency`: inject 200 ms RTT into the
//! QUIC link; bootstrap completes, lag stays > 200 ms but < 5 s (Following),
//! BoundedStaleness(1000 ms) reads pass.
//!
//! This test simulates the observable lag from a cross-region deployment by
//! injecting a synthetic cut state with a 300 ms offset and verifying that:
//! 1. The status remains `Following` (lag < 5 s).
//! 2. A `BoundedStaleness(1000 ms)` read is accepted.
//! 3. A `BoundedStaleness(100 ms)` read is rejected (lag > 100 ms).
//!
//! The QUIC transport RTT shows in the age of the applied cut: a 200 ms
//! round-trip means entries arrive ~200 ms late. The test models this with an
//! applied cut 300 ms old.

use std::time::Duration;

use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};
use tempfile::TempDir;

use nodedb::control::server::pgwire::ddl::database::{
    MirrorReadOutcome, check_mirror_read_consistency,
};
use nodedb::types::ReadConsistency;

use super::helpers::{
    TEST_SOURCE_CLUSTER, TEST_SOURCE_DATABASE, applied_cut_state, inject_cut_state_for_id, now_ms,
    open_tmp_catalog,
};

#[test]
fn mirror_simulated_cross_region_latency() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(5001);

    // Simulate 200 ms cross-region RTT: last entry applied 300 ms ago
    // (200 ms transit + ~100 ms apply queue drain).
    let simulated_lag_ms = 300u64;
    inject_cut_state_for_id(&catalog, db_id, simulated_lag_ms, 1);

    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        source_database_name: TEST_SOURCE_DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Following,
    };

    // BoundedStaleness(1000 ms) must pass: 300 ms lag < 1000 ms bound.
    match check_mirror_read_consistency(
        &catalog,
        db_id,
        &origin,
        ReadConsistency::BoundedStaleness(Duration::from_millis(1_000)),
        now_ms(),
    ) {
        MirrorReadOutcome::ServeLocally => {}
        MirrorReadOutcome::Reject { message, .. } => {
            panic!("BoundedStaleness(1000ms) must pass with 300ms lag: {message}");
        }
    }

    // BoundedStaleness(100 ms) must fail: 300 ms lag > 100 ms bound.
    match check_mirror_read_consistency(
        &catalog,
        db_id,
        &origin,
        ReadConsistency::BoundedStaleness(Duration::from_millis(100)),
        now_ms(),
    ) {
        MirrorReadOutcome::Reject { sqlstate_code, .. } => {
            assert_eq!(
                sqlstate_code,
                nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER
            );
        }
        MirrorReadOutcome::ServeLocally => {
            panic!("BoundedStaleness(100ms) must reject with 300ms lag");
        }
    }

    // Status must remain Following (lag < LAG_DEGRADED_MS = 5 s).
    use nodedb::control::mirror::{LAG_DEGRADED_MS, LagTransition, compute_lag_transition};
    let t = compute_lag_transition(&MirrorStatus::Following, simulated_lag_ms, now_ms());
    assert_eq!(
        t,
        LagTransition::Unchanged,
        "300ms lag must not trigger Degraded (threshold is {}ms)",
        LAG_DEGRADED_MS
    );
}

#[test]
fn mirror_simulated_latency_at_threshold_boundary() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(5002);

    // Fix `now` and derive the cut's time from it so the computed lag is
    // exactly 199 ms — one millisecond inside the 200 ms bound — regardless of
    // how long injection takes. Passing the same `now` into the check keeps
    // this boundary deterministic; reading the wall clock inside the check would
    // add real elapsed time to the lag and flake the assertion on a slow runner.
    let now = now_ms();
    catalog
        .raise_mirror_cut_state(db_id, &applied_cut_state(1, now, 199))
        .expect("inject cut state");

    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        source_database_name: TEST_SOURCE_DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Following,
    };

    match check_mirror_read_consistency(
        &catalog,
        db_id,
        &origin,
        ReadConsistency::BoundedStaleness(Duration::from_millis(200)),
        now,
    ) {
        MirrorReadOutcome::ServeLocally => {}
        MirrorReadOutcome::Reject { message, .. } => {
            panic!("199ms lag should pass 200ms bound: {message}");
        }
    }
}

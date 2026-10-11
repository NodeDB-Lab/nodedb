// SPDX-License-Identifier: BUSL-1.1

//! Event Plane restart recovery for AFTER triggers.
//!
//! On boot a consumer replays the WAL above its persisted watermark. Each
//! replayed event is held in the trigger action lane again at its replicated
//! position, and the lane's replicated cursor decides whether it fires:
//!
//! - An event the lane already fired sits at or below the cursor. It is
//!   released and never fires again.
//! - An event the lane held and did not fire fires once after the restart.
//!
//! Every case rewinds every core's persisted watermark to zero across a
//! graceful restart, so the whole WAL replays.

use std::time::Duration;

use nodedb::event::cdc::CdcOffset;
use nodedb::event::watermark::WatermarkStore;
use nodedb::types::Lsn;
use nodedb_test_support::pgwire_harness::{TestDataDir, TestServer};

const N: usize = 3;
const SRC: &str = "src";
const CONVERGE: Duration = Duration::from_secs(10);
const STEP: Duration = Duration::from_millis(100);

/// The `fire_log` row count.
async fn fire_log_rows(server: &TestServer) -> usize {
    server
        .query_text("SELECT marker FROM fire_log")
        .await
        .map(|rows| rows.len())
        .unwrap_or_else(|e| panic!("read fire_log: {e}"))
}

/// Poll `fire_log` until it holds `expected` rows. Fails on timeout, or at
/// once when it holds more.
async fn wait_for_fire_log_count(server: &TestServer, expected: usize) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let rows = fire_log_rows(server).await;
        assert!(
            rows <= expected,
            "fire_log holds {rows} row(s), more than the {expected} expected: an action fired twice"
        );
        if rows == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for fire_log to reach {expected} row(s), got {rows} row(s)"
        );
        tokio::time::sleep(STEP).await;
    }
}

/// How many of the source's events the lane holds for firing.
fn held(server: &TestServer) -> usize {
    let Some(ledgers) = server.shared.sink_ledgers.get() else {
        return 0;
    };
    let ledger = &ledgers.actions.ledger;
    ledger
        .partitions()
        .unwrap_or_default()
        .into_iter()
        .map(|partition| {
            ledger
                .held_after(partition, CdcOffset::ZERO, 10_000)
                .map(|held| held.iter().filter(|(_, a)| a.collection == SRC).count())
                .unwrap_or(0)
        })
        .sum()
}

/// Whether every consumer finished its boot catch-up and holds every event
/// its core emitted.
fn caught_up(server: &TestServer) -> bool {
    let Some(ledgers) = server.shared.sink_ledgers.get() else {
        return false;
    };
    let Some(emitted) = server.shared.authorization_fence.emitted_snapshot() else {
        return false;
    };
    ledgers.actions.delivered.delivered_through(&emitted)
}

/// Wait until `condition` holds, or fail naming `what`.
async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting until {what}"
        );
        tokio::time::sleep(STEP).await;
    }
}

/// A server with the source, the log and the trigger that writes the log.
async fn server_with_trigger() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();
    server
        .exec(&format!(
            "CREATE TRIGGER on_ins AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('i'); END;"
        ))
        .await
        .unwrap();
    server
}

async fn insert_rows(server: &TestServer) {
    for i in 0..N {
        server
            .exec(&format!("INSERT INTO {SRC} (id, v) VALUES ('row{i}', {i})"))
            .await
            .unwrap();
    }
}

/// Stop `server`, rewind every core's persisted watermark to zero, and hand
/// back its data directory.
async fn stop_and_rewind(server: TestServer) -> TestDataDir {
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let store = WatermarkStore::open(dir.path()).unwrap();
    for core in 0..64 {
        store.save(core, Lsn::ZERO).unwrap();
    }
    // Dropped before the reopen so the redb file lock is released.
    drop(store);
    dir
}

/// Wait until the restarted server replayed the WAL and its lane passed
/// every source event, then check the log holds `expected` rows.
async fn settles_at(server: &TestServer, expected: usize) {
    wait_until("the boot catch-up held every replayed event", || {
        caught_up(server)
    })
    .await;
    wait_until("the lane cursor passed every source event", || {
        held(server) == 0
    })
    .await;
    assert_eq!(
        fire_log_rows(server).await,
        expected,
        "every source event fires exactly once across the restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_does_not_refire_replayed_events_the_lane_fired() {
    let server = server_with_trigger().await;
    insert_rows(&server).await;
    wait_for_fire_log_count(&server, N).await;
    wait_until("the lane cursor passed every source event", || {
        held(&server) == 0
    })
    .await;

    let dir = stop_and_rewind(server).await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    settles_at(&server, N).await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_fires_held_events_the_lane_had_not_fired_once() {
    use nodedb_types::fail_point::{FailAction, FailGuard};

    let server = server_with_trigger().await;

    // Park the node's firing before the writes, so the lane holds the events
    // and fires none of them before the restart.
    let gates = tempfile::tempdir().unwrap();
    let release = gates.path().join("before_firing");
    let parked = gates.path().join("before_firing.parked");
    let gate = FailGuard::for_node(
        server.shared.node_id,
        "trigger::before_firing",
        FailAction::WaitForFile(release),
    );
    wait_until("the node's firing parks", || parked.exists()).await;

    insert_rows(&server).await;
    wait_until("the lane holds every source event", || held(&server) == N).await;
    assert_eq!(
        fire_log_rows(&server).await,
        0,
        "no event fired while parked"
    );

    let dir = stop_and_rewind(server).await;
    // The restarted node fires freely.
    drop(gate);
    let (server, _dir) = TestServer::open_on_path(dir).await;

    wait_for_fire_log_count(&server, N).await;
    settles_at(&server, N).await;
}

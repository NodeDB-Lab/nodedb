//! Integration test: pgwire listener is gated on GatewayEnable.
//!
//! The test:
//! 1. Builds a minimal node where the startup gate is held at Boot.
//! 2. Binds a real pgwire socket.
//! 3. Launches `pg_listener.run(...)` in a task — it blocks because the gate
//!    has not fired yet.
//! 4. Attempts a real `tokio_postgres::connect` to the bound address.
//!    The TCP connection completes (port is open) but the pgwire handshake
//!    stalls because `accept()` has not been called yet.
//! 5. Fires the gate from the test after 300 ms.
//! 6. Asserts the elapsed time is ≥ 250 ms (gate actually blocked the accept).
//! 7. Asserts the connection now works and `SELECT 1` returns a row.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use nodedb::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use nodedb::bridge::physical_plan::MetaOp;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;
use nodedb::types::Lsn;

mod common;

/// Build a minimal SharedState with a real StartupSequencer, returning the
/// sequencer, the GatewayEnable gate, the Data Plane channel data sides, and
/// the temp dir so the caller can keep them alive for the duration of the test.
fn make_gated_state() -> (
    Arc<SharedState>,
    StartupSequencer,
    nodedb::control::startup::ReadyGate,
    Vec<CoreChannelDataSide>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("gate_test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let mut shared = SharedState::new(dispatcher, wal);

    // Replace the pre-fired placeholder with a real sequencer.
    let (seq, gate) = StartupSequencer::new();
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable-test");

    // Install the real gate on SharedState before any clones.
    Arc::get_mut(&mut shared)
        .expect("SharedState not yet cloned")
        .startup = Arc::clone(&gate);

    (shared, seq, gw_gate, data_sides, dir)
}

/// Spawn a minimal fake Data Plane that echoes `MetaOp::RawResponse` payloads
/// back to the Control Plane. This is required so that `SELECT 1` (which the
/// planner converts to `MetaOp::RawResponse`) can complete.
///
/// The fake reactor runs in a Tokio task (safe here because it only moves the
/// `CoreChannelDataSide` channels — no io_uring or TPC involvement).
fn spawn_fake_data_plane(mut data_side: CoreChannelDataSide) {
    tokio::spawn(async move {
        loop {
            // Poll at 1 ms intervals — this is a test harness, not production.
            tokio::time::sleep(Duration::from_millis(1)).await;

            while let Ok(req) = data_side.request_rx.try_pop() {
                let request_id = req.inner.request_id;

                let payload = match &req.inner.plan {
                    PhysicalPlan::Meta(MetaOp::RawResponse { payload }) => {
                        Payload::from_vec(payload.clone())
                    }
                    _ => Payload::empty(),
                };

                let resp = BridgeResponse {
                    inner: Response {
                        request_id,
                        status: Status::Ok,
                        attempt: 1,
                        partial: false,
                        payload,
                        watermark_lsn: Lsn::ZERO,
                        error_code: None,
                    },
                };

                // Ignore send errors — the control-plane side may have already
                // timed out or dropped its channel in abnormal conditions.
                let _ = data_side.response_tx.try_push(resp);
            }
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_accept_blocked_until_gateway_enable() {
    let (shared, _seq, gw_gate, data_sides, _dir) = make_gated_state();
    let startup_gate = Arc::clone(&shared.startup);

    // Bind a real pgwire socket on an ephemeral port.
    let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("pgwire bind failed");
    let pg_addr = pg_listener.local_addr();

    // Spawn the listener — it will block inside `await_phase(GatewayEnable)`.
    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_pg = Arc::clone(&shared);
    let gate_for_listener = Arc::clone(&startup_gate);
    let bus_pg = shutdown_bus.clone();
    tokio::spawn(async move {
        let _ = pg_listener
            .run(
                shared_pg,
                AuthMode::Trust,
                None,
                Arc::new(tokio::sync::Semaphore::new(128)),
                gate_for_listener,
                bus_pg,
            )
            .await;
    });

    // Spawn the fake Data Plane reactor so that SELECT 1 can complete.
    // data_sides has exactly one entry (we created 1 core above).
    for ds in data_sides {
        spawn_fake_data_plane(ds);
    }

    // Spawn the Control Plane response pump — routes SPSC responses to
    // waiting session oneshots via SharedState::poll_and_route_responses.
    let pump_shared = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            pump_shared.poll_and_route_responses();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    // Give the listener task time to reach `await_phase`.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Start timing. Attempt a TCP + pgwire connect — this will stall until
    // the listener calls `accept()`, which happens only after GatewayEnable.
    let start = Instant::now();

    // Fire the gate after 300 ms in a background task.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        gw_gate.fire();
    });

    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=nodedb connect_timeout=10",
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("pgwire connect failed after gate fired");
    let elapsed = start.elapsed();

    // The connection must have taken at least 250 ms (gate was held for 300 ms).
    assert!(
        elapsed >= Duration::from_millis(250),
        "pgwire connection succeeded too fast ({elapsed:?}): gate did not block accept"
    );

    // Drive the connection.
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Verify the connection works.
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("SELECT 1 failed");
    assert_eq!(rows.len(), 1, "expected 1 row from SELECT 1");
}

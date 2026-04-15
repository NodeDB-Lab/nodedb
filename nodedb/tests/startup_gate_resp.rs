//! Integration test: RESP listener is gated on GatewayEnable.
//!
//! The test:
//! 1. Builds a minimal node with a real StartupSequencer (gate held).
//! 2. Binds a real RESP socket.
//! 3. Launches `resp_listener.run(...)` in a task — it blocks at `await_phase`.
//! 4. Opens a raw TCP connection to the bound port (TCP handshake succeeds).
//! 5. Sends a RESP `PING\r\n` inline command.
//! 6. Fires the gate after 300 ms in a background task.
//! 7. Asserts the PONG reply arrives only after ≥ 250 ms.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::server::resp::listener::RespListener;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;

mod common;

fn make_gated_state() -> (
    Arc<SharedState>,
    StartupSequencer,
    nodedb::control::startup::ReadyGate,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("gate_resp_test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let mut shared = SharedState::new(dispatcher, wal);

    let (seq, gate) = StartupSequencer::new();
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable-resp-test");

    Arc::get_mut(&mut shared)
        .expect("SharedState not yet cloned")
        .startup = Arc::clone(&gate);

    (shared, seq, gw_gate, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_accept_blocked_until_gateway_enable() {
    let (shared, _seq, gw_gate, _dir) = make_gated_state();
    let startup_gate = Arc::clone(&shared.startup);

    // Bind a real RESP socket on an ephemeral port.
    let resp_listener = RespListener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("RESP bind failed");
    let resp_addr = resp_listener.addr();

    // Spawn the listener — it blocks inside `await_phase(GatewayEnable)`.
    let (shutdown_bus, _) =
        nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let shared_resp = Arc::clone(&shared);
    let gate_for_listener = Arc::clone(&startup_gate);
    let bus_resp = shutdown_bus.clone();
    tokio::spawn(async move {
        let _ = resp_listener
            .run(
                shared_resp,
                Arc::new(tokio::sync::Semaphore::new(128)),
                None,
                gate_for_listener,
                bus_resp,
            )
            .await;
    });

    // Give the listener task time to reach `await_phase`.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Open a raw TCP connection — TCP handshake will succeed immediately.
    let mut stream = tokio::net::TcpStream::connect(resp_addr)
        .await
        .expect("TCP connect to RESP port failed");

    // Start timing before sending the PING.
    let start = Instant::now();

    // Fire the gate after 300 ms in a background task.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        gw_gate.fire();
    });

    // Send a RESP inline PING command.
    stream
        .write_all(b"PING\r\n")
        .await
        .expect("write PING failed");

    // Read the PONG response (+PONG\r\n).
    let mut buf = vec![0u8; 32];
    let n = stream.read(&mut buf).await.expect("read PONG failed");
    let elapsed = start.elapsed();

    let response = std::str::from_utf8(&buf[..n]).unwrap_or("");
    assert!(
        response.contains("PONG"),
        "expected PONG in RESP response, got: {response:?}"
    );

    assert!(
        elapsed >= Duration::from_millis(250),
        "RESP response arrived too fast ({elapsed:?}): gate did not block accept"
    );
}

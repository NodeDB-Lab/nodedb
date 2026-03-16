use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::wal::WalManager;

/// End-to-end test: psql-compatible client connects via pgwire,
/// sends a query, and gets a response.
#[tokio::test]
async fn pgwire_connect_and_query() {
    // Set up infrastructure (mirrors session::tests::full_request_response_roundtrip).
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal);

    // Start a Data Plane core in a background thread.
    let data_side = data_sides.into_iter().next().unwrap();
    let core_dir = dir.path().to_path_buf();
    let core_handle = tokio::task::spawn_blocking(move || {
        let mut core =
            CoreLoop::open(0, data_side.request_rx, data_side.response_tx, &core_dir).unwrap();
        for _ in 0..10_000 {
            core.tick();
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // Start response poller.
    let shared_poller = Arc::clone(&shared);
    let poller_handle = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_poller.poll_and_route_responses();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    // Bind pgwire listener on random port.
    let pg_listener = PgListener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let pg_addr = pg_listener.local_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shared_pg = Arc::clone(&shared);
    let pg_handle = tokio::spawn(async move {
        pg_listener
            .run(shared_pg, AuthMode::Trust, shutdown_rx)
            .await
            .unwrap();
    });

    // Give listener a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect using tokio-postgres (a real PostgreSQL client).
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("pgwire connect failed");

    // Spawn connection handler.
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Test 1: Simple query that NodeDB handles (SET command).
    let result = client.simple_query("SET client_encoding = 'UTF8'").await;
    assert!(result.is_ok(), "SET command failed: {:?}", result.err());

    // Test 2: The connection is alive and responsive.
    // Send a query that will go through DataFusion planning.
    // This will likely error (no table registered), but proves the full path works.
    let result = client.simple_query("SELECT 1").await;
    // We expect this might error since DataFusion may not have a table, but the
    // pgwire protocol exchange should complete without a connection-level failure.
    match &result {
        Ok(msgs) => {
            println!("SELECT 1 returned {} messages", msgs.len());
            for msg in msgs {
                match msg {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        println!("  Row: {:?}", row.get(0));
                    }
                    tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                        println!("  CommandComplete: {n}");
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {
            // A SQL-level error returned via pgwire ErrorResponse is OK —
            // it means the protocol is working correctly.
            println!("SELECT 1 returned error (expected): {e}");
        }
    }

    // Test 3: Connection is still alive after the query (error didn't kill it).
    let result2 = client.simple_query("SET search_path = 'public'").await;
    assert!(
        result2.is_ok(),
        "Connection died after query: {:?}",
        result2.err()
    );

    // Clean up.
    drop(client);
    let _ = conn_handle.await;
    let _ = shutdown_tx.send(true);
    let _ = pg_handle.await;
    let _ = poller_handle.await;
    let _ = core_handle.await;
}

// SPDX-License-Identifier: BUSL-1.1

//! Shared bring-up helpers used by every `TestServer` constructor: the
//! native-protocol listener bind, and the memory-governor wiring.

use std::sync::Arc;

use nodedb::config::auth::AuthMode;
use nodedb::control::server::listener::{Listener, ListenerRunParams};
use nodedb::control::state::SharedState;

/// Build a `MemoryGovernor` for integration tests using the **production**
/// wiring (`nodedb::memory::init_governor` over a default `EngineConfig`),
/// so the harness can never diverge from how a real server distributes its
/// memory budget. A hand-rolled all-engines map here once masked a bug
/// where production registered only a subset of engines and the first write
/// to any unregistered engine was rejected with `resources exhausted`.
///
/// An 8 GiB ceiling keeps even the smallest per-engine slice generous
/// enough that integration workloads never trip engine-level pressure.
/// Panics if `GovernorConfig` validation fails. Validation rejects a config
/// whose per-engine limits outgrow the ceiling. `EngineConfig::default()`
/// fractions sum below 1.0, so the derived budgets fit at any ceiling.
pub(super) fn init_test_memory_governor() -> Arc<nodedb_mem::MemoryGovernor> {
    let ceiling: usize = 8 * 1024 * 1024 * 1024; // 8 GiB
    let budgets = nodedb::config::EngineConfig::default().to_byte_budgets(ceiling);
    nodedb::memory::init_governor(ceiling, &budgets).expect("harness governor config is valid")
}

/// The one node that leads every group in a single-node routing table.
///
/// Panics when the table names no leader, or more than one: a single-node
/// harness cannot serve a group another node leads.
pub(super) fn single_routing_leader(routing: &nodedb_cluster::RoutingTable) -> u64 {
    let mut leaders: Vec<u64> = routing
        .group_members()
        .values()
        .map(|group| group.leader)
        .collect();
    leaders.sort_unstable();
    leaders.dedup();
    match leaders.as_slice() {
        [leader] if *leader != 0 => *leader,
        other => panic!("single-node harness routing must name one leader, got {other:?}"),
    }
}

/// Bind a native (MessagePack) protocol listener on `127.0.0.1:0` and
/// spawn its accept loop. Returns the listener's local port plus the
/// handle to await on shutdown.
pub(super) async fn bind_native_listener(
    shared: &Arc<SharedState>,
    shutdown_bus: &nodedb::control::shutdown::ShutdownBus,
    conn_semaphore: Arc<tokio::sync::Semaphore>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = Listener::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind native listener");
    let port = listener.local_addr().port();
    let state = Arc::clone(shared);
    let startup_gate = Arc::clone(&shared.startup);
    let bus = shutdown_bus.clone();
    // The registry `SharedState` carries, not a fresh one: quota DDL installs
    // its caps there. A private registry leaves the native listener enforcing
    // an empty set of caps.
    let admission = Arc::clone(&shared.admission_registry);
    let handle = tokio::spawn(async move {
        let _ = listener
            .run(ListenerRunParams {
                state,
                auth_mode: AuthMode::Trust,
                tls_acceptor: None,
                conn_semaphore,
                startup_gate,
                bus,
                admission,
            })
            .await;
    });
    (port, handle)
}

/// Bind an HTTP (REST/axum) listener on `127.0.0.1:0` and spawn its accept
/// loop. Returns the listener's local port plus the handle to await on
/// shutdown.
pub(super) async fn bind_http_listener(
    shared: &Arc<SharedState>,
    shutdown_bus: &nodedb::control::shutdown::ShutdownBus,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http listener");
    let port = listener.local_addr().expect("http local addr").port();
    let shared_clone = Arc::clone(shared);
    let bus_clone = shutdown_bus.clone();
    let handle = tokio::spawn(async move {
        let _ = nodedb::control::server::http::server::run_with_listener(
            listener,
            shared_clone,
            AuthMode::Trust,
            None,
            bus_clone,
        )
        .await;
    });
    (port, handle)
}

/// Runs the tenant rate-counter reset the server drives from
/// `spawn_background_loops`, which this harness does not start.
///
/// `requests_this_second` only means "this second" while something clears it.
/// Without this it is a running total, so a tenant is refused for good once a
/// test issues more requests than its quota, and the error names a rate the
/// test never reached.
pub(super) fn reset_tenant_rate_counters_each_second(
    shared: &SharedState,
    last_reset: &mut std::time::Instant,
) {
    if last_reset.elapsed() >= std::time::Duration::from_secs(1) {
        shared.reset_tenant_rate_counters();
        *last_reset = std::time::Instant::now();
    }
}

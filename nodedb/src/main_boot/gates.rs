// SPDX-License-Identifier: BUSL-1.1

//! Startup-gate registration for the boot sequence.

use nodedb::control::startup::{ReadyGate, StartupPhase, StartupSequencer};

/// Every gate handle `main()` needs after registering all startup phases
/// up front, bundled so the registration call site doesn't juggle ten
/// separate `let`s. Phases that have no concurrent sub-tasks get a
/// single gate that is fired inline.
pub(crate) struct StartupGates {
    pub(crate) wal_gate: ReadyGate,
    pub(crate) catalog_gate: ReadyGate,
    pub(crate) raft_gate: ReadyGate,
    pub(crate) schema_gate: ReadyGate,
    pub(crate) sanity_gate: ReadyGate,
    pub(crate) data_plane_replay_gate: ReadyGate,
    pub(crate) data_groups_gate: ReadyGate,
    pub(crate) transport_gate: ReadyGate,
    pub(crate) warm_peers_gate: ReadyGate,
    pub(crate) health_loop_gate: ReadyGate,
    pub(crate) gateway_enable_gate: ReadyGate,
    /// Fired where the client protocols start listening.
    pub(crate) serving_gate: ReadyGate,
}

/// Register all gates up-front so the sequencer knows every phase has
/// an owner, kept out of `main()` for readability.
pub(crate) fn register_startup_gates(startup_seq: &StartupSequencer) -> StartupGates {
    let wal_gate = startup_seq.register_gate(StartupPhase::WalRecovery, "wal");
    let catalog_gate =
        startup_seq.register_gate(StartupPhase::ClusterCatalogOpen, "cluster-catalog");
    let data_plane_replay_gate =
        startup_seq.register_gate(StartupPhase::DataPlaneReplay, "data-plane-replay");
    let raft_gate =
        startup_seq.register_gate(StartupPhase::RaftMetadataReplay, "raft-metadata-replay");
    let schema_gate =
        startup_seq.register_gate(StartupPhase::SchemaCacheWarmup, "schema-cache-warmup");
    let sanity_gate =
        startup_seq.register_gate(StartupPhase::CatalogSanityCheck, "catalog-sanity-check");
    let data_groups_gate =
        startup_seq.register_gate(StartupPhase::DataGroupsReplay, "data-groups-replay");
    let transport_gate = startup_seq.register_gate(StartupPhase::TransportBind, "transport-bind");
    let warm_peers_gate = startup_seq.register_gate(StartupPhase::WarmPeers, "warm-peers");
    let health_loop_gate = startup_seq.register_gate(StartupPhase::HealthLoopStart, "health-loop");
    let gateway_enable_gate =
        startup_seq.register_gate(StartupPhase::GatewayEnable, "gateway-enable");
    let serving_gate = startup_seq.register_gate(StartupPhase::Serving, "client-listeners");

    StartupGates {
        wal_gate,
        catalog_gate,
        raft_gate,
        schema_gate,
        sanity_gate,
        data_plane_replay_gate,
        data_groups_gate,
        transport_gate,
        warm_peers_gate,
        health_loop_gate,
        gateway_enable_gate,
        serving_gate,
    }
}

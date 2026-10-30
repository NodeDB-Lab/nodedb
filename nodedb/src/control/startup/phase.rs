// SPDX-License-Identifier: BUSL-1.1

//! Canonical startup phases, ordered from process entry to
//! the moment client-facing listeners begin processing
//! requests.
//!
//! Phases advance strictly sequentially via the gate-based
//! [`StartupSequencer`]. The underlying `u8` repr is kept stable
//! so the sequencer can carry the current phase in an `AtomicU8`
//! without a typed swap primitive.
//!
//! [`StartupSequencer`]: super::startup_sequencer::StartupSequencer

use std::fmt;

/// Total number of phases. Kept in sync with the enum below by
/// the `phase_order_matches_u8` unit test.
pub const PHASE_COUNT: usize = 14;

/// Startup phase. Ordered — use `Ord` / `PartialOrd` to compare.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum StartupPhase {
    /// Pre-startup default. Every `Sequencer` begins here.
    Boot = 0,
    /// WAL segments replayed, LSN watermark known.
    WalRecovery = 1,
    /// `SystemCatalog` (redb) opened, credential store loaded.
    ClusterCatalogOpen = 2,
    /// Every Data Plane core finished its WAL replay and drains its request
    /// ring. A request sent to a core before this waits in the ring, so boot
    /// work that dispatches to the cores waits for this phase first.
    DataPlaneReplay = 3,
    /// Metadata raft group's committed log has been applied up
    /// to the last-observed index. `MetadataCache.applied_index`
    /// is current.
    RaftMetadataReplay = 4,
    /// Post-apply hooks rebuilt every in-memory registry
    /// (triggers, streams, schedules, permissions, etc.) from
    /// the now-fresh redb state.
    SchemaCacheWarmup = 5,
    /// Applied-index gate, redb cross-table integrity, and
    /// in-memory registry ⇔ redb verification have all run
    /// without raising unrepairable divergences. See
    /// `control::cluster::recovery_check`.
    CatalogSanityCheck = 6,
    /// All data raft groups (vShards hosting data) have caught
    /// up to their committed watermark.
    DataGroupsReplay = 7,
    /// Listener sockets bound (pgwire / HTTP / ILP / RESP /
    /// native). Not yet accepting requests.
    TransportBind = 8,
    /// Parallel dials completed against every known peer so
    /// the QUIC peer cache is hot before any replicated
    /// request fires.
    WarmPeers = 9,
    /// Health monitor running.
    HealthLoopStart = 10,
    /// Cluster readiness completed: the node holds its first authorization
    /// lease and its gateway is installed. Listener accept loops may process
    /// requests once they exist. The client protocols do not listen yet.
    GatewayEnable = 11,
    /// The client protocols listen and every HTTP route answers. The final
    /// phase: the node is ready, and `/healthz` reports `ok`.
    Serving = 12,
    /// Terminal state — entered via [`StartupSequencer::fail`] or
    /// when a [`ReadyGate`] is dropped without firing. All
    /// [`StartupGate::await_phase`] waiters wake with an error.
    ///
    /// [`StartupSequencer::fail`]: super::startup_sequencer::StartupSequencer::fail
    /// [`ReadyGate`]: super::gate::ReadyGate
    /// [`StartupGate::await_phase`]: super::gate::StartupGate::await_phase
    Failed = 13,
}

impl StartupPhase {
    /// Stable human-readable phase name. Used in logs,
    /// `/health`, and the `Display` impl.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Boot => "boot",
            Self::WalRecovery => "wal_recovery",
            Self::ClusterCatalogOpen => "cluster_catalog_open",
            Self::DataPlaneReplay => "data_plane_replay",
            Self::RaftMetadataReplay => "raft_metadata_replay",
            Self::SchemaCacheWarmup => "schema_cache_warmup",
            Self::CatalogSanityCheck => "catalog_sanity_check",
            Self::DataGroupsReplay => "data_groups_replay",
            Self::TransportBind => "transport_bind",
            Self::WarmPeers => "warm_peers",
            Self::HealthLoopStart => "health_loop_start",
            Self::GatewayEnable => "gateway_enable",
            Self::Serving => "serving",
            Self::Failed => "failed",
        }
    }

    /// Next phase in the sequence, or `None` for terminal states.
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Boot => Some(Self::WalRecovery),
            Self::WalRecovery => Some(Self::ClusterCatalogOpen),
            Self::ClusterCatalogOpen => Some(Self::DataPlaneReplay),
            Self::DataPlaneReplay => Some(Self::RaftMetadataReplay),
            Self::RaftMetadataReplay => Some(Self::SchemaCacheWarmup),
            Self::SchemaCacheWarmup => Some(Self::CatalogSanityCheck),
            Self::CatalogSanityCheck => Some(Self::DataGroupsReplay),
            Self::DataGroupsReplay => Some(Self::TransportBind),
            Self::TransportBind => Some(Self::WarmPeers),
            Self::WarmPeers => Some(Self::HealthLoopStart),
            Self::HealthLoopStart => Some(Self::GatewayEnable),
            Self::GatewayEnable => Some(Self::Serving),
            Self::Serving => None,
            Self::Failed => None,
        }
    }

    /// Decode from a `u8` — used by the `AtomicU8`-backed
    /// sequencer state.
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Boot),
            1 => Some(Self::WalRecovery),
            2 => Some(Self::ClusterCatalogOpen),
            3 => Some(Self::DataPlaneReplay),
            4 => Some(Self::RaftMetadataReplay),
            5 => Some(Self::SchemaCacheWarmup),
            6 => Some(Self::CatalogSanityCheck),
            7 => Some(Self::DataGroupsReplay),
            8 => Some(Self::TransportBind),
            9 => Some(Self::WarmPeers),
            10 => Some(Self::HealthLoopStart),
            11 => Some(Self::GatewayEnable),
            12 => Some(Self::Serving),
            13 => Some(Self::Failed),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for StartupPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_order_matches_u8() {
        // Sequential ordering matches the repr(u8) values. If
        // a new phase is inserted mid-sequence, PHASE_COUNT
        // must be bumped to keep this test honest.
        let ordered = [
            StartupPhase::Boot,
            StartupPhase::WalRecovery,
            StartupPhase::ClusterCatalogOpen,
            StartupPhase::DataPlaneReplay,
            StartupPhase::RaftMetadataReplay,
            StartupPhase::SchemaCacheWarmup,
            StartupPhase::CatalogSanityCheck,
            StartupPhase::DataGroupsReplay,
            StartupPhase::TransportBind,
            StartupPhase::WarmPeers,
            StartupPhase::HealthLoopStart,
            StartupPhase::GatewayEnable,
            StartupPhase::Serving,
            StartupPhase::Failed,
        ];
        assert_eq!(ordered.len(), PHASE_COUNT);
        for (i, phase) in ordered.iter().enumerate() {
            assert_eq!(phase.as_u8() as usize, i);
            assert_eq!(StartupPhase::from_u8(i as u8), Some(*phase));
        }
        // Monotonic comparisons on the non-terminal sequence.
        for pair in ordered[..PHASE_COUNT - 1].windows(2) {
            assert!(pair[0] < pair[1]);
        }
    }

    #[test]
    fn next_chain_terminates_at_serving() {
        let mut cur = StartupPhase::Boot;
        let mut count = 1;
        while let Some(n) = cur.next() {
            cur = n;
            count += 1;
            if count > PHASE_COUNT {
                panic!("phase chain failed to terminate");
            }
        }
        assert_eq!(cur, StartupPhase::Serving);
    }

    #[test]
    fn failed_has_no_next() {
        assert_eq!(StartupPhase::Failed.next(), None);
    }

    #[test]
    fn from_u8_rejects_unknown() {
        assert_eq!(StartupPhase::from_u8(99), None);
    }

    #[test]
    fn names_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..PHASE_COUNT as u8 {
            let p = StartupPhase::from_u8(i).unwrap();
            assert!(seen.insert(p.name()), "duplicate name for {p:?}");
        }
    }

    #[test]
    fn display_matches_name() {
        assert_eq!(StartupPhase::GatewayEnable.to_string(), "gateway_enable");
    }
}

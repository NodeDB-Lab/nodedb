//! SWIM subsystem bootstrap.
//!
//! [`spawn`] is the one-stop entry point callers (cluster startup or
//! tests) use to stand up a running failure detector:
//!
//! 1. Constructs a [`MembershipList`] containing the local node at
//!    incarnation 0.
//! 2. Seeds the list with an `Alive` entry for every address in
//!    `seeds`, using a synthetic `NodeId` of the form `"seed:<addr>"`.
//!    The first successful probe replaces the placeholder with the
//!    peer's real node id via the normal merge path.
//! 3. Validates [`SwimConfig`] and constructs a [`FailureDetector`].
//! 4. Spawns the detector's run loop on a fresh tokio task.
//! 5. Returns a [`SwimHandle`] the caller can use to read membership,
//!    access the dissemination queue, and shut the detector down.

use std::net::SocketAddr;
use std::sync::Arc;

use nodedb_types::NodeId;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::config::SwimConfig;
use super::detector::{FailureDetector, ProbeScheduler, Transport};
use super::dissemination::DisseminationQueue;
use super::error::SwimError;
use super::incarnation::Incarnation;
use super::member::MemberState;
use super::member::record::MemberUpdate;
use super::membership::MembershipList;

/// Owns a running SWIM detector and its shutdown plumbing.
///
/// Dropping `SwimHandle` leaks the background task — callers should
/// always invoke [`SwimHandle::shutdown`] to request graceful drain.
pub struct SwimHandle {
    detector: Arc<FailureDetector>,
    membership: Arc<MembershipList>,
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl SwimHandle {
    /// Shared reference to the detector (for metrics, debugging, or
    /// injecting synthetic rumours in tests).
    pub fn detector(&self) -> &Arc<FailureDetector> {
        &self.detector
    }

    /// Shared reference to the membership list. Clone cheaply; the
    /// underlying `Arc` is identical to the detector's view.
    pub fn membership(&self) -> &Arc<MembershipList> {
        &self.membership
    }

    /// Shared reference to the dissemination queue. Used by callers
    /// that want to enqueue rumours from outside SWIM (e.g. the raft
    /// layer announcing a conf change).
    pub fn dissemination(&self) -> &Arc<DisseminationQueue> {
        self.detector.dissemination()
    }

    /// Signal the detector to shut down and await its task to finish.
    /// Returns whatever error the join handle surfaced (normally none).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

/// Bring up a SWIM failure detector.
///
/// * `cfg` — validated [`SwimConfig`]. An invalid config returns
///   [`SwimError::InvalidConfig`] before any task is spawned.
/// * `local_id` — this node's canonical id.
/// * `local_addr` — the socket address the transport is already bound
///   to. The membership list stores it verbatim for peers to echo back
///   in probe responses.
/// * `seeds` — initial peer addresses. Empty list is legal and yields a
///   solo-cluster detector that does nothing interesting until a peer
///   arrives via an external join.
/// * `transport` — any [`Transport`] impl (UDP in production, the
///   in-memory fabric in tests).
pub async fn spawn(
    cfg: SwimConfig,
    local_id: NodeId,
    local_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    transport: Arc<dyn Transport>,
) -> Result<SwimHandle, SwimError> {
    cfg.validate()?;

    let membership = Arc::new(MembershipList::new_local(
        local_id.clone(),
        local_addr,
        cfg.initial_incarnation,
    ));

    // Seed the membership table so the first probe round has somewhere
    // to go. Placeholder ids are replaced on the first ack.
    for seed_addr in &seeds {
        if *seed_addr == local_addr {
            continue;
        }
        membership.apply(&MemberUpdate {
            node_id: NodeId::new(format!("seed:{seed_addr}")),
            addr: seed_addr.to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::ZERO,
        });
    }

    let initial_inc = cfg.initial_incarnation;
    let detector = Arc::new(FailureDetector::new(
        cfg,
        Arc::clone(&membership),
        transport,
        ProbeScheduler::new(),
    ));

    // Prime the dissemination queue with our own Alive record so the
    // first outgoing probes advertise our canonical NodeId + addr to
    // every seed. Without this, seed placeholders would never be
    // replaced with real ids until some peer independently learned
    // our identity — which is not reliable from seed bootstrap alone.
    detector.dissemination().enqueue(MemberUpdate {
        node_id: local_id.clone(),
        addr: local_addr.to_string(),
        state: MemberState::Alive,
        incarnation: initial_inc,
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let join = tokio::spawn({
        let detector = Arc::clone(&detector);
        async move { detector.run(shutdown_rx).await }
    });

    Ok(SwimHandle {
        detector,
        membership,
        shutdown_tx,
        join,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::detector::TransportFabric;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    #[tokio::test]
    async fn spawn_solo_cluster_has_only_local() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7100)).await);
        let handle = spawn(cfg(), NodeId::new("a"), addr(7100), vec![], transport)
            .await
            .expect("spawn");
        assert_eq!(handle.membership().len(), 1);
        assert!(handle.membership().is_solo());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_seeds_populates_membership() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7110)).await);
        let handle = spawn(
            cfg(),
            NodeId::new("a"),
            addr(7110),
            vec![addr(7111), addr(7112)],
            transport,
        )
        .await
        .expect("spawn");
        assert_eq!(handle.membership().len(), 3);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_skips_local_addr_in_seeds() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7120)).await);
        let handle = spawn(
            cfg(),
            NodeId::new("a"),
            addr(7120),
            vec![addr(7120), addr(7121)],
            transport,
        )
        .await
        .expect("spawn");
        // Local + one real seed = 2.
        assert_eq!(handle.membership().len(), 2);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_config_rejected_before_task_spawned() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7130)).await);
        let mut bad = cfg();
        bad.probe_timeout = bad.probe_interval; // violates the strict-less rule
        let res = spawn(bad, NodeId::new("a"), addr(7130), vec![], transport).await;
        match res {
            Err(SwimError::InvalidConfig { .. }) => {}
            Err(other) => panic!("expected InvalidConfig, got {other:?}"),
            Ok(_) => panic!("expected InvalidConfig error"),
        }
    }

    #[tokio::test]
    async fn shutdown_joins_promptly() {
        let fab = TransportFabric::new();
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7140)).await);
        let handle = spawn(cfg(), NodeId::new("a"), addr(7140), vec![], transport)
            .await
            .expect("spawn");
        let start = std::time::Instant::now();
        tokio::time::timeout(Duration::from_millis(500), handle.shutdown())
            .await
            .expect("shutdown did not join within budget");
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}

//! `DecommissionObserver` — local-node self-shutdown signal.
//!
//! The coordinator proposes a full decommission plan through the
//! metadata Raft group. Every node (including the target itself)
//! applies the resulting entries through `CacheApplier`, which, when
//! attached with [`CacheApplier::with_live_state`](crate::metadata_group::CacheApplier::with_live_state),
//! cascades topology state transitions into the live
//! `Arc<RwLock<ClusterTopology>>` handle.
//!
//! The observer polls that handle for the *local* node id. Once the
//! node's own state reaches `Decommissioned` — or the node has been
//! removed from topology entirely by a committed `Leave` — the
//! observer flips a `tokio::sync::watch` channel to `true`, which is
//! the cooperative shutdown signal every long-lived background task
//! on this node is already listening on.
//!
//! This is the last link in the decommission chain: once the watch
//! is flipped, the raft loops, SWIM detector, reachability driver,
//! and transport accept loops all drain and exit on their own.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::interval;
use tracing::{info, warn};

use crate::topology::{ClusterTopology, NodeState};

/// Periodically checks the local node's topology state and fires a
/// shutdown signal on `Decommissioned` or removal.
pub struct DecommissionObserver {
    topology: Arc<RwLock<ClusterTopology>>,
    local_node_id: u64,
    shutdown_tx: watch::Sender<bool>,
    poll_interval: Duration,
}

impl DecommissionObserver {
    /// Build an observer and return it alongside the receiver half of
    /// its shutdown watch channel. Every subsystem that wants to
    /// cooperatively drain on decommission can call
    /// [`watch::Receiver::clone`] on the returned receiver.
    pub fn new(
        topology: Arc<RwLock<ClusterTopology>>,
        local_node_id: u64,
        poll_interval: Duration,
    ) -> (Self, watch::Receiver<bool>) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        (
            Self {
                topology,
                local_node_id,
                shutdown_tx,
                poll_interval,
            },
            shutdown_rx,
        )
    }

    /// Single check. Returns `true` iff the observer fired the
    /// shutdown signal during this call (or had already fired it
    /// previously — the watch is level-triggered, not edge).
    pub fn check_once(&self) -> bool {
        if *self.shutdown_tx.borrow() {
            return true;
        }
        let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
        let should_fire = match topo.get_node(self.local_node_id) {
            Some(node) => node.state == NodeState::Decommissioned,
            // Node is gone from topology — either a committed `Leave`
            // (post-decommission) or manual removal. Either way, we
            // are no longer part of the cluster.
            None => true,
        };
        if should_fire {
            info!(
                local_node_id = self.local_node_id,
                "decommission observer firing local shutdown signal"
            );
            if let Err(e) = self.shutdown_tx.send(true) {
                warn!(error = %e, "shutdown watch receivers all dropped");
            }
            return true;
        }
        false
    }

    /// Run the observer's poll loop until `cancel` flips to `true`.
    /// Exits immediately after firing its own shutdown signal —
    /// there is nothing more to watch.
    pub async fn run(self, mut cancel: watch::Receiver<bool>) {
        let mut tick = interval(self.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                changed = cancel.changed() => {
                    if changed.is_ok() && *cancel.borrow() {
                        return;
                    }
                }
                _ = tick.tick() => {
                    if self.check_once() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::NodeInfo;
    use std::net::SocketAddr;

    fn topo_with(node_id: u64, state: NodeState) -> Arc<RwLock<ClusterTopology>> {
        let mut t = ClusterTopology::new();
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        t.add_node(NodeInfo::new(node_id, addr, state));
        Arc::new(RwLock::new(t))
    }

    #[test]
    fn check_once_does_not_fire_while_active() {
        let topo = topo_with(5, NodeState::Active);
        let (obs, _rx) = DecommissionObserver::new(topo, 5, Duration::from_millis(10));
        assert!(!obs.check_once());
    }

    #[test]
    fn check_once_fires_on_decommissioned_state() {
        let topo = topo_with(5, NodeState::Active);
        let (obs, mut rx) = DecommissionObserver::new(topo.clone(), 5, Duration::from_millis(10));
        assert!(!obs.check_once());
        topo.write()
            .unwrap()
            .set_state(5, NodeState::Decommissioned);
        assert!(obs.check_once());
        assert!(*rx.borrow_and_update());
    }

    #[test]
    fn check_once_fires_when_node_removed_from_topology() {
        let topo = topo_with(5, NodeState::Active);
        let (obs, _rx) = DecommissionObserver::new(topo.clone(), 5, Duration::from_millis(10));
        topo.write().unwrap().remove_node(5);
        assert!(obs.check_once());
    }

    #[test]
    fn check_once_is_idempotent_after_firing() {
        let topo = topo_with(5, NodeState::Decommissioned);
        let (obs, _rx) = DecommissionObserver::new(topo, 5, Duration::from_millis(10));
        assert!(obs.check_once());
        // Second call sees the fired signal and reports true again.
        assert!(obs.check_once());
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_fires_shutdown_and_exits() {
        let topo = topo_with(5, NodeState::Active);
        let (obs, mut rx) = DecommissionObserver::new(topo.clone(), 5, Duration::from_millis(50));
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { obs.run(cancel_rx).await });

        // Advance twice — first tick = no-op, then flip state.
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        topo.write()
            .unwrap()
            .set_state(5, NodeState::Decommissioned);
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;

        let _ = tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("observer run loop did not exit");
        assert!(*rx.borrow_and_update());
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_exits_on_cancel_without_firing() {
        let topo = topo_with(5, NodeState::Active);
        let (obs, rx) = DecommissionObserver::new(topo, 5, Duration::from_millis(50));
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { obs.run(cancel_rx).await });
        let _ = cancel_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("cancel did not end run loop");
        assert!(!*rx.borrow());
    }
}

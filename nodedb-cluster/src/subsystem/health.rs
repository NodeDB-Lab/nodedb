//! Subsystem health tracking — per-subsystem state and cluster-wide aggregation.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

/// The lifecycle health state of a single subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubsystemHealth {
    /// The subsystem is in the process of starting up.
    Starting,
    /// The subsystem is fully operational.
    Running,
    /// The subsystem is gracefully winding down.
    Draining,
    /// The subsystem has stopped cleanly.
    Stopped,
    /// The subsystem has stopped due to an unrecoverable error.
    Failed { reason: String },
}

impl SubsystemHealth {
    /// Returns `true` if this state is considered healthy (Starting or Running).
    pub fn is_healthy(&self) -> bool {
        matches!(self, SubsystemHealth::Starting | SubsystemHealth::Running)
    }
}

/// Cluster-wide health aggregator.
///
/// Each subsystem writes its own `SubsystemHealth` into this shared map.
/// Callers can inspect individual entries or derive a summary.
#[derive(Clone, Default)]
pub struct ClusterHealth {
    inner: Arc<RwLock<HashMap<&'static str, SubsystemHealth>>>,
}

impl ClusterHealth {
    /// Create a new, empty health aggregator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a health update for the named subsystem.
    pub async fn set(&self, name: &'static str, health: SubsystemHealth) {
        let mut map = self.inner.write().await;
        map.insert(name, health);
    }

    /// Retrieve the current health for the named subsystem, or `None` if
    /// it has never reported.
    pub async fn get(&self, name: &'static str) -> Option<SubsystemHealth> {
        let map = self.inner.read().await;
        map.get(name).cloned()
    }

    /// Returns `true` when every registered subsystem reports `Running`.
    pub async fn all_running(&self) -> bool {
        let map = self.inner.read().await;
        map.values().all(|h| *h == SubsystemHealth::Running)
    }

    /// Returns the names of any subsystem currently in a `Failed` state.
    pub async fn failed_subsystems(&self) -> Vec<&'static str> {
        let map = self.inner.read().await;
        map.iter()
            .filter_map(|(name, h)| {
                if matches!(h, SubsystemHealth::Failed { .. }) {
                    Some(*name)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns a point-in-time snapshot of all subsystem health entries.
    pub async fn snapshot(&self) -> HashMap<&'static str, SubsystemHealth> {
        self.inner.read().await.clone()
    }
}

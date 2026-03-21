//! N-1 rolling upgrade compatibility.
//!
//! Supports mixed-version cluster operation where nodes running version N
//! coexist with nodes running version N-1. This enables zero-downtime
//! upgrades by rolling through nodes one at a time.
//!
//! ## Compatibility Rules
//!
//! - Wire format: version N accepts messages from N and N-1
//! - WAL format: version N reads WAL records from N and N-1
//! - Raft log: version N applies entries from N and N-1
//! - New features: only activated after ALL nodes upgraded (quorum check)
//!
//! ## Upgrade Procedure
//!
//! 1. Upgrade one node at a time (start with followers, leader last)
//! 2. Each upgraded node runs in "compat mode" (accepts N-1 messages)
//! 3. After all nodes upgraded, send `ACTIVATE_FEATURES` via metadata group
//! 4. New features become available cluster-wide

use crate::version::{MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION};

/// Cluster-wide version state.
#[derive(Debug, Clone)]
pub struct ClusterVersionState {
    /// Minimum version observed across all nodes.
    pub min_version: u16,
    /// Maximum version observed across all nodes.
    pub max_version: u16,
    /// Number of nodes reporting.
    pub node_count: usize,
    /// Per-node versions: `(node_id, wire_version)`.
    pub node_versions: Vec<(u64, u16)>,
}

impl ClusterVersionState {
    pub fn new() -> Self {
        Self {
            min_version: WIRE_FORMAT_VERSION,
            max_version: WIRE_FORMAT_VERSION,
            node_count: 0,
            node_versions: Vec::new(),
        }
    }

    /// Record a node's reported version.
    pub fn report_version(&mut self, node_id: u64, wire_version: u16) {
        // Update or insert.
        if let Some(entry) = self.node_versions.iter_mut().find(|(id, _)| *id == node_id) {
            entry.1 = wire_version;
        } else {
            self.node_versions.push((node_id, wire_version));
        }
        self.recalculate();
    }

    /// Remove a node (decommissioned/left).
    pub fn remove_node(&mut self, node_id: u64) {
        self.node_versions.retain(|(id, _)| *id != node_id);
        self.recalculate();
    }

    fn recalculate(&mut self) {
        self.node_count = self.node_versions.len();
        if self.node_versions.is_empty() {
            self.min_version = WIRE_FORMAT_VERSION;
            self.max_version = WIRE_FORMAT_VERSION;
        } else {
            // Safe: empty case handled above, so min/max always return Some.
            self.min_version = self
                .node_versions
                .iter()
                .map(|(_, v)| *v)
                .min()
                .unwrap_or(WIRE_FORMAT_VERSION);
            self.max_version = self
                .node_versions
                .iter()
                .map(|(_, v)| *v)
                .max()
                .unwrap_or(WIRE_FORMAT_VERSION);
        }
    }

    /// Whether the cluster is in mixed-version mode (not all nodes same version).
    pub fn is_mixed_version(&self) -> bool {
        self.min_version != self.max_version
    }

    /// Whether all nodes have upgraded to the latest version.
    pub fn all_upgraded(&self) -> bool {
        self.min_version == WIRE_FORMAT_VERSION
    }

    /// Whether a specific feature version can be activated.
    ///
    /// A feature requiring version V can only be activated when ALL nodes
    /// are running at least version V.
    pub fn can_activate_feature(&self, required_version: u16) -> bool {
        self.min_version >= required_version
    }

    /// Version spread: max_version - min_version.
    /// Must be <= 1 for supported operation (N and N-1 only).
    pub fn version_spread(&self) -> u16 {
        self.max_version.saturating_sub(self.min_version)
    }

    /// Whether the version spread is within supported bounds (N-1 only).
    pub fn is_supported_spread(&self) -> bool {
        self.version_spread() <= 1
    }
}

impl Default for ClusterVersionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a message from a remote node should be accepted.
///
/// Accepts messages from the same version or one version behind (N-1).
/// Rejects messages that are too new (upgrade this node) or too old.
pub fn accept_message(remote_version: u16) -> Result<(), String> {
    if remote_version > WIRE_FORMAT_VERSION {
        return Err(format!(
            "message version {remote_version} > local {WIRE_FORMAT_VERSION}: upgrade this node"
        ));
    }
    if remote_version < MIN_WIRE_FORMAT_VERSION {
        return Err(format!(
            "message version {remote_version} < minimum {MIN_WIRE_FORMAT_VERSION}: remote node too old"
        ));
    }
    if WIRE_FORMAT_VERSION - remote_version > 1 {
        return Err(format!(
            "message version {remote_version} is more than one generation behind {WIRE_FORMAT_VERSION}: N-2 not supported"
        ));
    }
    Ok(())
}

/// Determine if this node should operate in compatibility mode.
///
/// Compat mode is active when the cluster has mixed versions.
/// In compat mode, new features that require the latest version are disabled.
pub fn should_compat_mode(cluster_state: &ClusterVersionState) -> bool {
    cluster_state.is_mixed_version()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_version_cluster() {
        let mut state = ClusterVersionState::new();
        state.report_version(1, WIRE_FORMAT_VERSION);
        state.report_version(2, WIRE_FORMAT_VERSION);
        state.report_version(3, WIRE_FORMAT_VERSION);

        assert!(!state.is_mixed_version());
        assert!(state.all_upgraded());
        assert!(state.is_supported_spread());
        assert!(!should_compat_mode(&state));
    }

    #[test]
    fn mixed_version_n_minus_1() {
        let mut state = ClusterVersionState::new();
        state.report_version(1, WIRE_FORMAT_VERSION);
        state.report_version(2, WIRE_FORMAT_VERSION);
        state.report_version(3, WIRE_FORMAT_VERSION.saturating_sub(1));

        assert!(state.is_mixed_version());
        assert!(!state.all_upgraded());
        assert!(state.is_supported_spread()); // N-1 is supported.
        assert!(should_compat_mode(&state));
    }

    #[test]
    fn accept_same_version() {
        assert!(accept_message(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn accept_n_minus_1() {
        if WIRE_FORMAT_VERSION > 1 {
            assert!(accept_message(WIRE_FORMAT_VERSION - 1).is_ok());
        }
    }

    #[test]
    fn reject_newer() {
        assert!(accept_message(WIRE_FORMAT_VERSION + 1).is_err());
    }

    #[test]
    fn feature_activation() {
        let mut state = ClusterVersionState::new();
        state.report_version(1, 2);
        state.report_version(2, 2);
        state.report_version(3, 1); // Lagging node.

        assert!(!state.can_activate_feature(2)); // Node 3 is still on v1.

        state.report_version(3, 2); // Node 3 upgraded.
        assert!(state.can_activate_feature(2)); // All on v2 now.
    }

    #[test]
    fn node_removal() {
        let mut state = ClusterVersionState::new();
        state.report_version(1, 2);
        state.report_version(2, 1);
        assert!(state.is_mixed_version());

        state.remove_node(2);
        assert!(!state.is_mixed_version());
    }
}

use std::collections::HashMap;
use tracing::{debug, info};

/// Serializable ghost stub for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedGhostStub {
    target_shard: u16,
    refcount: u32,
    created_at_ms: u64,
}

/// Ghost edge stub.
///
/// When vShard rebalancing moves a node to a new shard, a ghost stub
/// `(node_id, target_shard_id, refcount)` remains on the source.
/// During traversal, ghost stubs trigger transparent scatter-gather
/// to the target shard. Ghosts are garbage-collected when `refcount`
/// (number of inbound edges from this shard) reaches zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostStub {
    /// The node ID that was moved.
    pub node_id: String,
    /// The target vShard where the node now lives.
    pub target_shard: u16,
    /// Number of local edges that still reference this ghost.
    pub refcount: u32,
    /// When this ghost was created.
    pub created_at_ms: u64,
}

impl GhostStub {
    pub fn new(node_id: String, target_shard: u16, initial_refcount: u32) -> Self {
        Self {
            node_id,
            target_shard,
            refcount: initial_refcount,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|e| e.duration())
                .as_millis() as u64,
        }
    }

    /// Decrement refcount when a local edge pointing to this ghost is deleted.
    /// Returns true if refcount reached zero (ghost can be purged).
    pub fn decrement_ref(&mut self) -> bool {
        self.refcount = self.refcount.saturating_sub(1);
        self.refcount == 0
    }

    /// Increment refcount when a local edge pointing to this ghost is added.
    pub fn increment_ref(&mut self) {
        self.refcount = self.refcount.saturating_add(1);
    }
}

/// Anti-entropy sweeper result for a single ghost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SweepVerdict {
    /// Ghost is still needed (edges exist or target confirms node).
    Keep,
    /// Ghost can be purged (no local edges AND target doesn't have the node).
    Purge,
    /// Couldn't verify (target unreachable). Keep for now, retry later.
    Inconclusive,
}

/// Ghost table for a single vShard.
///
/// Tracks ghost stubs left behind after node migration.
/// The anti-entropy sweeper runs periodically to purge stale ghosts.
#[derive(Debug, Clone)]
pub struct GhostTable {
    /// node_id → GhostStub.
    stubs: HashMap<String, GhostStub>,
    /// Cumulative count of ghosts purged (for metrics).
    purge_count: u64,
    /// Last sweep timestamp.
    last_sweep_ms: u64,
}

impl GhostTable {
    pub fn new() -> Self {
        Self {
            stubs: HashMap::new(),
            purge_count: 0,
            last_sweep_ms: 0,
        }
    }

    /// Insert a ghost stub after a node is migrated away.
    pub fn insert(&mut self, stub: GhostStub) {
        debug!(
            node = %stub.node_id,
            target_shard = stub.target_shard,
            refcount = stub.refcount,
            "inserted ghost stub"
        );
        self.stubs.insert(stub.node_id.clone(), stub);
    }

    /// Look up a ghost stub by node ID.
    pub fn get(&self, node_id: &str) -> Option<&GhostStub> {
        self.stubs.get(node_id)
    }

    /// Decrement refcount for a ghost (when a local edge is deleted).
    /// Returns true if the ghost was purged (refcount reached zero).
    pub fn decrement_ref(&mut self, node_id: &str) -> bool {
        if let Some(stub) = self.stubs.get_mut(node_id)
            && stub.decrement_ref()
        {
            self.stubs.remove(node_id);
            self.purge_count += 1;
            return true;
        }
        false
    }

    /// Increment refcount for a ghost (when a local edge to a ghost node is added).
    pub fn increment_ref(&mut self, node_id: &str) {
        if let Some(stub) = self.stubs.get_mut(node_id) {
            stub.increment_ref();
        }
    }

    /// Run anti-entropy sweep.
    ///
    /// For each ghost stub, the caller must verify against the target shard:
    /// 1. Does the target shard acknowledge the node exists?
    /// 2. Do any local edges still reference this ghost?
    ///
    /// `verify_fn` takes (node_id, target_shard) and returns SweepVerdict.
    /// The sweeper runs at lowest I/O priority and is rate-limited.
    pub fn sweep<F>(&mut self, verify_fn: F) -> SweepReport
    where
        F: Fn(&str, u16) -> SweepVerdict,
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_sweep_ms = now_ms;

        let mut report = SweepReport::default();
        let mut to_purge = Vec::new();

        for (node_id, stub) in &self.stubs {
            report.checked += 1;

            // Fast path: if refcount is already 0, purge without remote check.
            if stub.refcount == 0 {
                to_purge.push(node_id.clone());
                report.purged += 1;
                continue;
            }

            match verify_fn(node_id, stub.target_shard) {
                SweepVerdict::Purge => {
                    to_purge.push(node_id.clone());
                    report.purged += 1;
                }
                SweepVerdict::Keep => {
                    report.kept += 1;
                }
                SweepVerdict::Inconclusive => {
                    report.inconclusive += 1;
                }
            }
        }

        for node_id in to_purge {
            self.stubs.remove(&node_id);
            self.purge_count += 1;
        }

        if report.purged > 0 {
            info!(
                purged = report.purged,
                kept = report.kept,
                inconclusive = report.inconclusive,
                total_ghosts = self.stubs.len(),
                "anti-entropy sweep complete"
            );
        }

        report
    }

    pub fn len(&self) -> usize {
        self.stubs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stubs.is_empty()
    }

    pub fn total_purged(&self) -> u64 {
        self.purge_count
    }

    pub fn last_sweep_ms(&self) -> u64 {
        self.last_sweep_ms
    }

    /// All ghost stubs (for metrics/debugging).
    pub fn stubs(&self) -> impl Iterator<Item = &GhostStub> {
        self.stubs.values()
    }

    /// Resolve a node lookup: if the node is a ghost, return the target shard
    /// for scatter-gather. Otherwise return None.
    pub fn resolve(&self, node_id: &str) -> Option<u16> {
        self.stubs.get(node_id).map(|s| s.target_shard)
    }
}

impl GhostTable {
    /// Serialize all ghost stubs for persistence.
    ///
    /// Returns MessagePack bytes that can be stored in the cluster catalog.
    pub fn to_bytes(&self) -> Vec<u8> {
        let persisted: HashMap<String, PersistedGhostStub> = self
            .stubs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    PersistedGhostStub {
                        target_shard: v.target_shard,
                        refcount: v.refcount,
                        created_at_ms: v.created_at_ms,
                    },
                )
            })
            .collect();
        match rmp_serde::to_vec(&persisted) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(error = %e, "ghost table serialization failed — state will not persist");
                Vec::new()
            }
        }
    }

    /// Restore ghost stubs from persisted bytes.
    ///
    /// Called on startup to recover ghost state from the cluster catalog.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let persisted: HashMap<String, PersistedGhostStub> = rmp_serde::from_slice(data).ok()?;
        let stubs: HashMap<String, GhostStub> = persisted
            .into_iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    GhostStub {
                        node_id: k,
                        target_shard: v.target_shard,
                        refcount: v.refcount,
                        created_at_ms: v.created_at_ms,
                    },
                )
            })
            .collect();
        Some(Self {
            stubs,
            purge_count: 0,
            last_sweep_ms: 0,
        })
    }
}

impl Default for GhostTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Report from an anti-entropy sweep.
#[derive(Debug, Default, Clone)]
pub struct SweepReport {
    pub checked: usize,
    pub purged: usize,
    pub kept: usize,
    pub inconclusive: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ghost_lifecycle() {
        let mut table = GhostTable::new();
        let stub = GhostStub::new("node-42".into(), 10, 3);
        table.insert(stub);
        assert_eq!(table.len(), 1);

        // Resolve returns target shard.
        assert_eq!(table.resolve("node-42"), Some(10));
        assert_eq!(table.resolve("nonexistent"), None);

        // Decrement refcount 3 times.
        assert!(!table.decrement_ref("node-42"));
        assert!(!table.decrement_ref("node-42"));
        assert!(table.decrement_ref("node-42")); // refcount 0 → purged.
        assert!(table.is_empty());
        assert_eq!(table.total_purged(), 1);
    }

    #[test]
    fn ghost_increment_ref() {
        let mut table = GhostTable::new();
        table.insert(GhostStub::new("n1".into(), 5, 1));
        table.increment_ref("n1");
        // Now refcount is 2, needs 2 decrements to purge.
        assert!(!table.decrement_ref("n1"));
        assert!(table.decrement_ref("n1"));
    }

    #[test]
    fn sweep_purges_stale_ghosts() {
        let mut table = GhostTable::new();
        table.insert(GhostStub::new("stale".into(), 5, 1));
        table.insert(GhostStub::new("alive".into(), 6, 2));
        table.insert(GhostStub::new("unreachable".into(), 7, 1));

        let report = table.sweep(|node_id, _target| match node_id {
            "stale" => SweepVerdict::Purge,
            "alive" => SweepVerdict::Keep,
            "unreachable" => SweepVerdict::Inconclusive,
            _ => SweepVerdict::Keep,
        });

        assert_eq!(report.checked, 3);
        assert_eq!(report.purged, 1);
        assert_eq!(report.kept, 1);
        assert_eq!(report.inconclusive, 1);
        assert_eq!(table.len(), 2); // alive + unreachable remain
    }

    #[test]
    fn sweep_purges_zero_refcount_without_remote() {
        let mut table = GhostTable::new();
        let mut stub = GhostStub::new("zero-ref".into(), 5, 1);
        stub.refcount = 0; // Already zero.
        table.insert(stub);

        // verify_fn should not be called for zero-refcount ghosts,
        // but we make it return Keep to prove it's bypassed.
        let report = table.sweep(|_, _| SweepVerdict::Keep);
        assert_eq!(report.purged, 1);
        assert!(table.is_empty());
    }

    #[test]
    fn resolve_for_scatter_gather() {
        let mut table = GhostTable::new();
        table.insert(GhostStub::new("migrated-node".into(), 42, 5));

        // During graph traversal, encountering "migrated-node" resolves to shard 42.
        let target = table.resolve("migrated-node");
        assert_eq!(target, Some(42));
    }
}

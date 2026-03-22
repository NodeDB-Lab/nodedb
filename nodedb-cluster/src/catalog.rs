//! Cluster catalog — persistent storage for topology and routing tables.
//!
//! Uses redb (embedded B-Tree ACID storage) with MessagePack serialization.
//! Stores the cluster topology and routing table so nodes can recover
//! cluster state after a restart without contacting peers.

use std::path::Path;

use redb::{Database, TableDefinition};

use crate::error::{ClusterError, Result};
use crate::ghost::GhostTable;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// Single-row table for the serialized ClusterTopology.
const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("_cluster.topology");

/// Single-row table for the serialized RoutingTable.
const ROUTING_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("_cluster.routing");

/// Cluster metadata (cluster_id, bootstrap version, etc.).
const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("_cluster.metadata");

/// Ghost stub table — persists ghost edge refcounts across restarts.
const GHOST_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("_cluster.ghosts");

const KEY_TOPOLOGY: &str = "topology";
const KEY_CA_CERT: &str = "ca_cert";
const KEY_ROUTING: &str = "routing";
const KEY_CLUSTER_ID: &str = "cluster_id";

/// Persistent cluster catalog backed by redb.
pub struct ClusterCatalog {
    db: Database,
}

impl ClusterCatalog {
    /// Open or create the cluster catalog at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_err(|e| ClusterError::Transport {
            detail: format!("open cluster catalog {}: {e}", path.display()),
        })?;

        // Ensure tables exist.
        let txn = db.begin_write().map_err(catalog_err)?;
        {
            let _ = txn.open_table(TOPOLOGY_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(ROUTING_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            let _ = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;

        Ok(Self { db })
    }

    // ── Topology ────────────────────────────────────────────────────

    /// Persist the cluster topology.
    pub fn save_topology(&self, topology: &ClusterTopology) -> Result<()> {
        let bytes = rmp_serde::to_vec(topology).map_err(|e| ClusterError::Codec {
            detail: format!("serialize topology: {e}"),
        })?;

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(TOPOLOGY_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_TOPOLOGY, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster topology. Returns None if no topology has been saved.
    pub fn load_topology(&self) -> Result<Option<ClusterTopology>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(TOPOLOGY_TABLE).map_err(catalog_err)?;

        match table.get(KEY_TOPOLOGY).map_err(catalog_err)? {
            Some(guard) => {
                let bytes = guard.value();
                let topo: ClusterTopology =
                    rmp_serde::from_slice(bytes).map_err(|e| ClusterError::Codec {
                        detail: format!("deserialize topology: {e}"),
                    })?;
                Ok(Some(topo))
            }
            None => Ok(None),
        }
    }

    // ── Routing Table ───────────────────────────────────────────────

    /// Persist the routing table.
    pub fn save_routing(&self, routing: &RoutingTable) -> Result<()> {
        let bytes = rmp_serde::to_vec(routing).map_err(|e| ClusterError::Codec {
            detail: format!("serialize routing: {e}"),
        })?;

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(ROUTING_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_ROUTING, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the routing table. Returns None if no routing table has been saved.
    pub fn load_routing(&self) -> Result<Option<RoutingTable>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(ROUTING_TABLE).map_err(catalog_err)?;

        match table.get(KEY_ROUTING).map_err(catalog_err)? {
            Some(guard) => {
                let bytes = guard.value();
                let rt: RoutingTable =
                    rmp_serde::from_slice(bytes).map_err(|e| ClusterError::Codec {
                        detail: format!("deserialize routing: {e}"),
                    })?;
                Ok(Some(rt))
            }
            None => Ok(None),
        }
    }

    // ── Metadata ────────────────────────────────────────────────────

    /// Store the cluster ID (generated at bootstrap, immutable).
    pub fn save_cluster_id(&self, cluster_id: u64) -> Result<()> {
        let bytes = cluster_id.to_le_bytes();
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CLUSTER_ID, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster ID. Returns None if not yet bootstrapped.
    pub fn load_cluster_id(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;

        match table.get(KEY_CLUSTER_ID).map_err(catalog_err)? {
            Some(guard) => {
                let bytes = guard.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    let id = u64::from_le_bytes(arr);
                    Ok(Some(id))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Check if this catalog has been bootstrapped (has a cluster_id).
    pub fn is_bootstrapped(&self) -> Result<bool> {
        self.load_cluster_id().map(|id| id.is_some())
    }

    // ── TLS Certificates ────────────────────────────────────────────

    /// Store the cluster CA certificate (DER-encoded).
    pub fn save_ca_cert(&self, ca_cert_der: &[u8]) -> Result<()> {
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CA_CERT, ca_cert_der)
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    // ── Ghost Stubs ──────────────────────────────────────────────────

    /// Persist ghost stubs for a vShard.
    ///
    /// Called after each sweep or after ghost table mutations to ensure
    /// refcounts survive crash/restart.
    pub fn save_ghosts(&self, vshard_id: u16, ghost_table: &GhostTable) -> Result<()> {
        let bytes = ghost_table.to_bytes();
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load ghost stubs for a vShard. Returns None if no ghosts persisted.
    pub fn load_ghosts(&self, vshard_id: u16) -> Result<Option<GhostTable>> {
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;

        match table.get(key.as_str()).map_err(catalog_err)? {
            Some(guard) => Ok(GhostTable::from_bytes(guard.value())),
            None => Ok(None),
        }
    }

    /// Load all persisted ghost tables across all vShards.
    ///
    /// Returns `(vshard_id, GhostTable)` pairs for all vShards that have ghosts.
    pub fn load_all_ghosts(&self) -> Result<Vec<(u16, GhostTable)>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;

        let mut results = Vec::new();
        let range = table.range::<&str>(..).map_err(catalog_err)?;
        for entry in range {
            let (key, value) = entry.map_err(catalog_err)?;
            let key_str = key.value();
            if let Some(id_str) = key_str.strip_prefix("ghosts:")
                && let Ok(vshard_id) = id_str.parse::<u16>()
                && let Some(ghost_table) = GhostTable::from_bytes(value.value())
            {
                results.push((vshard_id, ghost_table));
            }
        }
        Ok(results)
    }

    /// Delete persisted ghosts for a vShard (after all ghosts purged).
    pub fn delete_ghosts(&self, vshard_id: u16) -> Result<()> {
        let key = format!("ghosts:{vshard_id}");

        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(GHOST_TABLE).map_err(catalog_err)?;
            let _ = table.remove(key.as_str()).map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster CA certificate. Returns None if not bootstrapped.
    pub fn load_ca_cert(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        match table.get(KEY_CA_CERT).map_err(catalog_err)? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }
}

fn catalog_err(e: impl std::fmt::Display) -> ClusterError {
    ClusterError::Transport {
        detail: format!("cluster catalog: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ghost::GhostStub;
    use crate::topology::{NodeInfo, NodeState};

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn topology_save_load_roundtrip() {
        let (_dir, catalog) = temp_catalog();

        let mut topo = ClusterTopology::new();
        topo.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo.add_node(NodeInfo::new(
            2,
            "10.0.0.2:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo.add_node(NodeInfo::new(
            3,
            "10.0.0.3:9400".parse().unwrap(),
            NodeState::Joining,
        ));

        catalog.save_topology(&topo).unwrap();
        let loaded = catalog.load_topology().unwrap().unwrap();

        assert_eq!(loaded.node_count(), 3);
        assert_eq!(loaded.version(), 3);
        assert_eq!(loaded.active_nodes().len(), 2);
        assert_eq!(loaded.get_node(1).unwrap().addr, "10.0.0.1:9400");
    }

    #[test]
    fn routing_save_load_roundtrip() {
        let (_dir, catalog) = temp_catalog();

        let rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        catalog.save_routing(&rt).unwrap();
        let loaded = catalog.load_routing().unwrap().unwrap();

        assert_eq!(loaded.num_groups(), 4);
        assert_eq!(loaded.vshard_to_group().len(), 1024);
        // Verify vShard routing matches.
        for i in 0..1024u16 {
            assert_eq!(
                rt.group_for_vshard(i).unwrap(),
                loaded.group_for_vshard(i).unwrap()
            );
        }
    }

    #[test]
    fn cluster_id_persistence() {
        let (_dir, catalog) = temp_catalog();

        assert!(!catalog.is_bootstrapped().unwrap());
        assert_eq!(catalog.load_cluster_id().unwrap(), None);

        catalog.save_cluster_id(42).unwrap();
        assert!(catalog.is_bootstrapped().unwrap());
        assert_eq!(catalog.load_cluster_id().unwrap(), Some(42));
    }

    #[test]
    fn empty_catalog_returns_none() {
        let (_dir, catalog) = temp_catalog();

        assert!(catalog.load_topology().unwrap().is_none());
        assert!(catalog.load_routing().unwrap().is_none());
    }

    #[test]
    fn ghost_persistence_roundtrip() {
        let (_dir, catalog) = temp_catalog();

        let mut ghosts = GhostTable::new();
        ghosts.insert(GhostStub::new("node-A".into(), 5, 3));
        ghosts.insert(GhostStub::new("node-B".into(), 10, 1));

        catalog.save_ghosts(42, &ghosts).unwrap();

        let loaded = catalog.load_ghosts(42).unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.resolve("node-A"), Some(5));
        assert_eq!(loaded.resolve("node-B"), Some(10));
        assert_eq!(loaded.get("node-A").unwrap().refcount, 3);
    }

    #[test]
    fn ghost_load_all() {
        let (_dir, catalog) = temp_catalog();

        let mut g1 = GhostTable::new();
        g1.insert(GhostStub::new("x".into(), 1, 1));
        catalog.save_ghosts(10, &g1).unwrap();

        let mut g2 = GhostTable::new();
        g2.insert(GhostStub::new("y".into(), 2, 2));
        catalog.save_ghosts(20, &g2).unwrap();

        let all = catalog.load_all_ghosts().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn ghost_delete() {
        let (_dir, catalog) = temp_catalog();

        let mut ghosts = GhostTable::new();
        ghosts.insert(GhostStub::new("z".into(), 3, 1));
        catalog.save_ghosts(99, &ghosts).unwrap();

        catalog.delete_ghosts(99).unwrap();
        assert!(catalog.load_ghosts(99).unwrap().is_none());
    }

    #[test]
    fn overwrite_topology() {
        let (_dir, catalog) = temp_catalog();

        let mut topo1 = ClusterTopology::new();
        topo1.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        catalog.save_topology(&topo1).unwrap();

        let mut topo2 = ClusterTopology::new();
        topo2.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo2.add_node(NodeInfo::new(
            2,
            "10.0.0.2:9400".parse().unwrap(),
            NodeState::Active,
        ));
        catalog.save_topology(&topo2).unwrap();

        let loaded = catalog.load_topology().unwrap().unwrap();
        assert_eq!(loaded.node_count(), 2);
    }
}

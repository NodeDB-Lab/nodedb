//! Topology and routing-table save / load.

use crate::error::{ClusterError, Result};
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

use super::core::ClusterCatalog;
use super::schema::{KEY_ROUTING, KEY_TOPOLOGY, ROUTING_TABLE, TOPOLOGY_TABLE, catalog_err};

impl ClusterCatalog {
    /// Persist the cluster topology.
    pub fn save_topology(&self, topology: &ClusterTopology) -> Result<()> {
        let bytes = zerompk::to_msgpack_vec(topology).map_err(|e| ClusterError::Codec {
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
                    zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Codec {
                        detail: format!("deserialize topology: {e}"),
                    })?;
                Ok(Some(topo))
            }
            None => Ok(None),
        }
    }

    /// Persist the routing table.
    pub fn save_routing(&self, routing: &RoutingTable) -> Result<()> {
        let bytes = zerompk::to_msgpack_vec(routing).map_err(|e| ClusterError::Codec {
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
                    zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Codec {
                        detail: format!("deserialize routing: {e}"),
                    })?;
                Ok(Some(rt))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        // uniform(4, ...) creates 4 data groups + 1 metadata group = 5 total.
        let rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        catalog.save_routing(&rt).unwrap();
        let loaded = catalog.load_routing().unwrap().unwrap();

        assert_eq!(loaded.num_groups(), 5);
        assert_eq!(loaded.vshard_to_group().len(), 1024);
        for i in 0..1024u16 {
            assert_eq!(
                rt.group_for_vshard(i).unwrap(),
                loaded.group_for_vshard(i).unwrap()
            );
        }
    }

    #[test]
    fn empty_catalog_returns_none() {
        let (_dir, catalog) = temp_catalog();

        assert!(catalog.load_topology().unwrap().is_none());
        assert!(catalog.load_routing().unwrap().is_none());
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

// SPDX-License-Identifier: BUSL-1.1

//! One storage sample: read this node's share of stored bytes and publish it.
//!
//! The share counts the vShards this node leads. A node with no peers leads
//! every vShard it serves, so its share is the whole total. Such a node also
//! publishes that total to the per-database gauge and to tenant quota usage.
//! A node has no peers when it runs without cluster routing, or when the
//! cluster topology holds this node alone.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::control::state::SharedState;

use super::led_vshards::led_vshards;
use super::query::query_local_storage_bytes;
use super::shares::StorageShare;

/// Period between storage samples. Each sample reads counters on every
/// core, so it runs on this period, never per write.
pub const STORAGE_SAMPLE_PERIOD: Duration = Duration::from_secs(30);

/// How long every core has to answer one sample.
const STORAGE_QUERY_DEADLINE: Duration = Duration::from_secs(10);

/// Read this node's share of stored bytes and publish it to
/// `state.storage_shares`. Returns the published share.
///
/// A node with no peers also publishes the share as the total: the
/// per-database storage gauge and each tenant's storage usage.
pub async fn sample_storage(state: &SharedState) -> crate::Result<Arc<StorageShare>> {
    let led = led_vshards(state);
    let totals =
        query_local_storage_bytes(state, led.clone(), Instant::now() + STORAGE_QUERY_DEADLINE)
            .await?;
    let share = Arc::new(StorageShare {
        totals,
        led_vshards: led,
        sampled_at: Instant::now(),
    });
    let prior = state.storage_shares.publish(Arc::clone(&share));
    if has_no_peers(state) {
        publish_node_total(state, &share, prior.as_deref());
    }
    Ok(share)
}

/// Whether this node runs alone: no cluster routing, or a cluster topology
/// that holds this node and no other. Routing with no topology wired is not
/// known to be alone.
fn has_no_peers(state: &SharedState) -> bool {
    if state.cluster_routing.is_none() {
        return true;
    }
    state.cluster_topology.as_ref().is_some_and(|topology| {
        let topology = topology.read().unwrap_or_else(|p| p.into_inner());
        topology.node_count() == 1 && topology.contains(state.node_id)
    })
}

/// Publish `share` as the node's total. A database the share does not name
/// reads zero. A tenant the prior share named and `share` does not drops to
/// zero.
fn publish_node_total(state: &SharedState, share: &StorageShare, prior: Option<&StorageShare>) {
    let databases = share.database_totals();
    let registry = &state.database_metrics;
    for id in registry.database_ids() {
        registry.set_storage_bytes(id, databases.get(&id).copied().unwrap_or(0));
    }

    let tenants = share.tenant_totals();
    let mut isolation = state.tenants.lock().unwrap_or_else(|p| p.into_inner());
    for (tenant_id, bytes) in &tenants {
        isolation.update_storage(*tenant_id, *bytes);
    }
    if let Some(prior) = prior {
        for tenant_id in prior.tenant_totals().keys() {
            if !tenants.contains_key(tenant_id) {
                isolation.update_storage(*tenant_id, 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::RwLock;

    use nodedb_cluster::{ClusterTopology, NodeInfo, NodeState, RoutingTable};
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn share(rows: &[(u64, u64, u64)]) -> StorageShare {
        StorageShare {
            totals: rows
                .iter()
                .map(|(d, t, b)| ((DatabaseId::new(*d), TenantId::new(*t)), *b))
                .collect::<HashMap<_, _>>(),
            led_vshards: Vec::new(),
            sampled_at: Instant::now(),
        }
    }

    fn fresh_state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("p.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    fn topology_of(nodes: &[u64]) -> Arc<RwLock<ClusterTopology>> {
        let mut topology = ClusterTopology::new();
        for (i, node_id) in nodes.iter().enumerate() {
            let addr = format!("127.0.0.1:{}", 9000 + i).parse().expect("addr");
            topology.add_node(NodeInfo::new(*node_id, addr, NodeState::Active));
        }
        Arc::new(RwLock::new(topology))
    }

    /// A node without routing, or alone in its topology, has no peers. A
    /// second member, or routing with no topology, means it may have peers.
    #[test]
    fn a_lone_member_has_no_peers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = fresh_state(dir.path());
        assert!(has_no_peers(&state));

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.node_id = 1;
        shared.cluster_routing = Some(Arc::new(RwLock::new(RoutingTable::uniform(1, &[1], 1))));
        shared.cluster_topology = None;
        assert!(!has_no_peers(&state));

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[1]));
        assert!(has_no_peers(&state));

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[1, 2]));
        assert!(!has_no_peers(&state));

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[2]));
        assert!(!has_no_peers(&state));
    }

    /// The node total reaches the database gauge and tenant usage, and a
    /// tenant that leaves the share reads zero.
    #[test]
    fn the_node_total_reaches_gauges_and_tenant_usage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("s.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let (a, b) = (DatabaseId::new(41), DatabaseId::new(42));
        state.database_metrics.ensure_database(a);
        state.database_metrics.ensure_database(b);
        state.database_metrics.set_storage_bytes(b, 99);

        let first = share(&[(41, 1, 100), (41, 2, 20)]);
        publish_node_total(&state, &first, None);
        assert_eq!(state.database_metrics.storage_bytes(a), 120);
        assert_eq!(state.database_metrics.storage_bytes(b), 0);
        let usage = |t: u64| {
            state
                .tenants
                .lock()
                .expect("tenants")
                .usage(TenantId::new(t))
                .map_or(0, |u| u.storage_bytes)
        };
        assert_eq!(usage(1), 100);
        assert_eq!(usage(2), 20);

        let second = share(&[(41, 1, 40)]);
        publish_node_total(&state, &second, Some(&first));
        assert_eq!(state.database_metrics.storage_bytes(a), 40);
        assert_eq!(usage(1), 40);
        assert_eq!(usage(2), 0);
    }
}

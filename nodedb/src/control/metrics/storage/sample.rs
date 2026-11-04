// SPDX-License-Identifier: BUSL-1.1

//! One storage sample: read this node's share of stored bytes and publish it.
//!
//! The share counts the vShards this node leads, split by the Raft group of
//! each vShard. The aggregator turns every node's share into the cluster
//! total.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::control::state::SharedState;

use super::led_vshards::{LedSet, led_vshards};
use super::query::{VShardTotals, query_local_storage_bytes};
use super::shares::{GroupTotals, StorageShare};

/// How long every core has to answer one sample.
const STORAGE_QUERY_DEADLINE: Duration = Duration::from_secs(10);

/// Read this node's share of stored bytes and publish it to
/// `state.storage_shares`. Returns the published share.
pub async fn sample_storage(state: &SharedState) -> crate::Result<Arc<StorageShare>> {
    let led = led_vshards(state);
    let totals = query_local_storage_bytes(
        state,
        led.vshards.clone(),
        Instant::now() + STORAGE_QUERY_DEADLINE,
    )
    .await?;
    let groups = split_by_group(&led, totals)?;
    let share = Arc::new(StorageShare {
        groups,
        led_vshards: led.vshards,
        sampled_at: Instant::now(),
    });
    state.storage_shares.publish(Arc::clone(&share));
    Ok(share)
}

/// File each vShard's bytes under its led group. Every led group appears,
/// with or without bytes. A row for a vShard outside `led` fails the sample:
/// the cores count led vShards only.
fn split_by_group(led: &LedSet, totals: VShardTotals) -> crate::Result<BTreeMap<u64, GroupTotals>> {
    let mut groups: BTreeMap<u64, GroupTotals> = led
        .groups()
        .into_iter()
        .map(|group| {
            (
                group.group_id,
                GroupTotals {
                    term: group.term,
                    totals: Default::default(),
                },
            )
        })
        .collect();
    for ((vshard, database_id, tenant_id), bytes) in totals {
        let Some(group) = led.group_of(vshard) else {
            return Err(crate::Error::Internal {
                detail: format!("a core counted stored bytes on vShard {vshard}, which is not led"),
            });
        };
        let entry = groups.entry(group.group_id).or_insert_with(|| GroupTotals {
            term: group.term,
            totals: Default::default(),
        });
        let total = entry.totals.entry((database_id, tenant_id)).or_insert(0);
        *total = total.saturating_add(bytes);
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use nodedb_cluster::RoutingTable;
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// The led set of node 1 in two data groups that it both leads by hint.
    fn two_group_led() -> (LedSet, u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("s.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        let mut table = RoutingTable::uniform(2, &[1, 2], 2);
        for group_id in table.group_ids() {
            table.set_leader(group_id, 1);
        }
        let even = table.group_for_vshard(0).expect("vShard 0 group");
        let odd = table.group_for_vshard(1).expect("vShard 1 group");
        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_routing = Some(Arc::new(RwLock::new(table)));
        shared.node_id = 1;
        (led_vshards(&state), even, odd)
    }

    fn key(d: u64, t: u64) -> (DatabaseId, TenantId) {
        (DatabaseId::new(d), TenantId::new(t))
    }

    /// Each vShard's bytes land in its own group, and a led group with no
    /// bytes still appears.
    #[test]
    fn bytes_split_by_the_group_of_each_vshard() {
        let (led, even, odd) = two_group_led();
        assert_ne!(even, odd);
        let mut totals = VShardTotals::new();
        totals.insert((0, DatabaseId::new(1), TenantId::new(1)), 10);
        totals.insert((2, DatabaseId::new(1), TenantId::new(1)), 5);
        totals.insert((1, DatabaseId::new(1), TenantId::new(1)), 7);
        let groups = split_by_group(&led, totals).expect("split");
        assert_eq!(groups[&even].totals[&key(1, 1)], 15);
        assert_eq!(groups[&odd].totals[&key(1, 1)], 7);

        let groups = split_by_group(&led, VShardTotals::new()).expect("split");
        assert_eq!(groups.len(), 2, "every led group appears: {groups:?}");
        assert!(groups.values().all(|group| group.totals.is_empty()));
    }

    /// A row for a vShard this node does not lead fails the sample.
    #[test]
    fn a_row_outside_the_led_set_fails() {
        let led = LedSet::default();
        let mut totals = VShardTotals::new();
        totals.insert((3, DatabaseId::new(0), TenantId::new(1)), 1);
        let err = split_by_group(&led, totals).expect_err("unled vShard");
        assert!(err.to_string().contains("vShard 3"), "{err}");
    }
}

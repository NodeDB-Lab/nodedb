// SPDX-License-Identifier: BUSL-1.1

//! This node's last sampled share of stored bytes.
//!
//! A share counts the vShards this node led when it was sampled, split by
//! Raft group. A cluster total takes each group from one share.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use nodedb_types::{DatabaseId, TenantId};

/// Stored bytes per `(database, tenant)`.
pub type TenantTotals = HashMap<(DatabaseId, TenantId), u64>;

/// The stored bytes of one Raft group, as its leader sampled them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupTotals {
    /// The term the sampling node led the group at.
    pub term: u64,
    /// Stored bytes per `(database, tenant)` on the group's vShards.
    pub totals: TenantTotals,
}

/// One sample of a node's stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageShare {
    /// Every group the node led at the sample, keyed by group id. A led group
    /// with no stored bytes still appears, so its term outranks older reports.
    pub groups: BTreeMap<u64, GroupTotals>,
    /// The vShards the node led when it was sampled, in ascending order.
    /// Empty for a share received from a peer.
    pub led_vshards: Vec<u32>,
    /// When the sample completed, on this node's clock.
    pub sampled_at: Instant,
}

impl StorageShare {
    /// The share's stored bytes per `(database, tenant)`, summed over groups.
    pub fn totals(&self) -> TenantTotals {
        let mut out = TenantTotals::new();
        for group in self.groups.values() {
            add_totals(&mut out, &group.totals);
        }
        out
    }
}

/// Add every entry of `from` into `into`, saturating.
pub fn add_totals(into: &mut TenantTotals, from: &TenantTotals) {
    for (key, bytes) in from {
        let total = into.entry(*key).or_insert(0);
        *total = total.saturating_add(*bytes);
    }
}

/// `totals` summed per database.
pub fn database_totals(totals: &TenantTotals) -> HashMap<DatabaseId, u64> {
    sum_by(totals, |(database_id, _)| *database_id)
}

/// `totals` summed per tenant, over databases.
pub fn tenant_totals(totals: &TenantTotals) -> HashMap<TenantId, u64> {
    sum_by(totals, |(_, tenant_id)| *tenant_id)
}

/// `totals` summed per the key `key_of` picks from each entry.
fn sum_by<K: std::hash::Hash + Eq>(
    totals: &TenantTotals,
    key_of: impl Fn(&(DatabaseId, TenantId)) -> K,
) -> HashMap<K, u64> {
    let mut out: HashMap<K, u64> = HashMap::new();
    for (key, bytes) in totals {
        let total = out.entry(key_of(key)).or_insert(0);
        *total = total.saturating_add(*bytes);
    }
    out
}

/// The latest [`StorageShare`] of this node. Empty until the first sample.
#[derive(Debug, Default)]
pub struct StorageShares {
    latest: RwLock<Option<Arc<StorageShare>>>,
}

impl StorageShares {
    /// Replace the latest share with `share`. Returns the share it replaces.
    pub fn publish(&self, share: Arc<StorageShare>) -> Option<Arc<StorageShare>> {
        let mut latest = self.latest.write().unwrap_or_else(|p| p.into_inner());
        latest.replace(share)
    }

    /// The latest share, or `None` before the first sample.
    pub fn latest(&self) -> Option<Arc<StorageShare>> {
        self.latest
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A share of `(group, database, tenant, bytes)` rows, every group at term 1.
    fn share(rows: &[(u64, u64, u64, u64)]) -> StorageShare {
        let mut groups: BTreeMap<u64, GroupTotals> = BTreeMap::new();
        for (g, d, t, b) in rows {
            let group = groups.entry(*g).or_insert_with(|| GroupTotals {
                term: 1,
                totals: TenantTotals::new(),
            });
            group
                .totals
                .insert((DatabaseId::new(*d), TenantId::new(*t)), *b);
        }
        StorageShare {
            groups,
            led_vshards: vec![0, 1],
            sampled_at: Instant::now(),
        }
    }

    #[test]
    fn totals_sum_per_database_and_per_tenant() {
        let s = share(&[(1, 0, 1, 6), (2, 0, 1, 4), (1, 0, 2, 5), (2, 7, 1, 3)]);
        assert_eq!(s.totals()[&(DatabaseId::new(0), TenantId::new(1))], 10);
        let databases = database_totals(&s.totals());
        assert_eq!(databases[&DatabaseId::new(0)], 15);
        assert_eq!(databases[&DatabaseId::new(7)], 3);
        let tenants = tenant_totals(&s.totals());
        assert_eq!(tenants[&TenantId::new(1)], 13);
        assert_eq!(tenants[&TenantId::new(2)], 5);
    }

    #[test]
    fn publish_replaces_the_latest_share() {
        let shares = StorageShares::default();
        assert!(shares.latest().is_none());
        assert!(shares.publish(Arc::new(share(&[(1, 0, 1, 10)]))).is_none());
        let prior = shares
            .publish(Arc::new(share(&[(1, 0, 1, 20)])))
            .expect("prior share");
        assert_eq!(prior.totals().values().sum::<u64>(), 10);
        let latest = shares.latest().expect("latest share");
        assert_eq!(latest.totals().values().sum::<u64>(), 20);
    }
}

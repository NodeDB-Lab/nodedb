// SPDX-License-Identifier: BUSL-1.1

//! This node's last sampled share of stored bytes.
//!
//! A share counts the vShards this node led when it was sampled. A cluster
//! total sums the shares of every node.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use nodedb_types::{DatabaseId, TenantId};

/// One sample of this node's stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageShare {
    /// Stored bytes per `(database, tenant)` on the vShards in `led_vshards`.
    pub totals: HashMap<(DatabaseId, TenantId), u64>,
    /// The vShards this node led when it was sampled, in ascending order.
    pub led_vshards: Vec<u32>,
    /// When the sample completed.
    pub sampled_at: Instant,
}

impl StorageShare {
    /// The share's stored bytes per database.
    pub fn database_totals(&self) -> HashMap<DatabaseId, u64> {
        let mut out: HashMap<DatabaseId, u64> = HashMap::new();
        for ((database_id, _), bytes) in &self.totals {
            let total = out.entry(*database_id).or_insert(0);
            *total = total.saturating_add(*bytes);
        }
        out
    }

    /// The share's stored bytes per tenant, summed over databases.
    pub fn tenant_totals(&self) -> HashMap<TenantId, u64> {
        let mut out: HashMap<TenantId, u64> = HashMap::new();
        for ((_, tenant_id), bytes) in &self.totals {
            let total = out.entry(*tenant_id).or_insert(0);
            *total = total.saturating_add(*bytes);
        }
        out
    }
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

    fn share(rows: &[(u64, u64, u64)]) -> StorageShare {
        StorageShare {
            totals: rows
                .iter()
                .map(|(d, t, b)| ((DatabaseId::new(*d), TenantId::new(*t)), *b))
                .collect(),
            led_vshards: vec![0, 1],
            sampled_at: Instant::now(),
        }
    }

    #[test]
    fn totals_sum_per_database_and_per_tenant() {
        let s = share(&[(0, 1, 10), (0, 2, 5), (7, 1, 3)]);
        let databases = s.database_totals();
        assert_eq!(databases[&DatabaseId::new(0)], 15);
        assert_eq!(databases[&DatabaseId::new(7)], 3);
        let tenants = s.tenant_totals();
        assert_eq!(tenants[&TenantId::new(1)], 13);
        assert_eq!(tenants[&TenantId::new(2)], 5);
    }

    #[test]
    fn publish_replaces_the_latest_share() {
        let shares = StorageShares::default();
        assert!(shares.latest().is_none());
        assert!(shares.publish(Arc::new(share(&[(0, 1, 10)]))).is_none());
        let prior = shares
            .publish(Arc::new(share(&[(0, 1, 20)])))
            .expect("prior share");
        assert_eq!(prior.totals.values().sum::<u64>(), 10);
        let latest = shares.latest().expect("latest share");
        assert_eq!(latest.totals.values().sum::<u64>(), 20);
    }
}

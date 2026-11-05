// SPDX-License-Identifier: BUSL-1.1

//! The cluster-wide stored-byte totals, read lock-free by admission.
//!
//! The aggregator publishes one immutable snapshot per round. A reader loads
//! the current snapshot with one atomic pointer read and never blocks the
//! publisher. A node with no peers publishes here too, so every reader has
//! one source.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use nodedb_types::{DatabaseId, TenantId};

use super::shares::{TenantTotals, database_totals, tenant_totals};

/// One published round of cluster totals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterTotalsSnapshot {
    /// Stored bytes per `(database, tenant)`.
    pub by_database_tenant: TenantTotals,
    /// Stored bytes per database.
    pub by_database: HashMap<DatabaseId, u64>,
    /// Stored bytes per tenant, over every database.
    pub by_tenant: HashMap<TenantId, u64>,
}

impl ClusterTotalsSnapshot {
    /// The snapshot of `totals`, with its roll-ups.
    pub fn from_totals(totals: TenantTotals) -> Self {
        Self {
            by_database: database_totals(&totals),
            by_tenant: tenant_totals(&totals),
            by_database_tenant: totals,
        }
    }
}

/// The latest cluster totals and whether one complete round has published.
#[derive(Debug)]
pub struct ClusterStorageTotals {
    snapshot: ArcSwap<ClusterTotalsSnapshot>,
    has_total: AtomicBool,
}

impl Default for ClusterStorageTotals {
    fn default() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(ClusterTotalsSnapshot::default()),
            has_total: AtomicBool::new(false),
        }
    }
}

impl ClusterStorageTotals {
    /// Whether a round with a report from every active member has published.
    /// Admission trusts the totals only once this holds.
    pub fn has_total(&self) -> bool {
        self.has_total.load(Ordering::Acquire)
    }

    /// The cluster's stored bytes of `tenant_id` in `database_id`.
    pub fn bytes(&self, database_id: DatabaseId, tenant_id: TenantId) -> u64 {
        self.snapshot
            .load()
            .by_database_tenant
            .get(&(database_id, tenant_id))
            .copied()
            .unwrap_or(0)
    }

    /// The cluster's stored bytes of `database_id`, over every tenant.
    pub fn database_bytes(&self, database_id: DatabaseId) -> u64 {
        self.snapshot
            .load()
            .by_database
            .get(&database_id)
            .copied()
            .unwrap_or(0)
    }

    /// The cluster's stored bytes of `tenant_id`, over every database.
    pub fn tenant_bytes(&self, tenant_id: TenantId) -> u64 {
        self.snapshot
            .load()
            .by_tenant
            .get(&tenant_id)
            .copied()
            .unwrap_or(0)
    }

    /// The latest snapshot.
    pub fn snapshot(&self) -> Arc<ClusterTotalsSnapshot> {
        self.snapshot.load_full()
    }

    /// Replace the totals with `totals`. `complete` marks a round with a
    /// report from every active member, and the first one sets
    /// [`Self::has_total`]. Returns the snapshot it replaces.
    pub fn publish(&self, totals: TenantTotals, complete: bool) -> Arc<ClusterTotalsSnapshot> {
        let prior = self
            .snapshot
            .swap(Arc::new(ClusterTotalsSnapshot::from_totals(totals)));
        if complete {
            self.has_total.store(true, Ordering::Release);
        }
        prior
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(rows: &[(u64, u64, u64)]) -> TenantTotals {
        rows.iter()
            .map(|(d, t, b)| ((DatabaseId::new(*d), TenantId::new(*t)), *b))
            .collect()
    }

    #[test]
    fn reads_are_zero_and_incomplete_before_the_first_round() {
        let store = ClusterStorageTotals::default();
        assert!(!store.has_total());
        assert_eq!(store.bytes(DatabaseId::new(1), TenantId::new(1)), 0);
        assert_eq!(store.database_bytes(DatabaseId::new(1)), 0);
        assert_eq!(store.tenant_bytes(TenantId::new(1)), 0);
    }

    #[test]
    fn a_round_publishes_totals_and_roll_ups() {
        let store = ClusterStorageTotals::default();
        store.publish(totals(&[(1, 1, 10), (1, 2, 5), (2, 1, 3)]), false);
        assert!(!store.has_total(), "an incomplete round sets no total");
        assert_eq!(store.bytes(DatabaseId::new(1), TenantId::new(2)), 5);
        assert_eq!(store.database_bytes(DatabaseId::new(1)), 15);
        assert_eq!(store.tenant_bytes(TenantId::new(1)), 13);

        let prior = store.publish(totals(&[(1, 1, 4)]), true);
        assert!(store.has_total());
        assert_eq!(prior.by_database[&DatabaseId::new(2)], 3);
        assert_eq!(store.database_bytes(DatabaseId::new(2)), 0);

        store.publish(totals(&[]), false);
        assert!(store.has_total(), "a later incomplete round keeps the flag");
    }
}

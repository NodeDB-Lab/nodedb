//! DDL invalidation hook for the gateway plan cache.
//!
//! `PlanCacheInvalidator` is stored on `SharedState` and called from the
//! metadata applier's post-apply path whenever a descriptor (collection,
//! trigger, etc.) is successfully committed.
//!
//! # Design
//!
//! The invalidator is an `Arc<PlanCacheInvalidator>` so it can be installed
//! on `SharedState` before the `PlanCache` is constructed and shared with
//! the gateway without a circular dependency. It wraps the cache in a
//! `Weak<PlanCache>` so the cache can be dropped independently.

use std::sync::{Arc, Weak};

use tracing::debug;

use super::plan_cache::PlanCache;

/// Callback object stored on `SharedState.gateway_invalidator`.
///
/// Called from `catalog_entry::post_apply` after every DDL commit that
/// mutates a descriptor. The call is synchronous and low-overhead — it
/// only acquires a `Mutex<VecDeque>` and drops entries matching `name`.
pub struct PlanCacheInvalidator {
    cache: Weak<PlanCache>,
}

impl PlanCacheInvalidator {
    /// Construct from a weak reference to the plan cache.
    pub fn new(cache: &Arc<PlanCache>) -> Self {
        Self {
            cache: Arc::downgrade(cache),
        }
    }

    /// Evict all cache entries whose version set references `name` at any
    /// version other than `new_version`.
    ///
    /// No-op if the plan cache has been dropped.
    pub fn invalidate(&self, name: &str, new_version: u64) {
        if let Some(cache) = self.cache.upgrade() {
            debug!(
                collection = name,
                new_version, "gateway plan cache: invalidating entries for descriptor"
            );
            cache.invalidate_descriptor(name, new_version);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::physical_plan::{KvOp, PhysicalPlan};
    use crate::control::gateway::plan_cache::{PlanCache, PlanCacheKey, hash_sql};
    use crate::control::gateway::version_set::GatewayVersionSet;

    fn kv_plan() -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: vec![],
            rls_filters: vec![],
        }))
    }

    fn key_for(sql: &str, col: &str, version: u64) -> PlanCacheKey {
        PlanCacheKey {
            sql_text_hash: hash_sql(sql),
            placeholder_types_hash: 0,
            version_set: GatewayVersionSet::from_pairs(vec![(col.into(), version)]),
        }
    }

    #[test]
    fn invalidate_drops_stale_entries_only() {
        let cache = Arc::new(PlanCache::new(16));
        let invalidator = PlanCacheInvalidator::new(&cache);

        let k_users_v1 = key_for("q1", "users", 1);
        let k_orders_v5 = key_for("q2", "orders", 5);

        cache.insert(k_users_v1.clone(), kv_plan());
        cache.insert(k_orders_v5.clone(), kv_plan());
        assert_eq!(cache.len(), 2);

        invalidator.invalidate("users", 2);

        // users entry at version=1 is gone; orders entry is intact.
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&k_users_v1).is_none());
        assert!(cache.get(&k_orders_v5).is_some());
    }

    #[test]
    fn invalidate_noop_when_cache_dropped() {
        let cache = Arc::new(PlanCache::new(4));
        let invalidator = PlanCacheInvalidator::new(&cache);
        drop(cache);
        // Should not panic.
        invalidator.invalidate("any_collection", 99);
    }
}

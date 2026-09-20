// SPDX-License-Identifier: BUSL-1.1

//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`.

use std::hash::{DefaultHasher, Hash, Hasher};

use nodedb_physical::physical_plan::PhysicalPlan;

use super::plan_keys::touched_collections;

/// Sort `pairs` by collection name and drop duplicate names (last write
/// wins), the invariant every constructor of [`GatewayVersionSet`] keeps.
fn sorted_deduped(mut pairs: Vec<(String, u64)>) -> Vec<(String, u64)> {
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs.dedup_by(|a, b| a.0 == b.0);
    pairs
}

/// Deterministic ordered set of `(collection_name, descriptor_version)` pairs.
///
/// - Sorted by `collection_name` for stable equality comparisons.
/// - Duplicate names are de-duped (last write wins — within a single plan
///   the version is stable, so duplicates carry the same version).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayVersionSet(Vec<(String, u64)>);

impl GatewayVersionSet {
    /// Construct from explicit (name, version) pairs.
    pub fn from_pairs(pairs: Vec<(String, u64)>) -> Self {
        Self(sorted_deduped(pairs))
    }

    /// Fold one more `(name, version)` pair into the set, re-sorting and
    /// re-deduping. Used to add the permission-tree / RLS pseudo-entries
    /// alongside the real collection entries `from_plan` already collected.
    pub fn with_extra(mut self, name: String, version: u64) -> Self {
        self.0.push((name, version));
        Self(sorted_deduped(self.0))
    }

    /// Re-look-up the current descriptor version for every collection already
    /// in this set, returning a new set with refreshed versions.
    ///
    /// Used to check a cached version set is still current before trusting a
    /// plan-cache hit: if the result equals the original, the cached plan is
    /// still valid. `version_fn` receives a collection name and returns the
    /// current descriptor version (or 0 if unknown).
    pub fn reverify(&self, version_fn: impl Fn(&str) -> u64) -> Self {
        let pairs: Vec<(String, u64)> = self
            .0
            .iter()
            .map(|(name, _)| {
                let v = version_fn(name);
                (name.clone(), v)
            })
            .collect();
        Self::from_pairs(pairs)
    }

    /// Collect all collection names touched by a plan with the provided
    /// version lookup function.
    ///
    /// `version_fn` receives a collection name and returns the current
    /// descriptor version (or 0 if unknown).
    pub fn from_plan(plan: &PhysicalPlan, version_fn: impl Fn(&str) -> u64) -> Self {
        let names = touched_collections(plan);
        let pairs: Vec<(String, u64)> = names
            .into_iter()
            .map(|name| {
                let v = version_fn(&name);
                (name, v)
            })
            .collect();
        Self(sorted_deduped(pairs))
    }

    /// Iterate over `(collection, version)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(String, u64)> {
        self.0.iter()
    }

    /// Returns `true` if the set mentions `name` at any version.
    pub fn contains_collection(&self, name: &str) -> bool {
        self.0.iter().any(|(n, _)| n == name)
    }

    /// Returns `true` if the set mentions `name` at exactly `version`.
    pub fn matches(&self, name: &str, version: u64) -> bool {
        self.0
            .iter()
            .any(|(n, v)| n.as_str() == name && *v == version)
    }

    /// Stable u64 hash of this set, used as part of `PlanCacheKey`.
    pub fn stable_hash(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::gateway::version_set::{permission_tree_version_key, rls_version_key};
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn with_extra_folds_in_a_pseudo_entry_and_stays_deterministic() {
        let vs = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 2)
            .with_extra(rls_version_key(7), 9);
        assert_eq!(vs.len(), 3);
        assert!(vs.matches(&permission_tree_version_key(7), 2));
        assert!(vs.matches(&rls_version_key(7), 9));
        assert!(vs.matches("orders", 4));
    }

    /// The plan-cache key is stale the instant the tenant's stamped
    /// permission-tree version diverges from what's live, exactly like a
    /// bumped collection descriptor.
    #[test]
    fn with_extra_pseudo_entry_participates_in_equality() {
        let a = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 1);
        let b = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 2);
        assert_ne!(a, b);
    }

    #[test]
    fn from_plan_kv_get() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            key: b"key".to_vec(),
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let vs = GatewayVersionSet::from_plan(&plan, |_| 5);
        assert_eq!(vs.len(), 1);
        assert!(vs.matches("users", 5));
    }

    #[test]
    fn from_plan_deterministic_order() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "alpha"),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let vs1 = GatewayVersionSet::from_plan(&plan, |_| 1);
        let vs2 = GatewayVersionSet::from_plan(&plan, |_| 1);
        assert_eq!(vs1, vs2);
        assert_eq!(vs1.stable_hash(), vs2.stable_hash());
    }

    #[test]
    fn contains_collection() {
        let vs = GatewayVersionSet::from_pairs(vec![("orders".into(), 3), ("users".into(), 7)]);
        assert!(vs.contains_collection("orders"));
        assert!(vs.contains_collection("users"));
        assert!(!vs.contains_collection("products"));
    }

    #[test]
    fn dedup_on_construction() {
        let vs = GatewayVersionSet::from_pairs(vec![
            ("a".into(), 1),
            ("a".into(), 1), // duplicate
        ]);
        assert_eq!(vs.len(), 1);
    }
}

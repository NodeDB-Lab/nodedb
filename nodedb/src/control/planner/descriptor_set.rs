// SPDX-License-Identifier: BUSL-1.1

//! Per-plan descriptor version set — the C.6 plan-cache key.
//!
//! When the planner resolves a collection through
//! [`crate::control::planner::catalog_adapter::OriginCatalog`], it
//! records the descriptor id and version into a
//! [`DescriptorVersionSet`]. The resulting set is handed back
//! alongside the compiled physical tasks and cached. On
//! subsequent lookups the cache validates each recorded
//! `(id, version)` against the current `SystemCatalog` — if any
//! version has bumped, that one entry is evicted and the plan
//! is re-planned. Unrelated DDLs no longer invalidate unrelated
//! cached plans the way a global schema counter would.
//!
//! The set is ordered and de-duped by `(object_type, database_id,
//! tenant_id, name)` so two plans that touch the same descriptors
//! in the same version produce byte-identical cache keys regardless
//! of resolution order.

use nodedb_cluster::DescriptorId;

/// A stable, de-duped set of `(descriptor_id, version)` pairs
/// the planner read while compiling a plan.
///
/// Stable == entries are sorted by the
/// `(kind, database_id, tenant_id, name)` tuple so equality
/// comparisons between two sets are insertion-order independent. De-duped
/// == a second insertion of the same descriptor id with the
/// same version is a no-op; a second insertion with a
/// different version panics (in debug) / replaces (in release)
/// because a single plan compilation should never observe two
/// different versions of the same descriptor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescriptorVersionSet {
    /// Sorted by `(kind, database_id, tenant_id, name)`.
    entries: Vec<(DescriptorId, u64)>,
    /// Descriptors the plan read without a lease, at their catalog content
    /// version. Same order. An array is one: its lease version is fixed,
    /// so only this entry changes when an ALTER changes the schema a plan
    /// embeds. Lease admission never sees these.
    unleased: Vec<(DescriptorId, u64)>,
    /// The tenant's permission-tree version observed at plan-build time.
    /// `0` when no permission cache was consulted. Compared in
    /// [`Self::all_fresh`] alongside the descriptor entries so a revoked
    /// grant evicts the plan exactly like a bumped descriptor does.
    permission_tree_version: u64,
    /// The tenant's RLS policy-store version observed at plan-build time.
    rls_version: u64,
    /// The redaction policy-store version the plan's column reads were
    /// checked against.
    redaction_version: u64,
}

impl DescriptorVersionSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the permission-tree version this plan was built against.
    pub fn set_permission_tree_version(&mut self, version: u64) {
        self.permission_tree_version = version;
    }

    /// Record the RLS policy-store version this plan was built against.
    pub fn set_rls_version(&mut self, version: u64) {
        self.rls_version = version;
    }

    pub fn permission_tree_version(&self) -> u64 {
        self.permission_tree_version
    }

    pub fn rls_version(&self) -> u64 {
        self.rls_version
    }

    /// Record the redaction policy-store version this plan was checked
    /// against.
    pub fn set_redaction_version(&mut self, version: u64) {
        self.redaction_version = version;
    }

    pub fn redaction_version(&self) -> u64 {
        self.redaction_version
    }

    /// Record a descriptor read at the given version. O(log n)
    /// binary search + O(n) worst-case insert on first
    /// observation; O(log n) no-op on re-observation of the
    /// same `(id, version)`.
    ///
    /// Panics in debug builds if the same `id` is recorded with
    /// a different version — that would indicate the planner
    /// read the same descriptor twice under different catalog
    /// snapshots, which is a planning bug. In release builds
    /// the later value wins.
    pub fn record(&mut self, id: DescriptorId, version: u64) {
        insert_sorted(&mut self.entries, id, version);
    }

    /// Record a descriptor read without a lease at its content `version`.
    /// Same ordering and duplicate rules as [`Self::record`]. Only the
    /// freshness check reads it.
    pub fn record_unleased(&mut self, id: DescriptorId, version: u64) {
        insert_sorted(&mut self.unleased, id, version);
    }

    /// Iterate `(id, version)` pairs in stable order.
    pub fn iter(&self) -> impl Iterator<Item = (&DescriptorId, u64)> {
        self.entries.iter().map(|(id, v)| (id, *v))
    }

    /// Number of distinct descriptors recorded.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Validate the recorded versions against a current-version
    /// lookup. Returns `true` iff every `(id, version)` still
    /// matches. Returns `false` on the first mismatch or if any
    /// recorded descriptor no longer exists.
    ///
    /// Used by the plan cache on every hit — a cached plan is
    /// only reusable while every descriptor it was built
    /// against still exists at the same version, AND the
    /// tenant's permission-tree, RLS, and redaction policy state has
    /// not changed since the plan was built.
    pub fn all_fresh<F>(&self, current: F, policies: PolicyVersions) -> bool
    where
        F: Fn(&DescriptorId) -> Option<u64>,
    {
        self.permission_tree_version == policies.permission_tree
            && self.rls_version == policies.rls
            && self.redaction_version == policies.redaction
            && self
                .entries
                .iter()
                .chain(&self.unleased)
                .all(|(id, v)| current(id) == Some(*v))
    }

    /// Merge `other`, the set a later statement of the same request
    /// recorded, into this set.
    ///
    /// A descriptor both sets hold at one version keeps one entry. A
    /// descriptor at two versions returns `RetryableSchemaChanged` and
    /// leaves this set unchanged. Each statement plans with its own catalog
    /// adapter, so two versions mean a DDL committed between two statements.
    /// A caller inside `retry_on_schema_change` then plans the request again.
    ///
    /// Each policy version takes the lower value. A lower version can only
    /// under-state freshness, which costs one replan.
    pub fn merge_statement(&mut self, other: &DescriptorVersionSet) -> crate::Result<()> {
        refuse_version_conflict(&self.entries, &other.entries)?;
        refuse_version_conflict(&self.unleased, &other.unleased)?;
        for (id, version) in &other.entries {
            insert_sorted(&mut self.entries, id.clone(), *version);
        }
        for (id, version) in &other.unleased {
            insert_sorted(&mut self.unleased, id.clone(), *version);
        }
        self.permission_tree_version = self
            .permission_tree_version
            .min(other.permission_tree_version);
        self.rls_version = self.rls_version.min(other.rls_version);
        self.redaction_version = self.redaction_version.min(other.redaction_version);
        Ok(())
    }
}

/// Return `RetryableSchemaChanged` for the first descriptor of `other` that
/// `held` records at a different version.
fn refuse_version_conflict(
    held: &[(DescriptorId, u64)],
    other: &[(DescriptorId, u64)],
) -> crate::Result<()> {
    for (id, version) in other {
        if let Ok(index) = held.binary_search_by(|(k, _)| cmp_id(k, id))
            && held
                .get(index)
                .is_some_and(|(_, held_version)| held_version != version)
        {
            return Err(crate::Error::RetryableSchemaChanged {
                descriptor: id.name.clone(),
            });
        }
    }
    Ok(())
}

/// Insert `(id, version)` into the sorted `entries`. A repeat of the same
/// `id` keeps one entry.
///
/// Panics in debug builds if `id` repeats with a different version: the
/// planner read the same descriptor twice under different catalog snapshots,
/// which is a planning bug. In release builds the later value wins.
fn insert_sorted(entries: &mut Vec<(DescriptorId, u64)>, id: DescriptorId, version: u64) {
    match entries.binary_search_by(|(k, _)| cmp_id(k, &id)) {
        Ok(idx) => {
            debug_assert_eq!(
                entries[idx].1, version,
                "planner observed two versions of the same descriptor in one plan"
            );
            entries[idx].1 = version;
        }
        Err(idx) => {
            entries.insert(idx, (id, version));
        }
    }
}

/// The policy-store versions a cached plan is compared against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PolicyVersions {
    pub permission_tree: u64,
    pub rls: u64,
    pub redaction: u64,
}

/// Total-order comparison for `DescriptorId` since the cluster
/// type does not derive `Ord` (only `Hash + Eq`). We define our
/// own by `(kind discriminant, database_id, tenant_id, name)`.
fn cmp_id(a: &DescriptorId, b: &DescriptorId) -> std::cmp::Ordering {
    (a.kind as u8, a.database_id, a.tenant_id, &a.name).cmp(&(
        b.kind as u8,
        b.database_id,
        b.tenant_id,
        &b.name,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;

    fn id(name: &str) -> DescriptorId {
        DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
    }

    #[test]
    fn record_sorts_and_dedupes() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("b"), 2);
        set.record(id("a"), 1);
        set.record(id("c"), 3);
        set.record(id("a"), 1); // duplicate, no-op

        let names: Vec<&str> = set.iter().map(|(id, _)| id.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn record_different_kinds_coexist() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("orders"), 1);
        set.record(
            DescriptorId::new(0, 1, DescriptorKind::Function, "orders".to_string()),
            1,
        );
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn same_name_in_different_databases_coexist_in_stable_order() {
        let mut set = DescriptorVersionSet::new();
        set.record(
            DescriptorId::new(9, 1, DescriptorKind::Collection, "orders"),
            2,
        );
        set.record(
            DescriptorId::new(3, 1, DescriptorKind::Collection, "orders"),
            1,
        );

        let entries: Vec<_> = set
            .iter()
            .map(|(id, version)| (id.database_id, version))
            .collect();
        assert_eq!(entries, vec![(3, 1), (9, 2)]);
    }

    fn policies(permission_tree: u64, rls: u64, redaction: u64) -> PolicyVersions {
        PolicyVersions {
            permission_tree,
            rls,
            redaction,
        }
    }

    #[test]
    fn all_fresh_true_when_versions_match() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        set.record(id("b"), 2);
        let lookup = |id: &DescriptorId| match id.name.as_str() {
            "a" => Some(1),
            "b" => Some(2),
            _ => None,
        };
        assert!(set.all_fresh(lookup, policies(0, 0, 0)));
    }

    #[test]
    fn all_fresh_false_on_version_mismatch() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        set.record(id("b"), 2);
        let lookup = |id: &DescriptorId| match id.name.as_str() {
            "a" => Some(1),
            "b" => Some(3), // bumped
            _ => None,
        };
        assert!(!set.all_fresh(lookup, policies(0, 0, 0)));
    }

    #[test]
    fn unleased_entries_gate_freshness_but_not_iteration() {
        let array = DescriptorId::new(0, 1, DescriptorKind::Array, "scene".to_string());
        let mut set = DescriptorVersionSet::new();
        set.record_unleased(array.clone(), 7);
        assert!(set.is_empty(), "lease admission iterates no unleased entry");
        assert!(set.all_fresh(|_| Some(7), policies(0, 0, 0)));
        assert!(!set.all_fresh(|_| Some(8), policies(0, 0, 0)));
        assert!(!set.all_fresh(|_| None, policies(0, 0, 0)));
    }

    #[test]
    fn all_fresh_false_on_missing_descriptor() {
        let mut set = DescriptorVersionSet::new();
        set.record(id("a"), 1);
        let lookup = |_: &DescriptorId| None;
        assert!(!set.all_fresh(lookup, policies(0, 0, 0)));
    }

    #[test]
    fn empty_set_is_always_fresh() {
        let set = DescriptorVersionSet::new();
        let lookup = |_: &DescriptorId| None;
        assert!(set.all_fresh(lookup, policies(0, 0, 0)));
    }

    #[test]
    fn all_fresh_false_on_permission_tree_version_mismatch() {
        let mut set = DescriptorVersionSet::new();
        set.set_permission_tree_version(3);
        let lookup = |_: &DescriptorId| None;
        assert!(set.all_fresh(lookup, policies(3, 0, 0)));
        assert!(
            !set.all_fresh(lookup, policies(4, 0, 0)),
            "a revoked grant bumps the tenant's permission version, so a stale stamp must not read as fresh"
        );
    }

    #[test]
    fn all_fresh_false_on_rls_version_mismatch() {
        let mut set = DescriptorVersionSet::new();
        set.set_rls_version(5);
        let lookup = |_: &DescriptorId| None;
        assert!(set.all_fresh(lookup, policies(0, 5, 0)));
        assert!(!set.all_fresh(lookup, policies(0, 6, 0)));
    }

    /// A redaction policy change must re-check the plan's column reads.
    #[test]
    fn all_fresh_false_on_redaction_version_mismatch() {
        let mut set = DescriptorVersionSet::new();
        set.set_redaction_version(2);
        let lookup = |_: &DescriptorId| None;
        assert!(set.all_fresh(lookup, policies(0, 0, 2)));
        assert!(!set.all_fresh(lookup, policies(0, 0, 3)));
    }

    #[test]
    fn equal_sets_are_byte_equal_regardless_of_insert_order() {
        let mut a = DescriptorVersionSet::new();
        a.record(id("z"), 1);
        a.record(id("a"), 2);
        let mut b = DescriptorVersionSet::new();
        b.record(id("a"), 2);
        b.record(id("z"), 1);
        assert_eq!(a, b);
    }
}

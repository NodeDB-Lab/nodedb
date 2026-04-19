//! Tenant-partitioned CSR index.
//!
//! The graph engine multiplexes many tenants onto one per-core `CsrIndex`.
//! Historically that multiplexing was done by prepending `"{tid}:"` to every
//! node key before handing it to the CSR — a lexical scheme that required
//! every API boundary (results, traversal output, algorithm emit) to strip
//! the prefix on the way back out. The heuristic strippers were fragile
//! (mangled any id that happened to match the shape) and the strip was
//! opt-in (forgetting it leaked internal keys to clients).
//!
//! `ShardedCsrIndex` replaces that scheme with structural partitioning:
//! one independent `CsrIndex` per tenant, keyed in a `HashMap<TenantId, _>`.
//! Node keys inside a partition are the raw user-visible names — no prefix,
//! no strip, no heuristic. Cross-tenant access is not a question of auth
//! checks; it is structurally impossible because partitions don't share
//! key space.
//!
//! ## Invariants
//!
//! - **One tenant per partition.** Every `CsrIndex` held in `partitions`
//!   contains only one tenant's nodes and edges. Mixing is a programming
//!   bug, not a runtime condition.
//! - **No key prefixing.** Callers pass user-visible names directly; the
//!   partition never sees a scoped form.
//! - **Partition lifecycle is explicit.** `get_or_create` constructs on
//!   first use; `drop_partition` removes atomically. No auto-eviction at
//!   this layer (belongs to a future pool-management concern).
//! - **Data Plane shape preserved.** This type is `!Send` in the same way
//!   `CsrIndex` is (via `Cell<u32>` access counters). It is owned by a
//!   single Data Plane core.

use std::collections::HashMap;
use std::collections::hash_map::{Entry, Iter, IterMut};

use nodedb_types::TenantId;

use crate::csr::CsrIndex;

/// Per-tenant partitioned CSR index.
///
/// Holds one `CsrIndex` per tenant on the owning Data Plane core.
/// Handlers resolve the caller's tenant to a partition up front; all
/// downstream graph operations (algorithms, MATCH, traversal) run
/// against that single partition and cannot reach any other tenant's
/// state.
pub struct ShardedCsrIndex {
    partitions: HashMap<TenantId, CsrIndex>,
}

impl ShardedCsrIndex {
    /// Construct an empty sharded index with no partitions.
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    /// Shared access to a tenant's partition, if it exists.
    ///
    /// Returns `None` if the tenant has never inserted any graph data.
    /// Callers expecting a read-only view of a possibly-empty partition
    /// should treat `None` as "empty" rather than as an error.
    pub fn partition(&self, tid: TenantId) -> Option<&CsrIndex> {
        self.partitions.get(&tid)
    }

    /// Mutable access to a tenant's partition, if it exists.
    pub fn partition_mut(&mut self, tid: TenantId) -> Option<&mut CsrIndex> {
        self.partitions.get_mut(&tid)
    }

    /// Mutable access to a tenant's partition, creating an empty one
    /// on first use.
    ///
    /// This is the canonical write-path entry point: insertion handlers
    /// call this once to resolve the partition, then operate on the
    /// returned `&mut CsrIndex` exactly as they would on a standalone
    /// instance.
    pub fn get_or_create(&mut self, tid: TenantId) -> &mut CsrIndex {
        self.partitions.entry(tid).or_default()
    }

    /// Drop a tenant's entire graph state.
    ///
    /// Returns `true` if a partition existed and was removed, `false` if
    /// the tenant had no graph state. Used by tenant-purge flows — O(1)
    /// structural deletion replaces the former "range-scan and erase
    /// every key with the `{tid}:` prefix" approach, which was both
    /// slow and coupled to the lexical encoding.
    pub fn drop_partition(&mut self, tid: TenantId) -> bool {
        self.partitions.remove(&tid).is_some()
    }

    /// Whether a partition exists for the tenant.
    pub fn contains_partition(&self, tid: TenantId) -> bool {
        self.partitions.contains_key(&tid)
    }

    /// Number of tenants with graph state on this core.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Iterate all (tenant, partition) pairs. Used for checkpointing,
    /// memory accounting, and administrative views that genuinely
    /// need to see every tenant's state.
    pub fn iter(&self) -> Iter<'_, TenantId, CsrIndex> {
        self.partitions.iter()
    }

    /// Mutable iteration over all partitions. Used by `compact()` and
    /// similar maintenance passes that apply per-partition without
    /// needing tenant routing.
    pub fn iter_mut(&mut self) -> IterMut<'_, TenantId, CsrIndex> {
        self.partitions.iter_mut()
    }

    /// Compact every partition. Mirrors `CsrIndex::compact` but across
    /// the full set — maintenance handlers call this once per core.
    pub fn compact_all(&mut self) {
        for (_tid, part) in self.iter_mut() {
            part.compact();
        }
    }

    /// Replace an existing partition (or install a new one) with the
    /// given `CsrIndex`. Used by the rebuild path — after rebuilding a
    /// tenant's CSR from persistent edge storage, this installs it
    /// atomically.
    pub fn install_partition(&mut self, tid: TenantId, csr: CsrIndex) {
        self.partitions.insert(tid, csr);
    }

    /// Access or create a partition via the `Entry` API, for cases that
    /// need conditional initialization with a non-default constructor.
    pub fn entry(&mut self, tid: TenantId) -> Entry<'_, TenantId, CsrIndex> {
        self.partitions.entry(tid)
    }
}

impl Default for ShardedCsrIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(n: u32) -> TenantId {
        TenantId::new(n)
    }

    #[test]
    fn empty_sharded_has_no_partitions() {
        let sharded = ShardedCsrIndex::new();
        assert_eq!(sharded.partition_count(), 0);
        assert!(!sharded.contains_partition(tid(1)));
        assert!(sharded.partition(tid(1)).is_none());
    }

    #[test]
    fn get_or_create_installs_empty_partition() {
        let mut sharded = ShardedCsrIndex::new();
        let part = sharded.get_or_create(tid(7));
        assert_eq!(part.node_count(), 0);
        assert!(sharded.contains_partition(tid(7)));
        assert_eq!(sharded.partition_count(), 1);
    }

    #[test]
    fn partitions_are_isolated_by_tenant() {
        // Two tenants insert identically-named nodes. Neither can see
        // the other's graph state — the test encodes the core
        // architectural guarantee of Option C: isolation by partition,
        // not by key prefix.
        let mut sharded = ShardedCsrIndex::new();

        sharded
            .get_or_create(tid(1))
            .add_edge("alice", "knows", "bob")
            .unwrap();
        sharded
            .get_or_create(tid(2))
            .add_edge("alice", "knows", "carol")
            .unwrap();

        let p1 = sharded.partition(tid(1)).unwrap();
        let p2 = sharded.partition(tid(2)).unwrap();

        // Tenant 1 sees alice→bob; no carol.
        assert!(p1.contains_node("alice"));
        assert!(p1.contains_node("bob"));
        assert!(!p1.contains_node("carol"));

        // Tenant 2 sees alice→carol; no bob.
        assert!(p2.contains_node("alice"));
        assert!(p2.contains_node("carol"));
        assert!(!p2.contains_node("bob"));

        // `alice` exists in both partitions but refers to two different
        // logical nodes — no collision, no ambiguity, no lexical prefix.
    }

    #[test]
    fn node_names_are_unprefixed() {
        let mut sharded = ShardedCsrIndex::new();
        sharded
            .get_or_create(tid(42))
            .add_edge("alice", "knows", "bob")
            .unwrap();
        sharded.get_or_create(tid(42)).compact();

        let part = sharded.partition(tid(42)).unwrap();
        let alice_id = part.node_id("alice").expect("alice must be present");
        // The stored name is exactly what the caller inserted — never
        // `"42:alice"`. Structural partitioning keeps shard ids out of
        // user-visible keys.
        assert_eq!(part.node_name(alice_id), "alice");
    }

    #[test]
    fn drop_partition_removes_tenant_state() {
        let mut sharded = ShardedCsrIndex::new();
        sharded
            .get_or_create(tid(1))
            .add_edge("a", "l", "b")
            .unwrap();
        assert!(sharded.contains_partition(tid(1)));

        assert!(sharded.drop_partition(tid(1)));
        assert!(!sharded.contains_partition(tid(1)));
        assert_eq!(sharded.partition_count(), 0);

        // Second drop is a no-op, not an error.
        assert!(!sharded.drop_partition(tid(1)));
    }

    #[test]
    fn drop_partition_does_not_touch_other_tenants() {
        let mut sharded = ShardedCsrIndex::new();
        sharded
            .get_or_create(tid(1))
            .add_edge("a", "l", "b")
            .unwrap();
        sharded
            .get_or_create(tid(2))
            .add_edge("c", "l", "d")
            .unwrap();

        sharded.drop_partition(tid(1));
        assert!(!sharded.contains_partition(tid(1)));
        assert!(sharded.contains_partition(tid(2)));
        assert!(sharded.partition(tid(2)).unwrap().contains_node("c"));
    }

    #[test]
    fn install_partition_replaces_existing() {
        let mut sharded = ShardedCsrIndex::new();
        sharded
            .get_or_create(tid(1))
            .add_edge("old", "l", "value")
            .unwrap();

        let mut replacement = CsrIndex::new();
        replacement.add_edge("new", "l", "value").unwrap();
        sharded.install_partition(tid(1), replacement);

        let part = sharded.partition(tid(1)).unwrap();
        assert!(part.contains_node("new"));
        assert!(!part.contains_node("old"));
    }

    #[test]
    fn compact_all_applies_to_every_partition() {
        let mut sharded = ShardedCsrIndex::new();
        for t in 1..=3 {
            sharded
                .get_or_create(tid(t))
                .add_edge("a", "l", "b")
                .unwrap();
        }
        // Pre-compact: edges live in the write buffer. `compact_all`
        // merges them into the dense CSR arrays for every partition.
        sharded.compact_all();
        for t in 1..=3 {
            let part = sharded.partition(tid(t)).unwrap();
            assert_eq!(part.edge_count(), 1);
            assert_eq!(part.node_count(), 2);
        }
    }
}

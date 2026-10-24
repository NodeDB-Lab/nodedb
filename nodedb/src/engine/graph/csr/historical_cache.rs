// SPDX-License-Identifier: BUSL-1.1

//! CSRs of one collection at one bitemporal cut, for `AS OF` graph reads.
//!
//! The live CSR holds only the current edges. A read at an earlier system
//! time, or at a valid-time instant, walks a CSR built from the versioned
//! edge store at that cut. One Data Plane core owns one cache. It keeps the
//! most recently used CSRs, up to the graph tuning's
//! `max_historical_csr_cache_entries`.
//!
//! A CSR at a cut stays correct while no version at or below the cut changes.
//! Two rules keep a cached CSR correct:
//!
//! - A cut above the core's applied ordinal is built and answered, never
//!   cached: a write still to come on this core can land at or below it.
//! - Every change to a version at or below a cached cut drops that CSR: a
//!   version written at or below it, a version removed, a TRUNCATE cut
//!   installed or removed, a purge, a restore, a node-label version.
//!
//! Every node of a historical CSR takes its identity and labels at the cut:
//! each endpoint's surrogate from the edge version the cut resolves to, and
//! each node's labels from its node-label version at the cut. A node deleted
//! after the cut keeps both.

use nodedb_graph::CsrIndex;
use nodedb_mem::{EngineId, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};
use std::sync::Arc;

use crate::engine::graph::edge_store::EdgeStore;

/// The identity of one historical CSR.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HistoricalCsrKey {
    pub db: DatabaseId,
    pub tid: TenantId,
    pub collection: String,
    /// The system-time ordinal the CSR's edges are read at.
    pub cutoff_ordinal: i64,
    /// The valid-time instant every edge's valid time contains, or `None`.
    pub valid_at_ms: Option<i64>,
}

/// Where a CSR is built from.
pub struct HistoricalCsrSource<'a> {
    pub store: &'a EdgeStore,
    pub governor: &'a Arc<MemoryGovernor>,
    /// Every version this core will apply at or below this ordinal is
    /// already applied.
    pub applied_ordinal: i64,
    /// How many CSRs the cache keeps.
    pub max_entries: usize,
}

/// One core's historical CSRs, least recently used first.
#[derive(Default)]
pub struct HistoricalCsrCache {
    entries: Vec<(HistoricalCsrKey, CsrIndex)>,
    /// The last CSR built at a cut the cache does not keep. It answers the
    /// read that built it, and is replaced by the next such build.
    transient: Option<(HistoricalCsrKey, CsrIndex)>,
}

impl HistoricalCsrCache {
    /// The CSR of `key`, built from `source` when the cache has none. The
    /// build reserves its memory under the key's
    /// `(database, tenant, EngineId::Graph)` scope.
    pub fn get_or_build(
        &mut self,
        key: HistoricalCsrKey,
        source: HistoricalCsrSource<'_>,
    ) -> crate::Result<&CsrIndex> {
        if let Some(index) = self.entries.iter().position(|(k, _)| *k == key) {
            let entry = self.entries.remove(index);
            self.entries.push(entry);
            return self.newest_entry();
        }
        if !self.transient.as_ref().is_some_and(|(k, _)| *k == key) {
            let csr = build(&key, &source)?;
            if key.cutoff_ordinal <= source.applied_ordinal && source.max_entries > 0 {
                self.entries.push((key, csr));
                let excess = self.entries.len().saturating_sub(source.max_entries);
                self.entries.drain(..excess);
                return self.newest_entry();
            }
            self.transient = Some((key, csr));
        }
        match &self.transient {
            Some((_, csr)) => Ok(csr),
            None => Err(missing_after_build()),
        }
    }

    /// The CSR of `key` when the cache holds it.
    pub fn get(&self, key: &HistoricalCsrKey) -> Option<&CsrIndex> {
        self.entries
            .iter()
            .chain(self.transient.iter())
            .find(|(k, _)| k == key)
            .map(|(_, csr)| csr)
    }

    /// Drop every CSR of `collection` whose cut is at or above
    /// `system_from`: a version written there changes what the cut reads.
    pub fn note_version(
        &mut self,
        db: DatabaseId,
        tid: TenantId,
        collection: &str,
        system_from: i64,
    ) {
        self.drop_where(|k| {
            k.db == db
                && k.tid == tid
                && k.collection == collection
                && k.cutoff_ordinal >= system_from
        });
    }

    /// Drop every CSR of `collection`.
    pub fn invalidate_collection(&mut self, db: DatabaseId, tid: TenantId, collection: &str) {
        self.drop_where(|k| k.db == db && k.tid == tid && k.collection == collection);
    }

    /// Drop every CSR of a `(database, tenant)`. Node labels are tenant-wide,
    /// so a node-label version drops the tenant's CSRs.
    pub fn invalidate_tenant(&mut self, db: DatabaseId, tid: TenantId) {
        self.drop_where(|k| k.db == db && k.tid == tid);
    }

    /// Drop every CSR.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.transient = None;
    }

    /// Number of cached CSRs, the transient one excluded.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// No CSR is cached.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn drop_where(&mut self, stale: impl Fn(&HistoricalCsrKey) -> bool) {
        self.entries.retain(|(k, _)| !stale(k));
        if self.transient.as_ref().is_some_and(|(k, _)| stale(k)) {
            self.transient = None;
        }
    }

    /// The most recently used cached CSR.
    fn newest_entry(&self) -> crate::Result<&CsrIndex> {
        self.entries
            .last()
            .map(|(_, csr)| csr)
            .ok_or_else(missing_after_build)
    }
}

fn missing_after_build() -> crate::Error {
    crate::Error::Internal {
        detail: "historical CSR cache: no CSR after a build".into(),
    }
}

/// Build the CSR of `key`: the collection's edges at the cut, each tagged
/// with the collection, and every node's identity and labels at the cut.
fn build(key: &HistoricalCsrKey, source: &HistoricalCsrSource<'_>) -> crate::Result<CsrIndex> {
    let edges = source.store.scan_collection_as_of(
        key.db.as_u64(),
        key.tid,
        &key.collection,
        None,
        key.cutoff_ordinal,
        key.valid_at_ms,
    )?;
    let labels = source
        .store
        .node_labels_at(key.db, key.tid, key.cutoff_ordinal)?;
    let memory = ScopedMemory::new(
        Arc::clone(source.governor),
        key.db,
        key.tid,
        EngineId::Graph,
    );
    let mut csr = CsrIndex::new(memory);
    for edge in &edges {
        csr.add_node(&edge.src)?;
        csr.add_node(&edge.dst)?;
    }
    for (node, _) in &labels {
        csr.add_node(node)?;
    }
    for edge in &edges {
        csr.insert_record_edge(
            &edge.src,
            &edge.label,
            &edge.dst,
            &key.collection,
            &edge.properties,
        )?;
    }
    csr.compact()?;
    for edge in &edges {
        for (node, surrogate) in [
            (&edge.src, edge.src_surrogate),
            (&edge.dst, edge.dst_surrogate),
        ] {
            if let Some(surrogate) = surrogate {
                csr.set_node_surrogate(node, surrogate);
            }
        }
    }
    for (node, node_labels) in &labels {
        for label in node_labels {
            if !csr.add_node_label(node, label)? {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "historical CSR of '{}' at ordinal {}: node '{node}' label '{label}' \
                         exceeds the distinct node-label limit",
                        key.collection, key.cutoff_ordinal
                    ),
                });
            }
        }
    }
    Ok(csr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::EdgeRef;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const T: TenantId = TenantId::new(1);

    struct Fixture {
        store: EdgeStore,
        governor: Arc<MemoryGovernor>,
        _dir: tempfile::TempDir,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open store");
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, "g", "a", "L", "b"),
                b"",
                100,
                0,
                i64::MAX,
            )
            .expect("put");
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, "g", "a", "L", "c"),
                b"",
                200,
                0,
                i64::MAX,
            )
            .expect("put");
        Fixture {
            store,
            governor: crate::data::executor::core_loop::test_governor(),
            _dir: dir,
        }
    }

    fn key(cutoff_ordinal: i64) -> HistoricalCsrKey {
        HistoricalCsrKey {
            db: DB,
            tid: T,
            collection: "g".into(),
            cutoff_ordinal,
            valid_at_ms: None,
        }
    }

    fn source<'a>(
        f: &'a Fixture,
        applied_ordinal: i64,
        max_entries: usize,
    ) -> HistoricalCsrSource<'a> {
        HistoricalCsrSource {
            store: &f.store,
            governor: &f.governor,
            applied_ordinal,
            max_entries,
        }
    }

    #[test]
    fn a_build_reads_the_edges_at_its_cut() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        let csr = cache
            .get_or_build(key(150), source(&f, 1_000, 4))
            .expect("build");
        assert_eq!(csr.edge_count(), 1);
        let csr = cache
            .get_or_build(key(250), source(&f, 1_000, 4))
            .expect("build");
        assert_eq!(csr.edge_count(), 2);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn the_cache_keeps_at_most_max_entries_least_recent_first_out() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        for cut in [110, 120, 130] {
            cache
                .get_or_build(key(cut), source(&f, 1_000, 2))
                .expect("build");
        }
        assert_eq!(cache.len(), 2);
        assert!(
            cache.get(&key(110)).is_none(),
            "the least recent is evicted"
        );
        // A hit makes 120 the most recent, so 130 goes next.
        cache
            .get_or_build(key(120), source(&f, 1_000, 2))
            .expect("hit");
        cache
            .get_or_build(key(140), source(&f, 1_000, 2))
            .expect("build");
        assert!(cache.get(&key(120)).is_some());
        assert!(cache.get(&key(130)).is_none());
    }

    #[test]
    fn a_cut_above_the_applied_ordinal_is_answered_but_not_cached() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        let csr = cache
            .get_or_build(key(500), source(&f, 300, 4))
            .expect("build");
        assert_eq!(csr.edge_count(), 2);
        assert!(
            cache.is_empty(),
            "a cut above the applied ordinal is not cached"
        );
        assert!(
            cache.get(&key(500)).is_some(),
            "the read that built it can use it"
        );
        cache
            .get_or_build(key(600), source(&f, 300, 4))
            .expect("build");
        assert!(
            cache.get(&key(500)).is_none(),
            "the next transient build replaces it"
        );
    }

    #[test]
    fn a_version_at_or_below_a_cut_drops_that_cut_only() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        for cut in [150, 250] {
            cache
                .get_or_build(key(cut), source(&f, 1_000, 4))
                .expect("build");
        }
        cache.note_version(DB, T, "g", 200);
        assert!(
            cache.get(&key(150)).is_some(),
            "a version above the cut leaves it"
        );
        assert!(
            cache.get(&key(250)).is_none(),
            "a version below the cut drops it"
        );
        cache.note_version(DB, T, "other", 0);
        assert!(
            cache.get(&key(150)).is_some(),
            "another collection leaves it"
        );
    }

    #[test]
    fn invalidation_drops_by_collection_and_by_tenant() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        cache
            .get_or_build(key(150), source(&f, 1_000, 4))
            .expect("build");
        cache.invalidate_collection(DB, T, "g");
        assert!(cache.is_empty());
        cache
            .get_or_build(key(150), source(&f, 1_000, 4))
            .expect("build");
        cache.invalidate_tenant(DB, TenantId::new(2));
        assert_eq!(cache.len(), 1, "another tenant leaves it");
        cache.invalidate_tenant(DB, T);
        assert!(cache.is_empty());
    }

    #[test]
    fn a_node_deleted_after_the_cut_keeps_its_identity_and_labels_there() {
        let f = fixture();
        let bound = EdgeRef::new(DB, T, "g", "x", "L", "y").with_surrogates(
            nodedb_types::Surrogate::new(10),
            nodedb_types::Surrogate::new(20),
        );
        f.store
            .put_edge_versioned(bound, b"", 100, 0, i64::MAX)
            .expect("put");
        f.store
            .put_node_label_version(DB, T, "x", 90, None, &["Person".to_string()])
            .expect("label");
        f.store
            .soft_delete_edge(EdgeRef::new(DB, T, "g", "x", "L", "y"), 300)
            .expect("delete");
        f.store
            .put_node_label_version(DB, T, "x", 300, None, &[])
            .expect("unlabel");
        let mut cache = HistoricalCsrCache::default();
        let csr = cache
            .get_or_build(key(200), source(&f, 1_000, 4))
            .expect("build");
        assert_eq!(
            csr.node_surrogate("x"),
            Some(nodedb_types::Surrogate::new(10))
        );
        let id = csr.node_id_raw("x").expect("x at the cut");
        assert_eq!(csr.node_labels(id), vec!["Person"]);
        let csr = cache
            .get_or_build(key(400), source(&f, 1_000, 4))
            .expect("build");
        assert!(csr.node_id_raw("x").is_none(), "x is gone after the delete");
    }

    #[test]
    fn zero_max_entries_caches_nothing() {
        let f = fixture();
        let mut cache = HistoricalCsrCache::default();
        cache
            .get_or_build(key(150), source(&f, 1_000, 0))
            .expect("build");
        assert!(cache.is_empty());
    }
}

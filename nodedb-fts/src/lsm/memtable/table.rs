// SPDX-License-Identifier: Apache-2.0

//! In-memory memtable for the FTS LSM engine.
//!
//! Accumulates postings per index until a spill threshold is reached.
//! Serves queries from memory. Thread-safe via interior mutability.
//!
//! Every index `(database, tenant, collection, field)` owns its postings,
//! corpus stats, and fieldnorms. Indexes are keyed structurally, so no
//! collection or field name can collide with another's. The spill
//! thresholds bound the sum over every index.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use nodedb_types::Surrogate;

use super::config::MemtableConfig;
use super::scope_state::{Removed, ScopeState};
use crate::block::CompactPosting;
use crate::scope::IndexScope;

/// collection → field key → index state.
type CollectionScopes = HashMap<String, HashMap<String, ScopeState>>;
/// `(database_id, tid)` → that tenant's indexes.
type ScopeMap = HashMap<(u64, u64), CollectionScopes>;

/// One index the memtable holds state for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemtableScope {
    pub database_id: u64,
    pub tid: u64,
    pub collection: String,
    /// Empty for the whole-document index.
    pub field: String,
}

impl MemtableScope {
    /// The index this scope addresses.
    pub fn index(&self) -> IndexScope<'_> {
        IndexScope::from_key(&self.collection, &self.field)
    }
}

/// In-memory accumulator for FTS postings.
///
/// Stores per-index, per-term posting lists keyed on `Surrogate` row
/// identities. Maintains incremental corpus stats per index.
pub struct Memtable {
    scopes: RefCell<ScopeMap>,
    /// Posting entries across every index.
    total_postings: Cell<usize>,
    /// Distinct `(index, term)` pairs across every index.
    total_terms: Cell<usize>,
    /// Spill configuration.
    config: MemtableConfig,
}

impl Memtable {
    pub fn new(config: MemtableConfig) -> Self {
        Self {
            scopes: RefCell::new(HashMap::new()),
            total_postings: Cell::new(0),
            total_terms: Cell::new(0),
            config,
        }
    }

    /// Insert a posting for a term of one index.
    pub fn insert(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
        posting: CompactPosting,
    ) {
        let mut scopes = self.scopes.borrow_mut();
        let state = state_entry(&mut scopes, database_id, tid, index);
        let new_term = state.insert(term, posting);
        self.add(1, usize::from(new_term));
    }

    /// Insert every posting of one document and record its length, in one
    /// index lookup.
    pub fn insert_doc<'t>(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
        doc_len: u32,
        postings: impl IntoIterator<Item = (&'t str, CompactPosting)>,
    ) {
        let mut scopes = self.scopes.borrow_mut();
        let state = state_entry(&mut scopes, database_id, tid, index);
        let (mut added, mut new_terms) = (0usize, 0usize);
        for (term, posting) in postings {
            added += 1;
            new_terms += usize::from(state.insert(term, posting));
        }
        state.record_doc(doc_id, doc_len);
        self.add(added, new_terms);
    }

    /// Record a document's stats in one index (call once per indexed document).
    pub fn record_doc(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        doc_id: Surrogate,
        doc_len: u32,
    ) {
        let mut scopes = self.scopes.borrow_mut();
        state_entry(&mut scopes, database_id, tid, index).record_doc(doc_id, doc_len);
    }

    /// Remove a document's postings from every term of one index.
    pub fn remove_doc(&self, database_id: u64, tid: u64, index: IndexScope<'_>, doc_id: Surrogate) {
        let mut scopes = self.scopes.borrow_mut();
        let Some(fields) = scopes
            .get_mut(&(database_id, tid))
            .and_then(|collections| collections.get_mut(index.collection()))
        else {
            return;
        };
        let Some(state) = fields.get_mut(index.field_key()) else {
            return;
        };
        let removed = state.remove_doc(doc_id);
        if state.is_vacant() {
            fields.remove(index.field_key());
        }
        prune_empty(&mut scopes, database_id, tid, index.collection());
        drop(scopes);
        self.subtract(removed);
    }

    /// Check if the memtable should be flushed (spill threshold reached).
    /// The thresholds bound the sum over every index.
    pub fn should_flush(&self) -> bool {
        self.total_postings.get() >= self.config.max_postings
            || self.total_terms.get() >= self.config.max_terms
    }

    /// Get the posting list for a term of one index. Empty if not found.
    pub fn get_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        term: &str,
    ) -> Vec<CompactPosting> {
        self.read(database_id, tid, index, |s| {
            s.postings.get(term).cloned().unwrap_or_default()
        })
        .unwrap_or_default()
    }

    /// Get all term names of one index.
    pub fn terms(&self, database_id: u64, tid: u64, index: IndexScope<'_>) -> Vec<String> {
        self.read(database_id, tid, index, |s| {
            s.postings.keys().cloned().collect()
        })
        .unwrap_or_default()
    }

    /// Get corpus stats of one index: (doc_count, total_token_sum).
    pub fn stats(&self, database_id: u64, tid: u64, index: IndexScope<'_>) -> (u32, u64) {
        self.read(database_id, tid, index, |s| s.stats)
            .unwrap_or((0, 0))
    }

    /// Get the fieldnorm array (SmallFloat-encoded doc lengths) of one index.
    pub fn fieldnorms(&self, database_id: u64, tid: u64, index: IndexScope<'_>) -> Vec<u8> {
        self.read(database_id, tid, index, |s| s.fieldnorms.clone())
            .unwrap_or_default()
    }

    /// Run `f` over one index's term→postings map without removing it.
    /// `None` when the memtable holds no state for the index.
    pub fn with_scope_postings<R>(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        f: impl FnOnce(&HashMap<String, Vec<CompactPosting>>) -> R,
    ) -> Option<R> {
        self.read(database_id, tid, index, |s| f(&s.postings))
    }

    /// Every index the memtable holds state for.
    pub fn scopes(&self) -> Vec<MemtableScope> {
        let scopes = self.scopes.borrow();
        let mut out = Vec::new();
        for (&(database_id, tid), collections) in scopes.iter() {
            for (collection, fields) in collections {
                for field in fields.keys() {
                    out.push(MemtableScope {
                        database_id,
                        tid,
                        collection: collection.clone(),
                        field: field.clone(),
                    });
                }
            }
        }
        out
    }

    /// Drain one index's postings (for flush). Returns its term→postings
    /// map and drops its state, including stats and fieldnorms. Every
    /// other index is untouched.
    pub fn drain_scope(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> HashMap<String, Vec<CompactPosting>> {
        let mut scopes = self.scopes.borrow_mut();
        let Some(state) = scopes
            .get_mut(&(database_id, tid))
            .and_then(|collections| collections.get_mut(index.collection()))
            .and_then(|fields| fields.remove(index.field_key()))
        else {
            return HashMap::new();
        };
        prune_empty(&mut scopes, database_id, tid, index.collection());
        drop(scopes);
        self.subtract(state.footprint());
        state.postings
    }

    /// Drop every index of one collection.
    pub fn drain_collection(&self, database_id: u64, tid: u64, collection: &str) {
        let mut scopes = self.scopes.borrow_mut();
        let Some(fields) = scopes
            .get_mut(&(database_id, tid))
            .and_then(|collections| collections.remove(collection))
        else {
            return;
        };
        prune_empty(&mut scopes, database_id, tid, collection);
        drop(scopes);
        for state in fields.values() {
            self.subtract(state.footprint());
        }
    }

    /// Drop every index of one `(database_id, tid)`.
    pub fn drain_tenant(&self, database_id: u64, tid: u64) {
        let Some(collections) = self.scopes.borrow_mut().remove(&(database_id, tid)) else {
            return;
        };
        for state in collections.values().flat_map(HashMap::values) {
            self.subtract(state.footprint());
        }
    }

    /// Distinct `(index, term)` pairs across every index.
    pub fn term_count(&self) -> usize {
        self.total_terms.get()
    }

    /// Posting entries across every index.
    pub fn posting_count(&self) -> usize {
        self.total_postings.get()
    }

    /// Whether no index holds a posting.
    pub fn is_empty(&self) -> bool {
        self.total_terms.get() == 0
    }

    fn read<R>(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        f: impl FnOnce(&ScopeState) -> R,
    ) -> Option<R> {
        self.scopes
            .borrow()
            .get(&(database_id, tid))
            .and_then(|collections| collections.get(index.collection()))
            .and_then(|fields| fields.get(index.field_key()))
            .map(f)
    }

    fn add(&self, postings: usize, terms: usize) {
        self.total_postings
            .set(self.total_postings.get() + postings);
        self.total_terms.set(self.total_terms.get() + terms);
    }

    fn subtract(&self, removed: Removed) {
        self.total_postings
            .set(self.total_postings.get().saturating_sub(removed.postings));
        self.total_terms
            .set(self.total_terms.get().saturating_sub(removed.terms));
    }
}

/// The state of one index, created empty on first use.
fn state_entry<'m>(
    scopes: &'m mut ScopeMap,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> &'m mut ScopeState {
    scopes
        .entry((database_id, tid))
        .or_default()
        .entry(index.collection().to_string())
        .or_default()
        .entry(index.field_key().to_string())
        .or_default()
}

/// Drop the collection and tenant maps that no longer hold any index.
fn prune_empty(scopes: &mut ScopeMap, database_id: u64, tid: u64, collection: &str) {
    let Some(collections) = scopes.get_mut(&(database_id, tid)) else {
        return;
    };
    if collections.get(collection).is_some_and(HashMap::is_empty) {
        collections.remove(collection);
    }
    if collections.is_empty() {
        scopes.remove(&(database_id, tid));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::smallfloat;

    const DB: u64 = 0;
    const T: u64 = 1;
    const S: IndexScope<'static> = IndexScope::document("docs");

    fn make_posting(doc_id: u32, tf: u32) -> CompactPosting {
        CompactPosting {
            doc_id: Surrogate(doc_id),
            term_freq: tf,
            fieldnorm: smallfloat::encode(100),
            positions: vec![0],
        }
    }

    #[test]
    fn anchored_posting_lists_release_sparse_migration_capacity() {
        let mt = Memtable::new(MemtableConfig::default());
        let terms = 12u32;
        let movers = 64u32;
        for term in 0..terms {
            mt.insert(DB, T, S, &format!("term{term}"), make_posting(term + 1, 1));
            mt.record_doc(DB, T, S, Surrogate(term + 1), 1);
        }
        for term in 0..terms {
            let key = format!("term{term}");
            for mover in 0..movers {
                let id = terms + mover + 1;
                mt.insert(DB, T, S, &key, make_posting(id, 1));
                mt.record_doc(DB, T, S, Surrogate(id), 1);
            }
            assert_eq!(mt.posting_count(), (terms + movers) as usize);
            assert_eq!(
                mt.stats(DB, T, S),
                (terms + movers, (terms + movers) as u64)
            );
            for mover in 0..movers {
                mt.remove_doc(DB, T, S, Surrogate(terms + mover + 1));
                let capacities_ok = mt
                    .read(DB, T, S, |s| {
                        s.postings.values().all(|postings| {
                            postings.capacity() <= postings.len().saturating_mul(2).max(4)
                        })
                    })
                    .unwrap_or(true);
                assert!(capacities_ok);
            }
            let anchor = mt.get_postings(DB, T, S, &key);
            assert_eq!(anchor.len(), 1);
            assert_eq!(anchor[0].doc_id, Surrogate(term + 1));
            assert_eq!(anchor[0].positions, vec![0]);
            assert_eq!(mt.posting_count(), terms as usize);
            assert_eq!(mt.stats(DB, T, S), (terms, terms as u64));
        }
        for term in 0..terms {
            assert_eq!(
                mt.get_postings(DB, T, S, &format!("term{term}"))[0].doc_id,
                Surrogate(term + 1)
            );
        }
    }

    #[test]
    fn insert_and_query() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(DB, T, S, "hello", make_posting(0, 1));
        mt.insert(DB, T, S, "hello", make_posting(1, 2));
        mt.insert(DB, T, S, "world", make_posting(0, 1));

        assert_eq!(mt.get_postings(DB, T, S, "hello").len(), 2);
        assert_eq!(mt.get_postings(DB, T, S, "world").len(), 1);
        assert!(mt.get_postings(DB, T, S, "missing").is_empty());
        assert_eq!(mt.term_count(), 2);
        assert_eq!(mt.posting_count(), 3);
    }

    #[test]
    fn remove_doc() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(DB, T, S, "hello", make_posting(0, 1));
        mt.insert(DB, T, S, "hello", make_posting(1, 2));
        mt.record_doc(DB, T, S, Surrogate(0), 100);
        mt.record_doc(DB, T, S, Surrogate(1), 50);

        mt.remove_doc(DB, T, S, Surrogate(0));

        assert_eq!(mt.get_postings(DB, T, S, "hello").len(), 1);
        assert_eq!(mt.get_postings(DB, T, S, "hello")[0].doc_id, Surrogate(1));
        assert_eq!(mt.stats(DB, T, S).0, 1); // Only doc 1 remains.
    }

    #[test]
    fn remove_doc_touches_only_its_index() {
        let mt = Memtable::new(MemtableConfig::default());
        let title = IndexScope::field("docs", "title").unwrap();
        mt.insert_doc(DB, T, S, Surrogate(1), 2, [("rust", make_posting(1, 1))]);
        mt.insert_doc(
            DB,
            T,
            title,
            Surrogate(1),
            1,
            [("rust", make_posting(1, 1))],
        );

        mt.remove_doc(DB, T, title, Surrogate(1));

        assert!(mt.get_postings(DB, T, title, "rust").is_empty());
        assert_eq!(mt.stats(DB, T, title), (0, 0));
        assert_eq!(mt.get_postings(DB, T, S, "rust").len(), 1);
        assert_eq!(mt.stats(DB, T, S), (1, 2));
        assert_eq!(mt.posting_count(), 1);
        assert_eq!(mt.term_count(), 1);
    }

    #[test]
    fn drain_scope_resets_only_that_index() {
        let mt = Memtable::new(MemtableConfig::default());
        let other = IndexScope::document("other");
        mt.insert_doc(
            DB,
            T,
            S,
            Surrogate(0),
            100,
            [("hello", make_posting(0, 1)), ("world", make_posting(0, 1))],
        );
        mt.insert_doc(
            DB,
            T,
            other,
            Surrogate(1),
            50,
            [("rust", make_posting(1, 1))],
        );

        let drained = mt.drain_scope(DB, T, S);
        assert_eq!(drained.len(), 2);
        assert_eq!(mt.stats(DB, T, S), (0, 0));
        assert!(mt.fieldnorms(DB, T, S).is_empty());
        assert!(mt.terms(DB, T, S).is_empty());

        assert_eq!(mt.stats(DB, T, other), (1, 50));
        assert_eq!(mt.get_postings(DB, T, other, "rust").len(), 1);
        assert_eq!(mt.posting_count(), 1);
        assert_eq!(mt.term_count(), 1);
        assert!(!mt.is_empty());

        mt.drain_scope(DB, T, other);
        assert!(mt.is_empty());
        assert_eq!(mt.posting_count(), 0);
        assert!(mt.scopes().is_empty());
    }

    #[test]
    fn drain_collection_selective() {
        let mt = Memtable::new(MemtableConfig::default());
        let a = IndexScope::document("col_a");
        let a_title = IndexScope::field("col_a", "title").unwrap();
        let b = IndexScope::document("col_b");
        mt.insert(DB, T, a, "hello", make_posting(0, 1));
        mt.insert(DB, T, a_title, "world", make_posting(1, 1));
        mt.insert(DB, T, b, "rust", make_posting(2, 1));
        mt.record_doc(DB, T, b, Surrogate(2), 1);

        mt.drain_collection(DB, T, "col_a");

        assert!(mt.get_postings(DB, T, a, "hello").is_empty());
        assert!(mt.get_postings(DB, T, a_title, "world").is_empty());
        assert_eq!(mt.get_postings(DB, T, b, "rust").len(), 1);
        assert_eq!(mt.stats(DB, T, b), (1, 1));
        assert_eq!(mt.posting_count(), 1);
    }

    #[test]
    fn drain_tenant_keeps_other_tenants() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(DB, 1, S, "hello", make_posting(0, 1));
        mt.insert(DB, 2, S, "hello", make_posting(0, 1));

        mt.drain_tenant(DB, 1);

        assert!(mt.get_postings(DB, 1, S, "hello").is_empty());
        assert_eq!(mt.get_postings(DB, 2, S, "hello").len(), 1);
        assert_eq!(mt.posting_count(), 1);
    }

    /// Collection `a:b` with field `c` and collection `a` with field `b:c`
    /// are distinct indexes: no separator in a name can merge them.
    #[test]
    fn names_with_separators_never_collide() {
        let mt = Memtable::new(MemtableConfig::default());
        let left = IndexScope::field("a:b", "c").unwrap();
        let right = IndexScope::field("a", "b:c").unwrap();
        mt.insert(DB, T, left, "term", make_posting(1, 1));

        assert_eq!(mt.get_postings(DB, T, left, "term").len(), 1);
        assert!(mt.get_postings(DB, T, right, "term").is_empty());
        assert_eq!(mt.scopes().len(), 1);
    }

    #[test]
    fn spill_threshold_sums_every_index() {
        let config = MemtableConfig {
            max_postings: 5,
            max_terms: 100,
        };
        let mt = Memtable::new(config);
        let title = IndexScope::field("docs", "title").unwrap();
        for i in 0..2 {
            mt.insert(DB, T, S, "term", make_posting(i, 1));
        }
        for i in 0..2 {
            mt.insert(DB, T, title, "term", make_posting(i, 1));
        }
        assert!(!mt.should_flush());
        mt.insert(DB, T, title, "term", make_posting(4, 1));
        assert!(mt.should_flush());
    }

    #[test]
    fn stats_invariant_under_insert_delete_churn() {
        // Spec: after any sequence of (record_doc, remove_doc) calls, the
        // memtable's corpus stats — specifically `total_token_sum` that feeds
        // BM25 `avg_doc_len` — must equal the stats of a memtable freshly
        // populated with only the currently-live documents.
        let churn = Memtable::new(MemtableConfig::default());
        let doc_id = Surrogate(0);
        let doc_len = 137u32; // not smallfloat-representable exactly
        for _ in 0..500 {
            churn.record_doc(DB, T, S, doc_id, doc_len);
            churn.remove_doc(DB, T, S, doc_id);
        }
        // After equal numbers of record / remove, the memtable should be empty
        // from a stats standpoint: zero docs, zero tokens.
        let fresh = Memtable::new(MemtableConfig::default());
        assert_eq!(
            churn.stats(DB, T, S),
            fresh.stats(DB, T, S),
            "stats drifted after insert/delete churn"
        );
    }

    #[test]
    fn stats_invariant_churn_then_single_live_doc() {
        // Spec: churn on one doc, then leave a single live doc. Stats must
        // exactly match a memtable that only ever saw that single live doc.
        let churn = Memtable::new(MemtableConfig::default());
        for _ in 0..200 {
            churn.record_doc(DB, T, S, Surrogate(0), 400);
            churn.remove_doc(DB, T, S, Surrogate(0));
        }
        churn.record_doc(DB, T, S, Surrogate(0), 250);

        let fresh = Memtable::new(MemtableConfig::default());
        fresh.record_doc(DB, T, S, Surrogate(0), 250);

        assert_eq!(
            churn.stats(DB, T, S),
            fresh.stats(DB, T, S),
            "stats diverged from fresh baseline"
        );
    }

    #[test]
    fn stats_total_token_sum_never_exceeds_live_doc_length() {
        // Regression guard on the specific failure mode: `total_token_sum`
        // growing beyond the sum of live document lengths. If this fails,
        // BM25 `avg_doc_len` is inflated and ranking is silently skewed.
        let mt = Memtable::new(MemtableConfig::default());
        for _ in 0..100 {
            mt.record_doc(DB, T, S, Surrogate(0), 777);
            mt.remove_doc(DB, T, S, Surrogate(0));
        }
        mt.record_doc(DB, T, S, Surrogate(0), 777);
        let (count, total) = mt.stats(DB, T, S);
        assert_eq!(count, 1);
        assert_eq!(
            total, 777,
            "total_token_sum drifted: got {total}, expected 777 for a single 777-token live doc"
        );
    }

    #[test]
    fn stats_are_per_index() {
        let mt = Memtable::new(MemtableConfig::default());
        let title = IndexScope::field("docs", "title").unwrap();
        mt.record_doc(DB, T, S, Surrogate(1), 10);
        mt.record_doc(DB, T, S, Surrogate(2), 20);
        mt.record_doc(DB, T, title, Surrogate(1), 3);

        assert_eq!(mt.stats(DB, T, S), (2, 30));
        assert_eq!(mt.stats(DB, T, title), (1, 3));
    }

    #[test]
    fn fieldnorms_recorded() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.record_doc(DB, T, S, Surrogate(0), 100);
        mt.record_doc(DB, T, S, Surrogate(5), 50);

        let norms = mt.fieldnorms(DB, T, S);
        assert_eq!(norms.len(), 6); // 0..=5
        assert_eq!(
            smallfloat::decode(norms[0]),
            smallfloat::decode(smallfloat::encode(100))
        );
        assert_eq!(
            smallfloat::decode(norms[5]),
            smallfloat::decode(smallfloat::encode(50))
        );
    }
}

// SPDX-License-Identifier: Apache-2.0

//! In-memory memtable for the FTS LSM engine.
//!
//! Accumulates postings in a HashMap until a spill threshold is reached.
//! Serves queries from memory. Thread-safe via interior mutability.

use std::cell::RefCell;
use std::collections::HashMap;

use nodedb_types::Surrogate;

use crate::block::CompactPosting;
use crate::codec::smallfloat;

/// Spill threshold: flush memtable when total posting entries exceed this.
pub const DEFAULT_SPILL_POSTINGS: usize = 32 * 1024 * 1024; // 32M entries

/// Spill threshold: flush when unique terms exceed this.
pub const DEFAULT_SPILL_TERMS: usize = 100_000;

/// Configuration for memtable spill thresholds.
#[derive(Debug, Clone, Copy)]
pub struct MemtableConfig {
    pub max_postings: usize,
    pub max_terms: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_postings: DEFAULT_SPILL_POSTINGS,
            max_terms: DEFAULT_SPILL_TERMS,
        }
    }
}

/// In-memory accumulator for FTS postings.
///
/// Stores per-term posting lists keyed on `Surrogate` row identities.
/// Maintains incremental corpus stats.
pub struct Memtable {
    /// term → sorted list of compact postings.
    postings: RefCell<HashMap<String, Vec<CompactPosting>>>,
    /// Total number of posting entries across all terms.
    total_postings: RefCell<usize>,
    /// Incremental stats: (doc_count, total_token_sum).
    stats: RefCell<(u32, u64)>,
    /// Fieldnorm array: surrogate.0 → SmallFloat-encoded length (used by BM25).
    fieldnorms: RefCell<Vec<u8>>,
    /// Exact-length sidecar: surrogate.0 → original u32 token count. Used to
    /// decrement `stats.total_token_sum` symmetrically with the exact
    /// increment in `record_doc`. Storing the original length here keeps
    /// the smallfloat array lossy (space-efficient for ranking) while
    /// making corpus-stat accounting exact.
    fieldnorms_exact: RefCell<Vec<u32>>,
    /// Spill configuration.
    config: MemtableConfig,
}

impl Memtable {
    pub fn new(config: MemtableConfig) -> Self {
        Self {
            postings: RefCell::new(HashMap::new()),
            total_postings: RefCell::new(0),
            stats: RefCell::new((0, 0)),
            fieldnorms: RefCell::new(Vec::new()),
            fieldnorms_exact: RefCell::new(Vec::new()),
            config,
        }
    }

    /// Insert a posting for a term. Surrogate is supplied by the caller.
    pub fn insert(&self, term: &str, posting: CompactPosting) {
        let mut map = self.postings.borrow_mut();
        map.entry(term.to_string()).or_default().push(posting);
        *self.total_postings.borrow_mut() += 1;
    }

    /// Record a document's stats (call once per indexed document).
    pub fn record_doc(&self, doc_id: Surrogate, doc_len: u32) {
        let mut stats = self.stats.borrow_mut();
        stats.0 += 1;
        stats.1 += doc_len as u64;

        let idx = doc_id.0 as usize;
        {
            let mut norms = self.fieldnorms.borrow_mut();
            if idx >= norms.len() {
                norms.resize(idx + 1, 0);
            }
            norms[idx] = smallfloat::encode(doc_len);
        }
        let mut exact = self.fieldnorms_exact.borrow_mut();
        if idx >= exact.len() {
            exact.resize(idx + 1, 0);
        }
        exact[idx] = doc_len;
    }

    /// Remove a document's postings from all terms.
    pub fn remove_doc(&self, doc_id: Surrogate) {
        let mut map = self.postings.borrow_mut();
        let mut removed = 0usize;
        map.retain(|_, postings| {
            let before = postings.len();
            postings.retain(|p| p.doc_id != doc_id);
            removed += before - postings.len();
            if postings.len() < before
                && !postings.is_empty()
                && postings.capacity() > postings.len().saturating_mul(2).max(4)
            {
                postings.shrink_to(postings.len());
            }
            !postings.is_empty()
        });
        *self.total_postings.borrow_mut() -= removed;

        // Decrement stats using the exact-length sidecar so the subtraction
        // matches the exact-length increment in record_doc. Using the
        // smallfloat-decoded length here would drift upward on churn.
        let mut exact = self.fieldnorms_exact.borrow_mut();
        if let Some(slot) = exact.get_mut(doc_id.0 as usize)
            && *slot > 0
        {
            let doc_len = *slot;
            *slot = 0;
            let mut stats = self.stats.borrow_mut();
            stats.0 = stats.0.saturating_sub(1);
            stats.1 = stats.1.saturating_sub(doc_len as u64);
        }
    }

    /// Check if the memtable should be flushed (spill threshold reached).
    pub fn should_flush(&self) -> bool {
        let tp = *self.total_postings.borrow();
        let terms = self.postings.borrow().len();
        tp >= self.config.max_postings || terms >= self.config.max_terms
    }

    /// Get the posting list for a term. Returns empty vec if not found.
    pub fn get_postings(&self, term: &str) -> Vec<CompactPosting> {
        self.postings
            .borrow()
            .get(term)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all term names in the memtable.
    pub fn terms(&self) -> Vec<String> {
        self.postings.borrow().keys().cloned().collect()
    }

    /// Get corpus stats: (doc_count, total_token_sum).
    pub fn stats(&self) -> (u32, u64) {
        *self.stats.borrow()
    }

    /// Get the fieldnorm array (SmallFloat-encoded doc lengths).
    pub fn fieldnorms(&self) -> Vec<u8> {
        self.fieldnorms.borrow().clone()
    }

    /// Drain all postings from the memtable (for flush).
    /// Returns the term→postings map and resets the memtable to empty,
    /// including stats and fieldnorms.
    pub fn drain(&self) -> HashMap<String, Vec<CompactPosting>> {
        let mut map = self.postings.borrow_mut();
        *self.total_postings.borrow_mut() = 0;
        *self.stats.borrow_mut() = (0, 0);
        self.fieldnorms.borrow_mut().clear();
        self.fieldnorms_exact.borrow_mut().clear();
        std::mem::take(&mut *map)
    }

    /// Drain only postings matching a key prefix.
    /// The caller is responsible for providing the full prefix (including
    /// any trailing separator). Resets stats/fieldnorms.
    pub fn drain_collection(&self, prefix: &str) {
        let mut map = self.postings.borrow_mut();
        let mut removed = 0usize;
        map.retain(|k, v| {
            if k.starts_with(prefix) {
                removed += v.len();
                false
            } else {
                true
            }
        });
        *self.total_postings.borrow_mut() -= removed;
        // Stats and fieldnorms are collection-scoped in the backend,
        // but the memtable tracks them globally. Reset to be safe.
        *self.stats.borrow_mut() = (0, 0);
        self.fieldnorms.borrow_mut().clear();
        self.fieldnorms_exact.borrow_mut().clear();
    }

    /// Number of unique terms.
    pub fn term_count(&self) -> usize {
        self.postings.borrow().len()
    }

    /// Total posting entries.
    pub fn posting_count(&self) -> usize {
        *self.total_postings.borrow()
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.postings.borrow().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            mt.insert(&format!("term{term}"), make_posting(term + 1, 1));
            mt.record_doc(Surrogate(term + 1), 1);
        }
        for term in 0..terms {
            let key = format!("term{term}");
            for mover in 0..movers {
                let id = terms + mover + 1;
                mt.insert(&key, make_posting(id, 1));
                mt.record_doc(Surrogate(id), 1);
            }
            assert_eq!(mt.posting_count(), (terms + movers) as usize);
            assert_eq!(mt.stats(), (terms + movers, (terms + movers) as u64));
            for mover in 0..movers {
                mt.remove_doc(Surrogate(terms + mover + 1));
                let map = mt.postings.borrow();
                for postings in map.values() {
                    assert!(postings.capacity() <= postings.len().saturating_mul(2).max(4));
                }
            }
            let anchor = mt.get_postings(&key);
            assert_eq!(anchor.len(), 1);
            assert_eq!(anchor[0].doc_id, Surrogate(term + 1));
            assert_eq!(anchor[0].positions, vec![0]);
            assert_eq!(mt.posting_count(), terms as usize);
            assert_eq!(mt.stats(), (terms, terms as u64));
        }
        for term in 0..terms {
            assert_eq!(
                mt.get_postings(&format!("term{term}"))[0].doc_id,
                Surrogate(term + 1)
            );
        }
    }

    #[test]
    fn insert_and_query() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("hello", make_posting(1, 2));
        mt.insert("world", make_posting(0, 1));

        assert_eq!(mt.get_postings("hello").len(), 2);
        assert_eq!(mt.get_postings("world").len(), 1);
        assert!(mt.get_postings("missing").is_empty());
        assert_eq!(mt.term_count(), 2);
        assert_eq!(mt.posting_count(), 3);
    }

    #[test]
    fn remove_doc() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("hello", make_posting(1, 2));
        mt.record_doc(Surrogate(0), 100);
        mt.record_doc(Surrogate(1), 50);

        mt.remove_doc(Surrogate(0));

        assert_eq!(mt.get_postings("hello").len(), 1);
        assert_eq!(mt.get_postings("hello")[0].doc_id, Surrogate(1));
        assert_eq!(mt.stats().0, 1); // Only doc 1 remains.
    }

    #[test]
    fn drain_resets_everything() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("hello", make_posting(0, 1));
        mt.insert("world", make_posting(1, 1));
        mt.record_doc(Surrogate(0), 100);
        mt.record_doc(Surrogate(1), 50);

        let drained = mt.drain();
        assert_eq!(drained.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.posting_count(), 0);
        assert_eq!(mt.stats(), (0, 0));
        assert!(mt.fieldnorms().is_empty());
    }

    #[test]
    fn drain_collection_selective() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("col_a:hello", make_posting(0, 1));
        mt.insert("col_a:world", make_posting(1, 1));
        mt.insert("col_b:rust", make_posting(2, 1));

        mt.drain_collection("col_a:");

        assert!(mt.get_postings("col_a:hello").is_empty());
        assert!(mt.get_postings("col_a:world").is_empty());
        assert_eq!(mt.get_postings("col_b:rust").len(), 1);
        assert_eq!(mt.posting_count(), 1);
    }

    #[test]
    fn spill_threshold() {
        let config = MemtableConfig {
            max_postings: 5,
            max_terms: 100,
        };
        let mt = Memtable::new(config);
        for i in 0..4 {
            mt.insert("term", make_posting(i, 1));
        }
        assert!(!mt.should_flush());
        mt.insert("term", make_posting(4, 1));
        assert!(mt.should_flush());
    }

    #[test]
    fn stats_invariant_under_insert_delete_churn() {
        // Spec: after any sequence of (record_doc, remove_doc) calls, the
        // memtable's corpus stats — specifically `total_token_sum` that feeds
        // BM25 `avg_doc_len` — must equal the stats of a memtable freshly
        // populated with only the currently-live documents.
        //
        // Today `remove_doc` subtracts `smallfloat::decode(encoded_len)` from
        // the exact-length sum. Because `decode(encode(L)) <= L`, every churn
        // cycle leaves positive residue in `total_token_sum`. After N cycles
        // of the same doc, the residue compounds and silently skews BM25.
        let churn = Memtable::new(MemtableConfig::default());
        let doc_id = Surrogate(0);
        let doc_len = 137u32; // not smallfloat-representable exactly
        for _ in 0..500 {
            churn.record_doc(doc_id, doc_len);
            churn.remove_doc(doc_id);
        }
        // After equal numbers of record / remove, the memtable should be empty
        // from a stats standpoint: zero docs, zero tokens.
        let fresh = Memtable::new(MemtableConfig::default());
        assert_eq!(
            churn.stats(),
            fresh.stats(),
            "stats drifted after insert/delete churn: churn={:?} fresh={:?}",
            churn.stats(),
            fresh.stats()
        );
    }

    #[test]
    fn stats_invariant_churn_then_single_live_doc() {
        // Spec: churn on one doc, then leave a single live doc. Stats must
        // exactly match a memtable that only ever saw that single live doc.
        let churn = Memtable::new(MemtableConfig::default());
        for _ in 0..200 {
            churn.record_doc(Surrogate(0), 400);
            churn.remove_doc(Surrogate(0));
        }
        churn.record_doc(Surrogate(0), 250);

        let fresh = Memtable::new(MemtableConfig::default());
        fresh.record_doc(Surrogate(0), 250);

        assert_eq!(
            churn.stats(),
            fresh.stats(),
            "stats diverged from fresh baseline: churn={:?} fresh={:?}",
            churn.stats(),
            fresh.stats()
        );
    }

    #[test]
    fn stats_total_token_sum_never_exceeds_live_doc_length() {
        // Regression guard on the specific failure mode: `total_token_sum`
        // growing beyond the sum of live document lengths. If this fails,
        // BM25 `avg_doc_len` is inflated and ranking is silently skewed.
        let mt = Memtable::new(MemtableConfig::default());
        for _ in 0..100 {
            mt.record_doc(Surrogate(0), 777);
            mt.remove_doc(Surrogate(0));
        }
        mt.record_doc(Surrogate(0), 777);
        let (count, total) = mt.stats();
        assert_eq!(count, 1);
        assert_eq!(
            total, 777,
            "total_token_sum drifted: got {total}, expected 777 for a single 777-token live doc"
        );
    }

    #[test]
    fn fieldnorms_recorded() {
        let mt = Memtable::new(MemtableConfig::default());
        mt.record_doc(Surrogate(0), 100);
        mt.record_doc(Surrogate(5), 50);

        let norms = mt.fieldnorms();
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

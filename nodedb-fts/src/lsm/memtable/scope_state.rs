// SPDX-License-Identifier: Apache-2.0

//! Memtable state of one index: its postings and its corpus stats.

use std::collections::HashMap;

use nodedb_types::Surrogate;

use crate::block::CompactPosting;
use crate::codec::smallfloat;

/// Postings, stats, and fieldnorms of one `(database, tenant, index)`.
#[derive(Default)]
pub(crate) struct ScopeState {
    /// term → list of compact postings.
    pub(super) postings: HashMap<String, Vec<CompactPosting>>,
    /// Posting entries across every term of this index.
    pub(super) total_postings: usize,
    /// Incremental stats: (doc_count, total_token_sum).
    pub(super) stats: (u32, u64),
    /// Fieldnorm array: surrogate.0 → SmallFloat-encoded length (used by BM25).
    pub(super) fieldnorms: Vec<u8>,
    /// Exact-length sidecar: surrogate.0 → original u32 token count. Used to
    /// decrement `stats.total_token_sum` symmetrically with the exact
    /// increment in `record_doc`. Storing the original length here keeps
    /// the smallfloat array lossy (space-efficient for ranking) while
    /// making corpus-stat accounting exact.
    pub(super) fieldnorms_exact: Vec<u32>,
}

/// Postings and terms a mutation removed from one index.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Removed {
    pub(crate) postings: usize,
    pub(crate) terms: usize,
}

impl ScopeState {
    /// Insert a posting for a term. Returns whether the term is new here.
    pub(super) fn insert(&mut self, term: &str, posting: CompactPosting) -> bool {
        self.total_postings += 1;
        match self.postings.get_mut(term) {
            Some(list) => {
                list.push(posting);
                false
            }
            None => {
                self.postings.insert(term.to_string(), vec![posting]);
                true
            }
        }
    }

    /// Record a document's stats (call once per indexed document).
    pub(super) fn record_doc(&mut self, doc_id: Surrogate, doc_len: u32) {
        self.stats.0 += 1;
        self.stats.1 += doc_len as u64;

        let idx = doc_id.0 as usize;
        if idx >= self.fieldnorms.len() {
            self.fieldnorms.resize(idx + 1, 0);
        }
        self.fieldnorms[idx] = smallfloat::encode(doc_len);
        if idx >= self.fieldnorms_exact.len() {
            self.fieldnorms_exact.resize(idx + 1, 0);
        }
        self.fieldnorms_exact[idx] = doc_len;
    }

    /// Remove a document's postings from every term of this index.
    pub(super) fn remove_doc(&mut self, doc_id: Surrogate) -> Removed {
        let mut removed = Removed::default();
        self.postings.retain(|_, postings| {
            let before = postings.len();
            postings.retain(|p| p.doc_id != doc_id);
            removed.postings += before - postings.len();
            if postings.len() < before
                && !postings.is_empty()
                && postings.capacity() > postings.len().saturating_mul(2).max(4)
            {
                postings.shrink_to(postings.len());
            }
            let keep = !postings.is_empty();
            if !keep {
                removed.terms += 1;
            }
            keep
        });
        self.total_postings -= removed.postings;

        // Decrement stats using the exact-length sidecar so the subtraction
        // matches the exact-length increment in record_doc. Using the
        // smallfloat-decoded length here would drift upward on churn.
        if let Some(slot) = self.fieldnorms_exact.get_mut(doc_id.0 as usize)
            && *slot > 0
        {
            let doc_len = *slot;
            *slot = 0;
            self.stats.0 = self.stats.0.saturating_sub(1);
            self.stats.1 = self.stats.1.saturating_sub(doc_len as u64);
        }
        removed
    }

    /// Whether the index holds neither postings nor counted documents.
    pub(super) fn is_vacant(&self) -> bool {
        self.postings.is_empty() && self.stats.0 == 0
    }

    /// What dropping this whole state removes.
    pub(super) fn footprint(&self) -> Removed {
        Removed {
            postings: self.total_postings,
            terms: self.postings.len(),
        }
    }
}

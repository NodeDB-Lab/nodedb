// SPDX-License-Identifier: Apache-2.0

//! A query string resolved against one index: its positive terms grouped by
//! query word, each with the posting blocks it scores against, and the
//! documents its negative terms exclude.
//!
//! Every search path resolves a query here, once, so the analyzer, synonym
//! expansion, fuzzy replacement, and negation are identical for indexed and
//! staged documents.

use std::collections::HashSet;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::codec::smallfloat;
use crate::index::FtsIndex;
use crate::index::error::FtsIndexError;
use crate::lsm::query as lsm_query;
use crate::posting::{Posting, QueryMode};
use crate::scope::IndexScope;
use crate::search::bmw::skip_index::TermBlocks;
use crate::search::query_parser::parse_query;
use crate::search::staged::StagedView;

/// The query inputs every search path resolves the same way.
#[derive(Debug, Clone, Copy)]
pub struct TextQuery<'a> {
    /// Raw query string (may contain `NOT <term>` / `-<term>` negation).
    pub query: &'a str,
    /// When `true`, a term with no posting falls back to fuzzy lookup. A
    /// collection configured with `FUZZY true` falls back either way.
    pub fuzzy_enabled: bool,
    /// Boolean combination of the query words.
    pub mode: QueryMode,
}

/// A query resolved against one index. `texts`, `blocks`, and `term_groups`
/// are parallel: one entry per positive query term.
pub(crate) struct ResolvedQuery {
    /// Each term looked up: the analyzed token, or its fuzzy replacement.
    pub(crate) texts: Vec<String>,
    /// Posting blocks of each term.
    pub(crate) blocks: Vec<TermBlocks>,
    /// Query word of each term.
    pub(crate) term_groups: Vec<usize>,
    /// Whether any term is a fuzzy replacement.
    pub(crate) fuzzy: bool,
    /// Number of query words.
    pub(crate) groups: usize,
    /// AND mode over more than one word: a match holds every word.
    pub(crate) require_all: bool,
    /// Analyzed, synonym-expanded negative terms.
    pub(crate) negative_terms: HashSet<String>,
    /// Indexed documents holding a negative term.
    pub(crate) negated: SurrogateBitmap,
    pub(crate) total_docs: u32,
    pub(crate) avg_doc_len: f32,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Resolve `query` against `index`. `None` when the query has no
    /// positive term after analysis, so it matches nothing.
    pub(crate) fn resolve_query(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        query: TextQuery<'_>,
        staged: Option<&StagedView>,
    ) -> Result<Option<ResolvedQuery>, FtsIndexError<B::Error>> {
        let collection = index.collection();
        let fuzzy_enabled = query.fuzzy_enabled
            || self
                .get_collection_fuzzy(database_id, tid, collection)
                .map_err(FtsIndexError::backend)?;
        let parsed = parse_query(query.query)?;
        let positive_raw = parsed.positive.join(" ");
        let base_tokens = self
            .analyze_for_collection(database_id, tid, collection, &positive_raw)
            .map_err(FtsIndexError::backend)?;
        if base_tokens.is_empty() {
            return Ok(None);
        }
        let raw_tokens = if fuzzy_enabled {
            self.tokenize_raw_for_collection(database_id, tid, collection, &positive_raw)
                .map_err(FtsIndexError::backend)?
        } else {
            Vec::new()
        };
        let words = self
            .expand_query_groups(database_id, tid, base_tokens)
            .map_err(FtsIndexError::backend)?;
        let groups = words.len();

        let mut texts: Vec<String> = Vec::new();
        let mut term_groups: Vec<usize> = Vec::new();
        let mut is_word: Vec<bool> = Vec::new();
        for (group, word) in words.into_iter().enumerate() {
            for (pos, term) in word.into_iter().enumerate() {
                texts.push(term);
                term_groups.push(group);
                is_word.push(pos == 0);
            }
        }

        let mut blocks = self.term_blocks(database_id, tid, index, &texts)?;
        let mut fuzzy = false;
        let positions = is_word.iter().zip(&term_groups);
        for ((text, term_blocks), (word, group)) in
            texts.iter_mut().zip(blocks.iter_mut()).zip(positions)
        {
            if !fuzzy_enabled || term_is_live(text, term_blocks, staged) {
                continue;
            }
            let raw = if *word {
                raw_tokens.get(*group).map_or(text.as_str(), String::as_str)
            } else {
                text.as_str()
            };
            if let Some((replacement, replacement_blocks)) =
                self.fuzzy_replacement(database_id, tid, index, raw, staged)?
            {
                *text = replacement;
                *term_blocks = replacement_blocks;
                fuzzy = true;
            }
        }

        let (negative_terms, negated) =
            self.negative_terms(database_id, tid, index, &parsed.negative)?;
        let (total_docs, avg_doc_len) = self
            .index_stats(database_id, tid, index)
            .map_err(FtsIndexError::backend)?;
        Ok(Some(ResolvedQuery {
            texts,
            blocks,
            term_groups,
            fuzzy,
            groups,
            require_all: query.mode == QueryMode::And && groups > 1,
            negative_terms,
            negated,
            total_docs,
            avg_doc_len,
        }))
    }

    /// The closest fuzzy term that matches a live document, with its blocks.
    fn fuzzy_replacement(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        raw: &str,
        staged: Option<&StagedView>,
    ) -> Result<Option<(String, TermBlocks)>, FtsIndexError<B::Error>> {
        let vocabulary = staged.map(StagedView::vocabulary).unwrap_or_default();
        let candidates = self
            .fuzzy_candidates(database_id, tid, index, raw, &vocabulary)
            .map_err(FtsIndexError::backend)?;
        for candidate in candidates {
            let mut blocks =
                self.term_blocks(database_id, tid, index, std::slice::from_ref(&candidate))?;
            let Some(blocks) = blocks.pop() else {
                continue;
            };
            if term_is_live(&candidate, &blocks, staged) {
                return Ok(Some((candidate, blocks)));
            }
        }
        Ok(None)
    }

    /// The analyzed, synonym-expanded negative terms and the indexed
    /// documents holding any of them.
    fn negative_terms(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        raw_negative: &[String],
    ) -> Result<(HashSet<String>, SurrogateBitmap), FtsIndexError<B::Error>> {
        let mut negated = SurrogateBitmap::new();
        if raw_negative.is_empty() {
            return Ok((HashSet::new(), negated));
        }
        let base = self
            .analyze_for_collection(database_id, tid, index.collection(), &raw_negative.join(" "))
            .map_err(FtsIndexError::backend)?;
        if base.is_empty() {
            return Ok((HashSet::new(), negated));
        }
        let terms = self
            .expand_query_with_synonyms(database_id, tid, base)
            .map_err(FtsIndexError::backend)?;
        for blocks in self.term_blocks(database_id, tid, index, &terms)? {
            for doc_id in blocks.doc_ids() {
                negated.insert(doc_id);
            }
        }
        Ok((terms.into_iter().collect(), negated))
    }

    /// Posting blocks of each term: the LSM memtable and segments, or the
    /// backend's posting table when the LSM holds none.
    pub(crate) fn term_blocks(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        texts: &[String],
    ) -> Result<Vec<TermBlocks>, FtsIndexError<B::Error>> {
        let mut blocks = lsm_query::collect_merged_term_blocks(
            &self.backend,
            database_id,
            tid,
            index,
            self.memtable(),
            texts,
            &self.governor,
        )
        .map_err(FtsIndexError::backend)?;
        for (text, term) in texts.iter().zip(blocks.iter_mut()) {
            if term.df > 0 {
                continue;
            }
            let postings = self
                .backend
                .read_postings(database_id, tid, index, text)
                .map_err(FtsIndexError::backend)?;
            if !postings.is_empty() {
                let compact = self.compact_postings(database_id, tid, index, &postings)?;
                *term = TermBlocks::from_blocks(into_blocks(compact));
            }
        }
        Ok(blocks)
    }

    /// Backend postings as compact postings, each with the fieldnorm of its
    /// document's recorded length. Lengths are read in one batch.
    fn compact_postings(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        postings: &[Posting],
    ) -> Result<Vec<CompactPosting>, FtsIndexError<B::Error>> {
        let doc_ids: Vec<Surrogate> = postings.iter().map(|p| p.doc_id).collect();
        let lengths = self
            .backend
            .read_doc_lengths(database_id, tid, index, &doc_ids)
            .map_err(FtsIndexError::backend)?;
        let mut compact = Vec::with_capacity(postings.len());
        for (posting, length) in postings.iter().zip(lengths) {
            let doc_len = match length {
                Some(len) => len,
                // A posting outlives no length in a consistent index. The
                // term frequency is the tightest length the posting proves.
                None => self
                    .read_fieldnorm(database_id, tid, index, posting.doc_id)
                    .map_err(FtsIndexError::backend)?
                    .unwrap_or(posting.term_freq),
            };
            compact.push(CompactPosting {
                doc_id: posting.doc_id,
                term_freq: posting.term_freq,
                fieldnorm: smallfloat::encode(doc_len),
                positions: posting.positions.clone(),
            });
        }
        Ok(compact)
    }
}

/// Whether `term` matches a document the reader sees: an indexed document
/// the transaction does not hide, or a staged document.
fn term_is_live(term: &str, blocks: &TermBlocks, staged: Option<&StagedView>) -> bool {
    match staged {
        None => blocks.df > 0,
        Some(view) => {
            (!view.hides_all() && blocks.doc_ids().any(|doc| !view.hides(doc)))
                || view.holds_term(term)
        }
    }
}

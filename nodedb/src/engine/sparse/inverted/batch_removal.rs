// SPDX-License-Identifier: BUSL-1.1

//! Removal of many documents from the inverted index in one write
//! transaction.
//!
//! A bulk delete removes its rows' text in batches. Each posting list a batch
//! occupies is rewritten once for the whole batch, and each index's corpus
//! counters are updated once, so a batch costs the lists it touches rather
//! than one rewrite per document per term.

use std::collections::{BTreeMap, HashSet};

use nodedb_fts::IndexScope;
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::doc_fields;
use super::doc_terms;
use super::errors::inverted_err;
use super::indexing::{IndexDocScope, prior_doc_length};
use crate::engine::sparse::fts_redb::keys::doc_key;
use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;

/// The edits one index takes from a batch.
#[derive(Default)]
struct IndexEdits {
    /// Term → documents leaving its posting list.
    postings: BTreeMap<String, HashSet<Surrogate>>,
    /// Documents leaving the index.
    count: i64,
    /// Token length leaving the index.
    total: i64,
}

impl InvertedIndex {
    /// Remove `surrogates` from every index of `collection` in one write
    /// transaction. A document not in an index is left alone there. Either
    /// every removal commits or none does.
    pub fn remove_documents(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogates: &[Surrogate],
    ) -> crate::Result<()> {
        if surrogates.is_empty() {
            return Ok(());
        }
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        // Keyed by field key: empty for the whole-document index.
        let mut edits: BTreeMap<String, IndexEdits> = BTreeMap::new();
        for surrogate in surrogates {
            let doc = IndexDocScope {
                database_id,
                tid,
                collection,
                surrogate: *surrogate,
            };
            self.note_doc_write(doc);
            let mut field_keys = doc_fields::read(&txn, doc)?;
            field_keys.push(String::new());
            for field_key in field_keys {
                let index = scope_of(collection, &field_key)?;
                let Some(old_len) = prior_doc_length(&txn, doc, index)? else {
                    continue;
                };
                let terms = doc_terms::occupied_terms(&txn, doc, index, true)?;
                doc_terms::clear(&txn, doc, index)?;
                {
                    let mut lengths = txn
                        .open_table(DOC_LENGTHS)
                        .map_err(|e| inverted_err("open doc_lengths", e))?;
                    lengths
                        .remove(doc_key(database_id, tid.as_u64(), index, *surrogate))
                        .map_err(|e| inverted_err("remove doc length", e))?;
                }
                let entry = edits.entry(field_key).or_default();
                for term in terms {
                    entry.postings.entry(term).or_default().insert(*surrogate);
                }
                entry.count -= 1;
                entry.total -= i64::from(old_len);
            }
            doc_fields::clear(&txn, doc)?;
        }
        for (field_key, index_edits) in &edits {
            let index = scope_of(collection, field_key)?;
            doc_terms::strip_postings_many(
                &txn,
                database_id,
                tid.as_u64(),
                index,
                &index_edits.postings,
            )?;
            Self::update_stats_in_txn(
                &txn,
                database_id,
                tid,
                index,
                index_edits.count,
                index_edits.total,
            )?;
        }
        txn.commit()
            .map_err(|e| inverted_err("commit batch remove", e))?;
        Ok(())
    }
}

/// The index a stored field key names: the whole-document index for the
/// empty key.
fn scope_of<'a>(collection: &'a str, field_key: &'a str) -> crate::Result<IndexScope<'a>> {
    if field_key.is_empty() {
        return Ok(IndexScope::document(collection));
    }
    IndexScope::field(collection, field_key)
        .ok_or_else(|| inverted_err("doc_fields", "stored field name is empty"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;

    use super::*;
    use crate::engine::durability_gate::GatedDatabase;
    use crate::engine::sparse::inverted::test_support::{body, fields};

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(GatedDatabase::new(redb::Database::create(&path).unwrap()));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    fn hits(idx: &InvertedIndex, index: IndexScope<'_>, query: &str) -> Vec<Surrogate> {
        idx.search(
            DB,
            T,
            index,
            FtsSearchParams {
                query,
                top_k: 100,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap()
        .into_iter()
        .map(|h| h.doc_id)
        .collect()
    }

    /// A batch removal leaves the index exactly as removing each document
    /// one by one does: postings, counters, and field indexes.
    #[test]
    fn batch_removal_matches_one_by_one_removal() {
        let (idx, _dir) = open_temp();
        for i in 1..=5u32 {
            idx.index_document(
                DB,
                T,
                "docs",
                Surrogate::new(i),
                &fields(&[("title", "rust"), ("body", "shared words here")]),
            )
            .unwrap();
        }
        idx.index_document(DB, T, "docs", Surrogate::new(6), &body("rust only"))
            .unwrap();

        let removed: Vec<Surrogate> = (1..=5u32).map(Surrogate::new).collect();
        idx.remove_documents(DB, T, "docs", &removed).unwrap();

        assert_eq!(
            hits(&idx, IndexScope::document("docs"), "rust"),
            vec![Surrogate::new(6)]
        );
        assert!(hits(&idx, IndexScope::document("docs"), "shared").is_empty());
        let title = IndexScope::field("docs", "title").unwrap();
        assert!(hits(&idx, title, "rust").is_empty());
        assert_eq!(idx.corpus_stats(DB, T, "docs").unwrap().0, 1);
        assert_eq!(idx.corpus_stats(DB, T, title).unwrap().0, 0);
    }

    /// A surrogate the index does not hold is a no-op, not a second
    /// decrement.
    #[test]
    fn removing_an_unindexed_document_changes_nothing() {
        let (idx, _dir) = open_temp();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha"))
            .unwrap();
        idx.remove_documents(DB, T, "docs", &[Surrogate::new(9)])
            .unwrap();
        assert_eq!(idx.corpus_stats(DB, T, "docs").unwrap().0, 1);
    }
}

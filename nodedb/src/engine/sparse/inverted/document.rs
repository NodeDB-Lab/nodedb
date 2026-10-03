// SPDX-License-Identifier: BUSL-1.1

//! Per-document orchestration across a collection's indexes: the
//! whole-document index and one index per top-level string field.

use std::collections::BTreeSet;

use redb::WriteTransaction;

use nodedb_fts::{DocumentText, IndexScope};

use super::core::InvertedIndex;
use super::doc_fields;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;

impl InvertedIndex {
    /// Replace a document's footprint in every index: the whole-document index
    /// and one index per top-level string field. A field the previous version
    /// held and this one does not is retracted from that field's index.
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        text: &DocumentText,
    ) -> crate::Result<()> {
        self.note_doc_write(doc);
        let prior = doc_fields::read(txn, doc)?;
        self.index_scope_in_txn(
            txn,
            doc,
            IndexScope::document(doc.collection),
            &text.whole(),
        )?;
        let mut held: BTreeSet<&str> = BTreeSet::new();
        for (index, value) in text.field_scopes(doc.collection) {
            if self.index_scope_in_txn(txn, doc, index, value)?
                && let Some(name) = index.field_name()
            {
                held.insert(name);
            }
        }
        for gone in prior.iter().filter(|f| !held.contains(f.as_str())) {
            let index = IndexScope::field(doc.collection, gone)
                .ok_or_else(|| inverted_err("doc_fields", "stored field name is empty"))?;
            self.remove_scope_in_txn(txn, doc, index)?;
        }
        doc_fields::put(txn, doc, &held)
    }

    /// Index `text` under one scope. Text with no terms removes the document
    /// from that scope. Returns whether the document holds postings there.
    fn index_scope_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        index: IndexScope<'_>,
        text: &str,
    ) -> crate::Result<bool> {
        let tokens = self.analyze_for_collection(doc.database_id, doc.tid, doc.collection, text)?;
        if tokens.is_empty() {
            self.remove_scope_in_txn(txn, doc, index)?;
            return Ok(false);
        }
        self.write_index_data(txn, doc, index, &tokens)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use redb::{Database, ReadableDatabase as _, ReadableTable as _};

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;
    use nodedb_types::{Surrogate, TenantId};

    use super::*;
    use crate::engine::sparse::fts_redb::keys::doc_fields_key;
    use crate::engine::sparse::fts_redb::tables::DOC_FIELDS;
    use crate::engine::sparse::inverted::test_support::fields;

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);
    const DOCS: IndexScope<'static> = IndexScope::document("docs");

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(crate::engine::durability_gate::GatedDatabase::new(
            Database::create(&path).unwrap(),
        ));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    fn scope(field: &str) -> IndexScope<'_> {
        IndexScope::field("docs", field).unwrap()
    }

    fn hits(idx: &InvertedIndex, index: IndexScope<'_>, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = idx
            .search(
                DB,
                T,
                index,
                FtsSearchParams {
                    query,
                    top_k: 100,
                    fuzzy_enabled: false,
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|r| r.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    fn doc_fields_row(idx: &InvertedIndex, surrogate: u32) -> Option<Vec<String>> {
        let txn = idx.backend().db().begin_read().unwrap();
        let table = txn.open_table(DOC_FIELDS).unwrap();
        table
            .get(doc_fields_key(
                DB,
                T.as_u64(),
                "docs",
                Surrogate::new(surrogate),
            ))
            .unwrap()
            .map(|v| zerompk::from_msgpack::<Vec<String>>(v.value()).unwrap())
    }

    fn crossed(idx: &InvertedIndex) {
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &fields(&[("title", "rust"), ("body", "python guide")]),
        )
        .unwrap();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(2),
            &fields(&[("body", "rust handbook")]),
        )
        .unwrap();
    }

    #[test]
    fn a_field_search_returns_only_documents_holding_the_term_in_that_field() {
        let (idx, _dir) = open_temp();
        crossed(&idx);
        assert_eq!(hits(&idx, scope("title"), "rust"), vec![1]);
        assert_eq!(hits(&idx, scope("body"), "rust"), vec![2]);
        assert_eq!(hits(&idx, DOCS, "rust"), vec![1, 2]);
    }

    #[test]
    fn a_field_index_counts_only_documents_holding_the_field() {
        let (idx, _dir) = open_temp();
        crossed(&idx);
        assert_eq!(idx.corpus_stats(DB, T, scope("title")).unwrap(), (1, 1.0));
        assert_eq!(idx.corpus_stats(DB, T, scope("body")).unwrap(), (2, 2.0));
        assert_eq!(idx.corpus_stats(DB, T, DOCS).unwrap(), (2, 2.5));
    }

    #[test]
    fn moving_a_word_between_fields_retracts_it_and_keeps_the_whole_document() {
        let (idx, _dir) = open_temp();
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &fields(&[("body", "guide"), ("title", "rust")]),
        )
        .unwrap();
        let whole_before = (
            idx.corpus_stats(DB, T, DOCS).unwrap(),
            hits(&idx, DOCS, "rust"),
            hits(&idx, DOCS, "guide"),
        );

        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &fields(&[("body", "guide rust")]),
        )
        .unwrap();

        assert!(hits(&idx, scope("title"), "rust").is_empty());
        assert_eq!(idx.corpus_stats(DB, T, scope("title")).unwrap().0, 0);
        assert_eq!(hits(&idx, scope("body"), "rust"), vec![1]);
        assert_eq!(idx.term_df(DB, T, scope("title"), "rust").unwrap(), 0);
        let whole_after = (
            idx.corpus_stats(DB, T, DOCS).unwrap(),
            hits(&idx, DOCS, "rust"),
            hits(&idx, DOCS, "guide"),
        );
        assert_eq!(whole_before, whole_after);
        assert_eq!(doc_fields_row(&idx, 1), Some(vec!["body".to_string()]));
    }

    #[test]
    fn delete_removes_every_scope_and_the_field_set() {
        let (idx, _dir) = open_temp();
        crossed(&idx);
        assert_eq!(
            doc_fields_row(&idx, 1),
            Some(vec!["body".to_string(), "title".to_string()])
        );

        idx.remove_document(DB, T, "docs", Surrogate::new(1))
            .unwrap();

        assert!(hits(&idx, scope("title"), "rust").is_empty());
        assert!(hits(&idx, scope("body"), "python").is_empty());
        assert_eq!(hits(&idx, DOCS, "rust"), vec![2]);
        assert_eq!(idx.corpus_stats(DB, T, scope("title")).unwrap().0, 0);
        assert_eq!(idx.corpus_stats(DB, T, scope("body")).unwrap().0, 1);
        assert_eq!(idx.corpus_stats(DB, T, DOCS).unwrap().0, 1);
        assert_eq!(doc_fields_row(&idx, 1), None);
    }

    #[test]
    fn purge_collection_removes_field_scopes() {
        let (idx, _dir) = open_temp();
        crossed(&idx);
        idx.index_document(
            DB,
            T,
            "other",
            Surrogate::new(3),
            &fields(&[("title", "rust")]),
        )
        .unwrap();

        idx.purge_collection(DB, T, "docs").unwrap();

        for index in [scope("title"), scope("body"), DOCS] {
            assert!(!idx.has_text(DB, T, index).unwrap());
            assert!(hits(&idx, index, "rust").is_empty());
        }
        assert_eq!(doc_fields_row(&idx, 1), None);
        assert_eq!(doc_fields_row(&idx, 2), None);
        let other_title = IndexScope::field("other", "title").unwrap();
        assert!(idx.has_text(DB, T, other_title).unwrap());
    }

    #[test]
    fn has_text_is_false_after_the_last_holder_is_deleted() {
        let (idx, _dir) = open_temp();
        crossed(&idx);
        assert!(idx.has_text(DB, T, scope("title")).unwrap());
        assert!(!idx.has_text(DB, T, scope("missing")).unwrap());

        idx.remove_document(DB, T, "docs", Surrogate::new(1))
            .unwrap();

        assert!(!idx.has_text(DB, T, scope("title")).unwrap());
        assert!(idx.has_text(DB, T, scope("body")).unwrap());
    }
}

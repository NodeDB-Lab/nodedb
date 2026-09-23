// SPDX-License-Identifier: BUSL-1.1

//! A document's complete index footprint, captured so a rollback can put it
//! back exactly.
//!
//! The index does not keep a document's text. It keeps, per term, the
//! positions the term occupies in the document's analyzed token stream, and
//! the stream's length. Those positions are dense (`0..len`), so the token
//! stream rebuilds from them exactly. Re-indexing that stream reproduces the
//! document's postings, its length row, its term set, and the collection's
//! corpus counters.

use nodedb_fts::posting::Posting;
use nodedb_types::{Surrogate, TenantId};
use redb::ReadableTable as _;

use super::core::InvertedIndex;
use super::doc_terms;
use super::errors::inverted_err;
use super::indexing::{IndexDocScope, prior_doc_length};
use crate::engine::sparse::fts_redb::tables::POSTINGS;

/// The analyzed token stream one document is indexed with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsDocImage {
    tokens: Vec<String>,
}

impl InvertedIndex {
    /// The index footprint of `surrogate` in `collection`, or `None` when the
    /// document is not indexed. Reads only: the write transaction it opens to
    /// reach the term-set fallback is aborted.
    pub fn document_image(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Option<FtsDocImage>> {
        let scope = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        };
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        let image = Self::read_document_image(&txn, scope)?;
        txn.abort()
            .map_err(|e| inverted_err("abort image read", e))?;
        Ok(image)
    }

    /// Put the index footprint of `surrogate` back to `image`, or remove the
    /// document when `image` is `None`: it was not indexed before.
    pub fn restore_document_image(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        image: Option<&FtsDocImage>,
    ) -> crate::Result<()> {
        let Some(image) = image.filter(|image| !image.tokens.is_empty()) else {
            return self.remove_document(database_id, tid, collection, surrogate);
        };
        let scope = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        };
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_index_data(&txn, scope, &image.tokens)?;
        txn.commit()
            .map_err(|e| inverted_err("commit image restore", e))?;
        Ok(())
    }

    fn read_document_image(
        txn: &redb::WriteTransaction,
        scope: IndexDocScope<'_>,
    ) -> crate::Result<Option<FtsDocImage>> {
        let Some(doc_len) = prior_doc_length(txn, scope)? else {
            return Ok(None);
        };
        let terms = doc_terms::occupied_terms(txn, scope, true)?;
        let postings = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;
        let mut tokens = vec![String::new(); doc_len as usize];
        for term in terms {
            let list: Vec<Posting> = postings
                .get((
                    scope.database_id,
                    scope.tid.as_u64(),
                    scope.collection,
                    term.as_str(),
                ))
                .map_err(|e| inverted_err("read postings", e))?
                .map(|bytes| zerompk::from_msgpack(bytes.value()))
                .transpose()
                .map_err(|e| inverted_err("decode postings", e))?
                .unwrap_or_default();
            let Some(posting) = list.iter().find(|p| p.doc_id == scope.surrogate) else {
                continue;
            };
            for &position in &posting.positions {
                let slot = tokens.get_mut(position as usize).ok_or_else(|| {
                    inverted_err(
                        "document image",
                        format!(
                            "term '{term}' sits at position {position} past the document \
                             length {doc_len}"
                        ),
                    )
                })?;
                slot.clone_from(&term);
            }
        }
        if let Some(position) = tokens.iter().position(String::is_empty) {
            return Err(inverted_err(
                "document image",
                format!("no posting covers position {position} of {doc_len}"),
            ));
        }
        Ok(Some(FtsDocImage { tokens }))
    }
}

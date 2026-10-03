// SPDX-License-Identifier: BUSL-1.1

//! A document's complete index footprint, captured so a rollback can put it
//! back exactly.
//!
//! The index does not keep a document's text. It keeps, per index and per
//! term, the positions the term occupies in the document's analyzed token
//! stream, and the stream's length. Those positions are dense (`0..len`), so
//! each index's token stream rebuilds from them exactly. Re-indexing those
//! streams reproduces the document's postings, its length rows, its term
//! sets, its field set, and every index's corpus counters.

use std::collections::BTreeSet;

use nodedb_fts::IndexScope;
use nodedb_fts::posting::Posting;
use nodedb_types::{Surrogate, TenantId};
use redb::ReadableTable as _;

use super::core::InvertedIndex;
use super::doc_fields;
use super::doc_terms;
use super::errors::inverted_err;
use super::indexing::{IndexDocScope, prior_doc_length};
use crate::engine::sparse::fts_redb::keys::posting_key;
use crate::engine::sparse::fts_redb::tables::POSTINGS;

/// The analyzed token streams one document is indexed with, one per index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsDocImage {
    /// `(field_key, tokens)` per index the document holds postings in. The
    /// empty field key is the whole-document index.
    scopes: Vec<(String, Vec<String>)>,
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
        let doc = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        };
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        let image = Self::read_document_image(&txn, doc)?;
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
        let Some(image) = image.filter(|image| !image.scopes.is_empty()) else {
            return self.remove_document(database_id, tid, collection, surrogate);
        };
        let doc = IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        };
        let db = self.inner.backend().db();
        let txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_document_image(&txn, doc, image)?;
        txn.commit()
            .map_err(|e| inverted_err("commit image restore", e))?;
        Ok(())
    }

    /// Replace the document's footprint in every index with `image`.
    pub(super) fn write_document_image(
        &self,
        txn: &redb::WriteTransaction,
        doc: IndexDocScope<'_>,
        image: &FtsDocImage,
    ) -> crate::Result<()> {
        self.remove_document_in_txn(txn, doc)?;
        let mut fields: BTreeSet<&str> = BTreeSet::new();
        for (field, tokens) in &image.scopes {
            let index = IndexScope::from_key(doc.collection, field);
            self.write_index_data(txn, doc, index, tokens)?;
            if let Some(name) = index.field_name() {
                fields.insert(name);
            }
        }
        doc_fields::put(txn, doc, &fields)
    }

    /// The footprint of `doc` across the whole-document index and every field
    /// index its field set names. `None` when it holds no postings anywhere.
    pub(super) fn read_document_image(
        txn: &redb::WriteTransaction,
        doc: IndexDocScope<'_>,
    ) -> crate::Result<Option<FtsDocImage>> {
        let mut keys = vec![String::new()];
        keys.extend(doc_fields::read(txn, doc)?);
        let mut scopes = Vec::with_capacity(keys.len());
        for key in keys {
            let index = IndexScope::from_key(doc.collection, &key);
            if let Some(tokens) = read_scope_tokens(txn, doc, index)? {
                scopes.push((key, tokens));
            }
        }
        Ok((!scopes.is_empty()).then_some(FtsDocImage { scopes }))
    }
}

/// The analyzed token stream of `doc` in one index, or `None` when it is not
/// in that index.
fn read_scope_tokens(
    txn: &redb::WriteTransaction,
    doc: IndexDocScope<'_>,
    index: IndexScope<'_>,
) -> crate::Result<Option<Vec<String>>> {
    let Some(doc_len) = prior_doc_length(txn, doc, index)? else {
        return Ok(None);
    };
    let terms = doc_terms::occupied_terms(txn, doc, index, true)?;
    let postings = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;
    let mut tokens = vec![String::new(); doc_len as usize];
    for term in terms {
        let list: Vec<Posting> = postings
            .get(posting_key(
                doc.database_id,
                doc.tid.as_u64(),
                index,
                term.as_str(),
            ))
            .map_err(|e| inverted_err("read postings", e))?
            .map(|bytes| zerompk::from_msgpack(bytes.value()))
            .transpose()
            .map_err(|e| inverted_err("decode postings", e))?
            .unwrap_or_default();
        let Some(posting) = list.iter().find(|p| p.doc_id == doc.surrogate) else {
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
    Ok(Some(tokens))
}

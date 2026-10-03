// SPDX-License-Identifier: BUSL-1.1

//! Full-text maintenance for UPDATE.
//!
//! An UPDATE rewrites a row's body in place, so the row keeps its surrogate
//! while its text changes. The inverted index must follow in the update's
//! own transaction: without it, the row keeps matching the words its old
//! body held and misses the words its new body holds.
//!
//! The text is extracted exactly as the insert path extracts it
//! (`fts_text::extract_fts_fields`), so an updated row indexes the same way a
//! freshly inserted row with the same body does. `index_document_in_txn`
//! retracts the terms the new text no longer contains, retracts the row from
//! every field index the new body no longer fills, and removes the row from
//! an index whose new text has no indexable word.

use redb::WriteTransaction;

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_fields;
use crate::engine::sparse::inverted::IndexDocScope;
use crate::types::TenantId;

/// Inputs to [`CoreLoop::update_reindex_text`].
pub(in crate::data::executor) struct UpdateTextReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: Surrogate,
    /// The row's post-update document.
    pub new_doc: &'a serde_json::Value,
}

impl CoreLoop {
    /// Re-index the updated row's text inside `txn`. An error rejects the
    /// update: `txn` must then be dropped un-committed, so the body and its
    /// postings never disagree.
    pub(in crate::data::executor) fn update_reindex_text(
        &self,
        txn: &WriteTransaction,
        p: UpdateTextReindex<'_>,
    ) -> crate::Result<()> {
        let text = extract_fts_fields(p.new_doc);
        self.inverted
            .index_document_in_txn(
                txn,
                IndexDocScope {
                    database_id: p.database_id,
                    tid: TenantId::new(p.tid),
                    collection: p.collection,
                    surrogate: p.surrogate,
                },
                &text,
            )
            .inspect_err(|e| {
                crate::diag::fts_index_update_failed(e, p.collection, p.surrogate.as_u32());
            })
    }
}

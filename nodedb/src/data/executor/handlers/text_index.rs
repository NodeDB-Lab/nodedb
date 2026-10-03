// SPDX-License-Identifier: BUSL-1.1

//! Resolution of the inverted index a full-text read of one field runs
//! against. Shared by text search, phrase search, score columns, and the
//! text leg of hybrid search.

use nodedb_fts::IndexScope;
use nodedb_types::text_search::TextColumnFault;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_fields;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// The index a read of `field` runs against. `None` field reads the
    /// whole-document index.
    ///
    /// `Ok(None)`: the collection holds no text at all, so the read matches
    /// nothing. A field that no document holds as text, while other text
    /// exists, is `TextColumn { NotIndexed }`, the same as Lite. A column the
    /// strict schema declares is never that error: it holds no text yet.
    pub(in crate::data::executor) fn text_index<'a>(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &'a str,
        field: Option<&'a str>,
    ) -> crate::Result<Option<IndexScope<'a>>> {
        let database_id = task.request.database_id;
        let tenant = TenantId::new(tid);
        let document = IndexScope::document(collection);
        let Some(name) = field else {
            return Ok(Some(document));
        };
        let column_error = |fault| crate::Error::TextColumn {
            collection: collection.to_owned(),
            column: name.to_owned(),
            fault,
        };
        let index = IndexScope::field(collection, name)
            .ok_or_else(|| column_error(TextColumnFault::NotAColumn))?;
        let declared = self
            .strict_schema_for(database_id, tenant, collection)
            .is_some_and(|schema| schema.columns.iter().any(|c| c.name == name));
        if declared
            || self
                .inverted
                .has_text(database_id.as_u64(), tenant, index)?
            || self.staged_index_text(task, tid, collection, index)?
        {
            return Ok(Some(index));
        }
        if self
            .inverted
            .has_text(database_id.as_u64(), tenant, document)?
            || self.staged_index_text(task, tid, collection, document)?
        {
            return Err(column_error(TextColumnFault::NotIndexed));
        }
        Ok(None)
    }

    /// Whether a staged put of the issuing transaction holds text in `index`.
    /// A field first written inside the transaction is thereby searchable
    /// before COMMIT.
    fn staged_index_text(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        index: IndexScope<'_>,
    ) -> crate::Result<bool> {
        let Some(txn_id) = task.request.txn_id else {
            return Ok(false);
        };
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(false);
        };
        let config_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        for (_, staged) in overlay.iter_for_collection(&config_key) {
            let Staged::Put(body) = staged else {
                continue;
            };
            let Some(doc) = self.decode_indexed_body(&config_key, body)? else {
                continue;
            };
            if !extract_fts_fields(&doc).text_of(index).is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

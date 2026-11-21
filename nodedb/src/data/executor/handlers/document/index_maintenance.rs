// SPDX-License-Identifier: BUSL-1.1

//! Document secondary-index maintenance: BackfillIndex, DropIndex.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_backfill_index`].
pub(in crate::data::executor) struct BackfillIndexParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
}

impl CoreLoop {
    /// Backfill an index: walk every version of every document in the
    /// collection and populate versioned index entries for the given field.
    /// Atomic — one write transaction covers the whole backfill, and a
    /// UNIQUE violation among the current rows aborts it before any entry is
    /// written, leaving the index empty (the caller's Building→Ready flip is
    /// skipped, so readers never see a partial-index view).
    pub(in crate::data::executor) fn execute_backfill_index(
        &mut self,
        task: &ExecutionTask,
        params: BackfillIndexParams<'_>,
    ) -> Response {
        match self.backfill_document_index(task.request.database_id, params) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(task, crate::bridge::envelope::ErrorCode::from(e)),
        }
    }

    /// The build [`Self::execute_backfill_index`] answers for, shared with
    /// restart replay of a `DocumentIndexBuild` record. A UNIQUE duplicate is
    /// `Error::RejectedConstraint`, returned before any entry is written.
    pub(in crate::data::executor) fn backfill_document_index(
        &mut self,
        database_id: crate::types::DatabaseId,
        params: BackfillIndexParams<'_>,
    ) -> crate::Result<()> {
        let crs = self.crs_context();
        let BackfillIndexParams {
            tid,
            collection,
            path,
            is_array,
            unique,
            case_insensitive,
            predicate,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %path,
            unique,
            case_insensitive,
            partial = predicate.is_some(),
            "backfill index"
        );
        if let Some(ref m) = self.metrics {
            m.record_document_index_backfill();
        }

        // Parse the partial-index predicate once, up front. An
        // unparsable predicate is a catalog-level bug — the DDL layer
        // already validates the text at CREATE INDEX time, so a
        // failure here means the stored entry drifted from what the
        // grammar accepts. Refuse the backfill rather than silently
        // over-populating a "partial" index.
        let parsed_predicate = match predicate {
            Some(text) => match crate::engine::document::predicate::IndexPredicate::parse(text) {
                Some(p) => Some(p),
                None => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "backfill: partial-index predicate failed to parse: {text}"
                        ),
                    });
                }
            },
            None => None,
        };

        // The encoding of these stored bodies is resolved from the collection's
        // registered kind, never sniffed from the bytes: a strict collection
        // stores Binary Tuples and a vector-primary one stores tagged sidecars,
        // and the schemaless MessagePack decoder reads neither. Decoding them
        // all as documents is what made `CREATE INDEX` on a strict collection
        // build an EMPTY index and report success.
        let body_format =
            self.sparse_body_format(database_id, crate::types::TenantId::new(tid), collection);
        let database_id = database_id.as_u64();

        // A UNIQUE index judges the current version of every row before any
        // entry is written: a duplicate among them refuses the backfill and
        // leaves the index empty. Rows stream one at a time, so no row count
        // caps the judgement.
        if unique {
            let mut seen: std::collections::HashMap<
                String,
                crate::engine::document::store::StorageKey,
            > = std::collections::HashMap::new();
            self.for_each_current_row(database_id, tid, collection, |doc_id, bytes| {
                // A row skipped here is a row the finished index permanently
                // omits, and the index is then reported as built.
                let doc = decode_scanned_document(bytes, body_format.as_format_ref())?;
                // Partial-index predicate: only rows for which the predicate
                // is explicitly true are indexed (Postgres partial-index
                // semantics).
                if let Some(ref p) = parsed_predicate
                    && !p.evaluate_json(&doc, &crs)
                {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
                for raw in
                    crate::engine::document::store::extract_index_values(&doc, path, is_array)
                {
                    let stored = if case_insensitive {
                        raw.to_lowercase()
                    } else {
                        raw
                    };
                    if let Some(prev) = seen.get(&stored)
                        && prev != doc_id
                    {
                        return Err(crate::Error::RejectedConstraint {
                            collection: collection.to_string(),
                            constraint: "unique".to_string(),
                            detail: format!(
                                "unique index backfill: duplicate value '{stored}' on \
                                     '{path}' (existing '{prev}', new '{doc_id}')"
                            ),
                        });
                    }
                    seen.insert(stored, *doc_id);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
        }

        let txn = self.sparse.begin_write()?;

        // Every version of every row, each at its own system time, so a
        // lookup at any cut finds each row by the values it held then.
        self.backfill_versioned_index(
            &txn,
            &super::index_backfill_versions::VersionedBackfill {
                database_id,
                tid,
                collection,
                path,
                is_array,
                case_insensitive,
                predicate: parsed_predicate.as_ref(),
                body_format: body_format.as_format_ref(),
            },
        )?;

        txn.commit().map_err(|e| crate::Error::Internal {
            detail: format!("backfill commit: {e}"),
        })
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Removes every version of the field's index entries.
    /// Returns `{"removed": N}` as the response payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            "drop document index"
        );

        let removed = self.purge_document_index_field(
            task.request.database_id.as_u64(),
            tid,
            collection,
            field,
        );
        match removed {
            Ok(removed) => {
                match super::super::super::response_codec::encode_count("removed", removed) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => {
                        self.response_error(task, crate::bridge::envelope::ErrorCode::from(e))
                    }
                }
            }
            Err(e) => self.response_error(task, crate::bridge::envelope::ErrorCode::from(e)),
        }
    }

    /// Remove every version of every entry of `field` in one write
    /// transaction, and return how many left. Shared with restart replay of
    /// a `DocumentIndexRetire` record. A field with no entry removes none.
    pub(in crate::data::executor) fn purge_document_index_field(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<usize> {
        let txn = self.sparse.begin_write()?;
        let removed = self.sparse.versioned_index_drop_field_in_txn(
            &txn,
            database_id,
            tid,
            collection,
            field,
        )?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("drop index commit: {e}"),
        })?;
        Ok(removed)
    }
}

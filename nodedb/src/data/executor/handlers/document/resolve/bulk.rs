// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the predicate document writes: `BulkUpdate`, `BulkDelete`.
//! Each scans the matched set via [`CoreLoop::scan_matching_documents`],
//! decides the write policy per row, and reports one mutation per row. The
//! matched set is resolved and shipped here so no replica re-scans the
//! predicate — drift is caught by each row's own content precondition instead.

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, ResolvedSumTarget, ReturningSpec, UpdateValue,
};
use nodedb_types::RlsWriteCheck;

use super::context::{
    ResolveResult, ResolvedPut, affected_payload, delete_mutation, put_mutation,
    resolved_response_payload,
};
use crate::bridge::envelope::ErrorCode;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::bulk_dml::update_project::{
    ProjectUpdateRows, ProjectedUpdateRow,
};
use crate::data::executor::handlers::{returning_rows, rls_write_gate};
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;

/// Borrowed arguments for [`CoreLoop::resolve_bulk_update`].
pub(super) struct ResolveBulkUpdate<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise — see `ProjectUpdateRows::declared_primary_key`.
    pub declared_primary_key: Option<&'a str>,
}

/// Borrowed arguments for [`CoreLoop::resolve_bulk_delete`].
pub(super) struct ResolveBulkDelete<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Resolve a `BulkUpdate` to one row write per matched row.
    pub(super) fn resolve_bulk_update(
        &self,
        task: &ExecutionTask,
        args: ResolveBulkUpdate<'_>,
    ) -> ResolveResult {
        let ResolveBulkUpdate {
            tid,
            collection,
            filter_bytes,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if let Some(config) = self.doc_configs.get(&config_key) {
            crate::data::executor::handlers::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )?;
        }

        let filters = decode_filters(filter_bytes)?;
        let doc_ids = self
            .scan_matching_documents(ctx.database_id, tid, collection, &filters)
            .map_err(ErrorCode::from)?;

        // The same projection the live handler runs before it writes anything,
        // so the resolve and the apply agree on each row's post-image.
        let projected = self
            .project_bulk_update_rows(ProjectUpdateRows {
                database_id: ctx.database_id,
                tid,
                collection,
                doc_ids: &doc_ids,
                updates,
                strict_schema: ctx.strict_schema.as_ref(),
                declared_primary_key,
            })
            .map_err(ErrorCode::from)?;

        let mut mutations = Vec::with_capacity(projected.len());
        let mut returned_docs: Vec<serde_json::Value> = Vec::new();
        for row in projected {
            let ProjectedUpdateRow {
                doc_id,
                storage_key,
                current_bytes,
                old_doc: _,
                doc,
                updated_bytes: _,
            } = row;
            // Decided against the post-update image, exactly as
            // `execute_bulk_update` decides it — on the same JSON document.
            rls_write_gate::admit_row(rls_write_check, &doc, tid, collection)
                .map_err(ErrorCode::from)?;
            // The projection stage already parsed `doc_id` as a storage key,
            // so its surrogate identity is always available here.
            let surrogate = storage_key.surrogate();
            mutations.push(put_mutation(ResolvedPut {
                collection,
                document_id: &doc_id,
                surrogate,
                value: doc_format::encode_to_msgpack(&doc),
                precondition: Some(current_bytes),
                resolved_sum_targets,
            }));
            if returning.is_some() {
                returned_docs.push(doc);
            }
        }

        let response_payload = match returning {
            Some(spec) => returning_rows::build_rows_payload(spec, rls_filters, &returned_docs)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                })?,
            None => affected_payload(mutations.len()),
        };
        Ok(DocumentResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve a `BulkDelete` to one row removal per matched row.
    pub(super) fn resolve_bulk_delete(
        &self,
        task: &ExecutionTask,
        args: ResolveBulkDelete<'_>,
    ) -> ResolveResult {
        let ResolveBulkDelete {
            tid,
            collection,
            filter_bytes,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let filters = decode_filters(filter_bytes)?;
        let doc_ids = self
            .scan_matching_documents(ctx.database_id, tid, collection, &filters)
            .map_err(ErrorCode::from)?;

        let mut mutations = Vec::with_capacity(doc_ids.len());
        let mut rows: Vec<(RowIdentity, Vec<u8>)> = Vec::new();
        for doc_id in doc_ids {
            // `doc_id` is a bare string from the raw-table scan. A shape
            // that fails to parse as a storage key can hold no row in
            // DOCUMENTS either way, so it is skipped the same as a row that
            // vanished between the scan and this read.
            let Some(key) = crate::engine::document::store::StorageKey::parse(&doc_id) else {
                continue;
            };
            let Some(stored) = self.doc_resolve_read(&ctx, collection, &key)? else {
                continue;
            };
            // `RETURNING` reports the row's client-visible identity, never
            // the storage key.
            let identity = key.to_identity();
            rls_write_gate::admit_stored_row(
                rls_write_check,
                &stored,
                &identity,
                ctx.strict_schema.as_ref(),
                tid,
                collection,
            )
            .map_err(ErrorCode::from)?;
            let surrogate = key.surrogate();
            mutations.push(delete_mutation(
                collection,
                &doc_id,
                surrogate,
                Some(stored.clone()),
                resolved_sum_targets,
            ));
            rows.push((identity, stored));
        }

        let borrowed: Vec<(&RowIdentity, &[u8])> = rows
            .iter()
            .map(|(id, body)| (id, body.as_slice()))
            .collect();
        let response_payload = resolved_response_payload(
            returning,
            rls_filters,
            ctx.strict_schema.as_ref(),
            &borrowed,
        )?;
        Ok(DocumentResolveOutcome {
            mutations,
            response_payload,
        })
    }
}

/// Decode a plan's filter payload. Empty means "no WHERE clause" — every row.
fn decode_filters(filter_bytes: &[u8]) -> Result<Vec<ScanFilter>, ErrorCode> {
    if filter_bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(filter_bytes).map_err(|e| ErrorCode::Internal {
        detail: format!("deserialize filters: {e}"),
    })
}

// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::UpdateFromJoin`: build a source join map, scan
//! the target and evaluate assignments into post-images, then write them or,
//! on the commit-time resolve pass, return matched rows for the expander to
//! rewrite into concrete `PointPut` ops without writing.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::response_codec::encode_json_as_msgpack;
use crate::data::executor::task::ExecutionTask;

use super::update_from_join_write::WriteResolvedRowsCtx;

pub(in crate::data::executor) use super::update_from_join_types::ResolvedUpdateRow;
pub(in crate::data::executor) use super::update_from_join_types::UpdateFromJoinParams;

impl CoreLoop {
    /// Execute an `UPDATE target FROM source WHERE target.join_col = source.join_col` operation.
    pub(in crate::data::executor) fn execute_update_from_join(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: UpdateFromJoinParams<'_>,
    ) -> Response {
        let UpdateFromJoinParams {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            target_filter_bytes,
            returning,
            resolve_only,
            source_rows,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            resolve_only,
            "update from join"
        );

        // Phase 1: Scan source collection, build join map:
        //   source_join_value (as string) → serde_json::Value (the source document).
        let source_map = match self.build_source_join_map(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            source_join_col,
            source_rows,
        ) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Check for strict storage mode on the target.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        if source_map.is_empty() {
            // No source rows — nothing matches. The RESOLVE pass returns an
            // empty match set; the write path reports zero affected.
            if resolve_only {
                return self.encode_resolved_update_rows(task, Vec::new(), strict_schema.as_ref());
            }
            let result = serde_json::json!({ "affected": 0u64 });
            return match encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        }

        // Phase 2: Deserialize target filters.
        let target_filters: Vec<ScanFilter> = if target_filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(target_filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize target_filters: {e}"),
                        },
                    );
                }
            }
        };

        // Scan the target, join, evaluate assignments, encode the post-image —
        // without writing. Shared by both the resolve pass and write path.
        let rows = match self.collect_update_from_join_rows(
            super::update_from_join_collect::CollectUpdateRows {
                task,
                tid,
                target_collection,
                source_alias,
                target_join_col,
                updates,
                source_map: &source_map,
                target_filters: &target_filters,
                strict_schema: strict_schema.as_ref(),
                config_key: &config_key,
                declared_primary_key,
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // RESOLVE pass: hand the matched rows back for COMMIT-time expansion.
        // No `sparse.put`, no vector re-index, no write-set, no events.
        if resolve_only {
            return self.encode_resolved_update_rows(task, rows, strict_schema.as_ref());
        }

        // Materialized-sum coverage check (leader-only): the plan's resolution
        // was derived from a resolve pass taken before this write pass, so the
        // join map or a row's join key may have moved since. Both images are
        // handed in, since a rewritten join key debits one target and credits
        // another. `updates` is empty: post-images are supplied, not re-derived.
        let sum_check = SumTargetCheck {
            database_id: task.request.database_id.as_u64(),
            tid,
            collection: target_collection,
            updates: &[],
            resolved: resolved_sum_targets,
        };
        // Gated so a target collection declaring no binding — nearly every one —
        // never decodes a pre-image or clones a post-image for this.
        if self.declares_materialized_sums(&sum_check) {
            let mut sum_images: Vec<serde_json::Value> = Vec::with_capacity(rows.len() * 2);
            for row in &rows {
                if let Some(old_doc) = self.decode_source_row(&sum_check, &row.old_body) {
                    sum_images.push(old_doc);
                }
                sum_images.push(row.doc.clone());
            }
            if self.sum_targets_diverged(&sum_check, &sum_images) {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // Gate every matched row on the target's write policy before any write,
        // so a rejected row can't leave rows ahead of it rewritten.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for row in &rows {
                if let Err(e) =
                    rls_write_gate::admit_row(rls_write_check, &row.doc, tid, target_collection)
                {
                    return self.response_error(task, e);
                }
            }
        }

        // Gate vector maintenance once so a non-vector target pays nothing;
        // a rewritten embedding must re-index or KNN scores stale vectors.
        let database_id = task.request.database_id.as_u64();
        let has_vectors = self.collection_has_vectors(database_id, tid, target_collection);

        // Checked over the whole resolved set before the first row is written —
        // each row commits in its own transaction, so a check after the loop
        // could only report a violation already made durable.
        let balanced_entries = {
            let images: Vec<(&[u8], &[u8])> = rows
                .iter()
                .map(|row| (row.old_body.as_slice(), row.body.as_slice()))
                .collect();
            self.balanced_entries_for_stored_updates(database_id, tid, target_collection, &images)
        };
        match balanced_entries {
            Ok(entries) => {
                if let Err(e) =
                    self.settle_balanced_entries(database_id, tid, target_collection, entries)
                {
                    return self.response_error(task, e);
                }
            }
            Err(e) => return self.response_error(task, e),
        }

        let outcome = match self.write_resolved_update_from_join_rows(
            task,
            WriteResolvedRowsCtx {
                tid,
                target_collection,
                resolved_sum_targets,
                has_vectors,
                is_strict: strict_schema.is_some(),
                want_returning: returning.is_some(),
            },
            rows,
        ) {
            Ok(o) => o,
            Err(resp) => return resp,
        };

        let mut response = if let Some(spec) = returning {
            match super::returning_rows::build_rows_payload(
                spec,
                rls_filters,
                &outcome.returned_docs,
            ) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("RETURNING encode: {e}"),
                    },
                ),
            }
        } else {
            let result = serde_json::json!({ "affected": outcome.affected });
            match encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        };
        if !outcome.write_set.is_empty() {
            response.write_set = outcome.write_set;
        }
        response
    }

    /// Encode the resolve pass payload the expander rewrites into concrete
    /// `PointPut` ops. The pre-image travels too, since materialized-sum
    /// resolution needs the delta between images.
    fn encode_resolved_update_rows(
        &self,
        task: &ExecutionTask,
        rows: Vec<ResolvedUpdateRow>,
        strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
    ) -> Response {
        let mut wire: Vec<crate::query::ResolvedUpdateRowWire> = Vec::with_capacity(rows.len());
        for r in rows {
            // The pre-image is stored bytes read straight off the scan; the
            // post-image is already decoded on the row.
            let old_doc = match doc_format::decode_document_or_binary_tuple(
                &r.old_body,
                strict_schema,
                "UPDATE ... FROM pre-image",
            ) {
                Ok(d) => d,
                Err(e) => return self.response_error(task, e),
            };
            wire.push((
                r.key.to_string(),
                r.key.surrogate().as_u32(),
                doc_format::encode_resolved_wire_body(&r.doc),
                doc_format::encode_resolved_wire_body(&old_doc),
            ));
        }
        match zerompk::to_msgpack_vec(&wire) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("update-from-join resolve encode: {e}"),
                },
            ),
        }
    }
}

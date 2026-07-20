// SPDX-License-Identifier: BUSL-1.1

//! Columnar insert dispatcher: builds rows, applies ON CONFLICT semantics,
//! drives the per-row insert, flushes the memtable, updates spatial index.

use nodedb_types::surrogate::Surrogate;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::document::UpdateValue;

use super::row_ingest::RowIngestParams;

/// Parameters for [`CoreLoop::execute_columnar_insert`].
pub(in crate::data::executor) struct ColumnarInsertParams<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub surrogates: &'a [Surrogate],
    pub schema_bytes: &'a [u8],
    pub provenance: Option<&'a SyncProvenance>,
}

impl CoreLoop {
    /// Execute a columnar insert: write rows from MessagePack payload to
    /// `MutationEngine`, applying intent-specific semantics on duplicate
    /// PK (upsert-overwrite for `Insert` and `Put`, silent skip for
    /// `InsertIfAbsent`, merge-via-`apply_on_conflict_updates` for `Put`
    /// with non-empty `on_conflict_updates`).
    ///
    /// When `provenance` is `Some`, the sync idempotency gate runs first.
    /// Duplicate / Fenced / Gap arms return `SyncAckResult` via
    /// `response_with_payload` without touching engine state.
    /// Apply → proceed with insert → call `sync_commit` → return
    /// `SyncAckResult{Applied}`.
    ///
    /// When `provenance` is `None` (SQL path), behave as before.
    pub(in crate::data::executor) fn execute_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        params: ColumnarInsertParams<'_>,
    ) -> Response {
        let ColumnarInsertParams {
            collection,
            payload,
            format: _format,
            intent,
            on_conflict_updates,
            surrogates,
            schema_bytes,
            provenance,
        } = params;
        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the insert path below.
                }
                non_apply => {
                    let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
        }
        // Parse payload: msgpack-encoded nodedb_types::Value (array or object).
        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if ndb_rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        let engine_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let tid = task.request.tenant_id.as_u64();
        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        // Ensure MutationEngine exists (auto-create on first write). Prefers
        // the DDL schema carried on `schema_bytes` over inference from the
        // payload — inference cannot distinguish JSON columns from plain
        // strings because both arrive as `Value::String`. Shared with the
        // in-transaction staging path so a staged insert into a brand-new
        // collection registers the same schema (see
        // `ensure_columnar_engine_schema` doc comment).
        let schema = self.ensure_columnar_engine_schema(
            &engine_key,
            collection,
            bitemporal,
            &ndb_rows[0],
            schema_bytes,
        );

        let accepted = match self.insert_columnar_rows(
            task,
            RowIngestParams {
                engine_key: &engine_key,
                schema: &schema,
                bitemporal,
                intent,
                on_conflict_updates,
                surrogates,
                ndb_rows: &ndb_rows,
            },
        ) {
            Ok(accepted) => accepted,
            Err(response) => return response,
        };

        if let Err(response) = self.flush_columnar_memtable_if_needed(task, &engine_key, collection)
        {
            return response;
        }

        // Populate R-tree for geometry columns so spatial predicates work.
        self.index_columnar_geometry_columns(task, &schema, collection, &ndb_rows);

        tracing::debug!(
            core = self.core_id,
            %collection,
            accepted,
            total = ndb_rows.len(),
            "columnar insert complete"
        );

        // Invalidate cached aggregate results for this collection so that
        // COUNT(*) and GROUP BY queries see the newly written rows.
        if accepted > 0 {
            self.invalidate_aggregate_cache_for_collection(
                task.request.database_id.as_u64(),
                task.request.tenant_id.as_u64(),
                collection,
            );
        }

        self.checkpoint_coordinator
            .mark_dirty("columnar", accepted as usize);

        // Advance the collection floor for this committed columnar write.
        if accepted > 0 {
            self.note_collection_write_lsn(task, collection);
        }

        // On the sync path, advance the HWM and return SyncAckResult payload.
        if let Some(prov) = provenance {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }

        let result = serde_json::json!({
            "accepted": accepted,
            "collection": collection,
        });
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}

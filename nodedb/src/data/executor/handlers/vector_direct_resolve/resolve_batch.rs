// SPDX-License-Identifier: BUSL-1.1

//! Resolver and live handler for `VectorOp::DirectBatchWrite`.
//!
//! The page is judged row by row, in page order, exactly as one
//! `DirectInsert` / `DirectInsertIfAbsent` / `DirectUpsert` of the page's
//! intent judges its row. A key the page names twice is judged against the
//! body its earlier row produced. Nothing is written while judging. Each key
//! the page writes becomes one `Upsert` mutation carrying the sidecar the
//! key held before the page, so the apply refuses a page that drifted.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{
    ReturningSpec, UpdateValue, VectorDirectRow, VectorDirectWriteIntent, VectorOp,
    VectorResolveOutcome, VectorResolvedMutation,
};
use nodedb_types::{RlsWriteCheck, StorageKey, Surrogate, Value};
use tracing::debug;

use super::apply::{VectorResolvedApplyParams, VectorResolvedIndexSpec};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::{
    VectorRowColumns, vector_stored_rows_payload,
};
use crate::data::executor::handlers::vector_direct_row::{
    VectorDirectIndexSpec, VectorSidecarRow, encode_sidecar,
};
use crate::data::executor::handlers::vector_upsert::{
    VectorUpsertPatch, decode_payload_lowercased, merge_vector_upsert_patch,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// The fields of a `VectorOp::DirectBatchWrite` the resolve reads.
struct BatchPage<'a> {
    collection: &'a str,
    field: &'a str,
    intent: VectorDirectWriteIntent,
    rows: &'a [VectorDirectRow],
    index: VectorResolvedIndexSpec<'a>,
    on_conflict_updates: &'a [(String, UpdateValue)],
    rls_write_check: &'a RlsWriteCheck,
    returning: Option<&'a ReturningSpec>,
    rls_filters: &'a [u8],
}

/// One key the page writes.
struct PageKey<'a> {
    surrogate: Surrogate,
    pk_bytes: &'a [u8],
    /// The vector of the last row that wrote the key.
    vector: &'a [f32],
    /// The key's sidecar fields after every row of the page.
    fields: HashMap<String, Value>,
    /// The sidecar the key held before the page; `None` when unbound.
    old_payload: Option<Vec<u8>>,
}

/// The `DirectBatchWrite` fields, or `None` for any other op.
fn batch_page(op: &VectorOp) -> Option<BatchPage<'_>> {
    let VectorOp::DirectBatchWrite {
        collection,
        field,
        intent,
        rows,
        quantization,
        storage_dtype,
        payload_indexes,
        returning,
        rls_filters,
        on_conflict_updates,
        rls_write_check,
    } = op
    else {
        return None;
    };
    Some(BatchPage {
        collection: collection.as_str(),
        field,
        intent: *intent,
        rows,
        index: VectorResolvedIndexSpec {
            collection: collection.as_str(),
            field,
            quantization: *quantization,
            storage_dtype: *storage_dtype,
            payload_indexes,
        },
        on_conflict_updates,
        rls_write_check,
        returning: returning.as_ref(),
        rls_filters,
    })
}

fn not_a_batch() -> ErrorCode {
    ErrorCode::Internal {
        detail: "vector batch write handler received another vector op".into(),
    }
}

fn duplicate_key(collection: &str) -> ErrorCode {
    ErrorCode::from(crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: "unique".to_string(),
        detail: format!("duplicate key value violates primary-key uniqueness on '{collection}'"),
    })
}

impl CoreLoop {
    /// Run a `DirectBatchWrite` that reached the Data Plane unresolved:
    /// resolve it, then apply the resolution as one resolved write.
    pub(in crate::data::executor) fn execute_vector_direct_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &VectorOp,
    ) -> Response {
        let Some(page) = batch_page(op) else {
            return self.response_error(task, not_a_batch());
        };
        let outcome = match self.resolve_page(task, tid, &page) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        self.execute_vector_resolved_direct_write(VectorResolvedApplyParams {
            task,
            tid,
            index: page.index,
            mutations: &outcome.mutations,
            response_payload: &outcome.response_payload,
            rls_write_check: page.rls_write_check,
        })
    }

    /// Resolve `VectorOp::DirectBatchWrite`. Reads only.
    pub(super) fn resolve_vector_direct_batch(
        &self,
        task: &ExecutionTask,
        tid: u64,
        op: &VectorOp,
    ) -> Result<VectorResolveOutcome, ErrorCode> {
        let page = batch_page(op).ok_or_else(not_a_batch)?;
        self.resolve_page(task, tid, &page)
    }

    fn resolve_page(
        &self,
        task: &ExecutionTask,
        tid: u64,
        page: &BatchPage<'_>,
    ) -> Result<VectorResolveOutcome, ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        let collection = page.collection;
        debug!(
            core = self.core_id,
            %collection,
            count = page.rows.len(),
            intent = ?page.intent,
            "vector resolve direct batch"
        );
        self.check_page_widths(database_id, tid, page)?;
        let probe_key = CoreLoop::vector_index_key(database_id, tid, collection, page.field);
        let patch = VectorUpsertPatch {
            database_id,
            tid,
            collection,
            field: page.field,
            on_conflict_updates: page.on_conflict_updates,
            rls_write_check: page.rls_write_check,
        };
        let mut keys: Vec<PageKey<'_>> = Vec::new();
        let mut slot_of: HashMap<Surrogate, usize> = HashMap::new();
        // One entry per row the page writes: the key's slot, and whether the
        // row found the key present.
        let mut written: Vec<(usize, bool)> = Vec::new();
        for row in page.rows {
            let proposed = decode_proposed(&row.payload)?;
            let earlier = slot_of
                .get(&row.surrogate)
                .copied()
                .and_then(|slot| Some((slot, keys.get_mut(slot)?)));
            if let Some((slot, key)) = earlier {
                match page.intent {
                    VectorDirectWriteIntent::Insert => return Err(duplicate_key(collection)),
                    VectorDirectWriteIntent::InsertIfAbsent => continue,
                    VectorDirectWriteIntent::Upsert => {}
                }
                key.fields = if page.on_conflict_updates.is_empty() {
                    proposed
                } else {
                    let stored = VectorSidecarRow {
                        bytes: encode_sidecar(&key.fields)?,
                        fields: std::mem::take(&mut key.fields),
                    };
                    merge_vector_upsert_patch(&patch, Some(stored), proposed, &self.crs_context())?
                        .fields
                };
                key.vector = &row.vector;
                written.push((slot, true));
                continue;
            }
            let existing = self.vector_direct_node(&probe_key, row.surrogate).is_some();
            let (fields, old_payload) = match page.intent {
                VectorDirectWriteIntent::Insert if existing => {
                    return Err(duplicate_key(collection));
                }
                // The key stays as stored, so a later row naming it is
                // judged against the stored row again.
                VectorDirectWriteIntent::InsertIfAbsent if existing => continue,
                VectorDirectWriteIntent::Upsert
                    if existing && !page.on_conflict_updates.is_empty() =>
                {
                    let planned = self.plan_vector_upsert_patch(&patch, row.surrogate, proposed)?;
                    (
                        planned.fields,
                        Some(planned.old_sidecar.unwrap_or_default()),
                    )
                }
                // A bound node with no sidecar row reads as an empty
                // sidecar, which no encoded one is.
                VectorDirectWriteIntent::Upsert if existing => {
                    let stored = self
                        .vector_sidecar_bytes(database_id, tid, collection, row.surrogate)?
                        .unwrap_or_default();
                    (proposed, Some(stored))
                }
                VectorDirectWriteIntent::Insert
                | VectorDirectWriteIntent::InsertIfAbsent
                | VectorDirectWriteIntent::Upsert => (proposed, None),
            };
            slot_of.insert(row.surrogate, keys.len());
            written.push((keys.len(), existing));
            keys.push(PageKey {
                surrogate: row.surrogate,
                pk_bytes: &row.pk_bytes,
                vector: &row.vector,
                fields,
                old_payload,
            });
        }

        let sidecars: Vec<Vec<u8>> = keys
            .iter()
            .map(|key| encode_sidecar(&key.fields))
            .collect::<Result<_, _>>()?;
        let response_payload =
            self.page_reply(database_id, tid, page, &keys, &sidecars, &written)?;
        let mutations = keys
            .into_iter()
            .zip(sidecars)
            .map(|(key, payload)| VectorResolvedMutation::Upsert {
                surrogate: key.surrogate,
                pk_bytes: key.pk_bytes.to_vec(),
                vector: key.vector.to_vec(),
                payload,
                old_payload: key.old_payload,
            })
            .collect();
        Ok(VectorResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Every vector of the page fits the index it lands in. An absent index
    /// takes the width of the first vector, so every vector shares it.
    fn check_page_widths(
        &self,
        database_id: u64,
        tid: u64,
        page: &BatchPage<'_>,
    ) -> Result<(), ErrorCode> {
        let mut width: Option<usize> = None;
        for row in page.rows {
            let dim = row.vector.len();
            self.check_vector_direct_index(
                database_id,
                tid,
                &VectorDirectIndexSpec {
                    collection: page.collection,
                    field: page.field,
                    dim,
                    quantization: page.index.quantization,
                    storage_dtype: page.index.storage_dtype,
                    payload_indexes: page.index.payload_indexes,
                },
            )?;
            match width {
                Some(first) if first != dim => {
                    return Err(super::super::vector::dimension_mismatch(first, dim));
                }
                Some(_) => {}
                None => width = Some(dim),
            }
        }
        Ok(())
    }

    /// The reply the page owes: one `RETURNING` row per row written, each
    /// its key's stored post-image, or the count of rows written.
    fn page_reply(
        &self,
        database_id: u64,
        tid: u64,
        page: &BatchPage<'_>,
        keys: &[PageKey<'_>],
        sidecars: &[Vec<u8>],
        written: &[(usize, bool)],
    ) -> Result<Vec<u8>, ErrorCode> {
        let count = written.len() as u64;
        if let Some(spec) = page.returning {
            let storage_keys: Vec<StorageKey> = keys
                .iter()
                .map(|key| StorageKey::for_surrogate(key.surrogate))
                .collect();
            let rows: Vec<(&StorageKey, &[u8], &[f32])> = written
                .iter()
                .filter_map(|&(slot, _)| {
                    Some((
                        storage_keys.get(slot)?,
                        sidecars.get(slot)?.as_slice(),
                        keys.get(slot)?.vector,
                    ))
                })
                .collect();
            let identity_column = self.identity_column(database_id, tid, page.collection);
            return vector_stored_rows_payload(
                spec,
                page.rls_filters,
                VectorRowColumns {
                    identity: &identity_column,
                    vector: page.field,
                },
                &rows,
                &self.crs_context(),
            )
            .map_err(ErrorCode::from);
        }
        if page.on_conflict_updates.is_empty() {
            return Ok(response_codec::encode_affected(count));
        }
        // A statement that both inserts and updates reports `insert`, as
        // the per-row tags fold.
        let all_updated = !written.is_empty() && written.iter().all(|&(_, updated)| updated);
        let op = if all_updated { "update" } else { "insert" };
        Ok(response_codec::encode_affected_with_op(count, op))
    }
}

/// A row's proposed fields, lower-cased. An empty payload is a row that
/// carried no non-vector column.
fn decode_proposed(payload: &[u8]) -> Result<HashMap<String, Value>, ErrorCode> {
    if payload.is_empty() {
        return Ok(HashMap::new());
    }
    decode_payload_lowercased(payload).map_err(|e| ErrorCode::Internal {
        detail: format!("payload decode error: {e}"),
    })
}

// SPDX-License-Identifier: BUSL-1.1

//! KV batch write handler: BatchPut.

use std::borrow::Cow;

use nodedb_physical::physical_plan::KvBatchIntent;
use tracing::debug;

use super::batch_plan::{PlannedKvRow, merge_batch_verb, plan_kv_batch};
use super::rls::admit_kv_row;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvBatchPutParams;

/// Parameters for [`CoreLoop::execute_kv_batch_put`].
///
/// Bundles the plain data args so the method stays argument-count clean;
/// `task` (response routing) is passed separately since it isn't part of
/// the KV batch-put payload.
pub(in crate::data::executor) struct KvBatchPutArgs<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
    pub ttl_ms: u64,
    /// What a key already stored, or repeated in `entries`, does.
    pub intent: &'a KvBatchIntent,
    pub surrogates: &'a [nodedb_types::Surrogate],
    /// When `Some`, return one row per written entry, in `entries` order.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

/// One `(key, value)` pair of a batch put.
type KvEntry = (Vec<u8>, Vec<u8>);

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_batch_put(
        &mut self,
        task: &ExecutionTask,
        args: KvBatchPutArgs<'_>,
    ) -> Response {
        let KvBatchPutArgs {
            did,
            tid,
            collection,
            entries,
            ttl_ms,
            intent,
            surrogates,
            returning,
            rls_filters,
        } = args;
        debug!(core = self.core_id, %collection, count = entries.len(), "kv batch put");

        // Memory budget check: reject new writes when over budget, as the
        // point writes do.
        if self.kv_engine.is_over_budget() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "KV memory budget exceeded, retry later".into(),
                },
            );
        }

        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms: u64 = self.kv_ttl_now_ms(task);
        let rows = match plan_kv_batch(
            collection,
            entries,
            intent,
            self.declared_columns_of(did, tid, collection),
            &self.crs_context(),
            |key| self.kv_engine.get(did, tid, collection, key, now_ms),
        ) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        if let KvBatchIntent::Merge {
            rls_write_check, ..
        } = intent
        {
            // The policy decides each body that persists: the entry when the
            // key was absent, the merge when it was stored.
            for row in &rows {
                if let Err(e) = admit_kv_row(
                    rls_write_check,
                    &row.stored,
                    row.key,
                    tid,
                    collection,
                    &self.crs_context(),
                ) {
                    return self.response_error(task, e);
                }
            }
        }

        // The entries the batch writes, with their surrogates: the batch
        // itself when every entry stores verbatim.
        let written: Vec<KvEntry>;
        let written_surrogates: Vec<nodedb_types::Surrogate>;
        let verbatim = rows.len() == entries.len()
            && rows
                .iter()
                .all(|row| matches!(row.stored, Cow::Borrowed(_)));
        let (write_entries, write_surrogates) = if verbatim {
            (entries, surrogates)
        } else {
            written = rows
                .iter()
                .map(|row| (row.key.to_vec(), row.stored.to_vec()))
                .collect();
            written_surrogates = rows
                .iter()
                .filter_map(|row| surrogates.get(row.index).copied())
                .collect();
            (written.as_slice(), written_surrogates.as_slice())
        };
        let new_count = match self.kv_engine.batch_put(KvBatchPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            entries: write_entries,
            ttl_ms,
            now_ms,
            surrogates: write_surrogates,
        }) {
            Ok(count) => count,
            Err(e) => return self.response_error(task, e),
        };
        if let Some(ref m) = self.metrics {
            for _ in &rows {
                m.record_kv_put();
            }
        }
        // One write event per written row: a batch INSERT fires the same row
        // triggers as the single-row form.
        for row in &rows {
            let op = if row.updated() {
                crate::event::WriteOp::Update
            } else {
                crate::event::WriteOp::Insert
            };
            self.emit_kv_write_event(
                task,
                collection,
                op,
                row.key,
                Some(row.stored.as_ref()),
                row.prior.as_deref(),
            );
        }
        // One WAL record covers the whole batch; record every written key's
        // version against that single LSN.
        if task.wal_lsn().is_some() {
            for row in &rows {
                self.note_kv_write(task, did, tid, collection, row.key);
            }
        }
        if let Some(spec) = returning {
            // One row per written row, in `entries` order — the order the
            // statement listed them, which is the order PostgreSQL returns
            // them in. Each projects the body it stored.
            let projected = stored_rows(&rows);
            return self.kv_stored_returning_response(task, spec, rls_filters, &projected);
        }
        match intent {
            KvBatchIntent::Put => match response_codec::encode_count("inserted", new_count) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            },
            // One count per row written, as one point op per row reports.
            KvBatchIntent::Upsert | KvBatchIntent::Insert | KvBatchIntent::InsertIfAbsent => {
                self.response_affected(task, rows.len() as u64)
            }
            KvBatchIntent::Merge { .. } => {
                self.response_affected_with_op(task, rows.len() as u64, merge_batch_verb(&rows))
            }
        }
    }
}

/// The `(key, stored body)` of every planned row, for a `RETURNING`
/// projection.
pub(in crate::data::executor) fn stored_rows<'r>(
    rows: &'r [PlannedKvRow<'_>],
) -> Vec<crate::data::executor::handlers::returning_rows::KvStoredRow<'r>> {
    rows.iter().map(|row| (row.key, &*row.stored)).collect()
}

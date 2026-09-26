// SPDX-License-Identifier: BUSL-1.1

//! KV batch operation handlers: BatchGet, BatchPut.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvBatchPutParams;
use crate::engine::kv::current_ms;

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
    pub surrogates: &'a [nodedb_types::Surrogate],
    /// When `Some`, return one row per written entry, in `entries` order.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_batch_get(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        keys: &[Vec<u8>],
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv batch get");
        let now_ms = current_ms();

        // Read-your-own-writes: consult this transaction's staging overlay
        // per key before falling back to base storage. `kv_overlay_body`
        // returns `None` immediately (no txn, or no overlay entry for this
        // key) so an autocommit `BatchGet` takes the exact base-only path it
        // always has.
        let results: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(
                |key| match self.kv_overlay_body(task, tid, collection, key) {
                    Some(overlay_result) => overlay_result,
                    None => self.kv_engine.get(did, tid, collection, key, now_ms),
                },
            )
            .collect();

        // A row the read policy excludes reads as absent — indistinguishable
        // from a missing key, so a caller cannot probe for keys it may not read.
        let mut json_results: Vec<serde_json::Value> = Vec::with_capacity(results.len());
        for opt in results {
            let entry = match opt {
                Some(v) => match self.row_passes_rls(&v, rls_filters) {
                    Ok(true) => serde_json::Value::String(base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        &v,
                    )),
                    Ok(false) => serde_json::Value::Null,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                },
                None => serde_json::Value::Null,
            };
            json_results.push(entry);
        }
        match response_codec::encode_json_vec_as_msgpack(&json_results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

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
            surrogates,
            returning,
            rls_filters,
        } = args;
        debug!(core = self.core_id, %collection, count = entries.len(), "kv batch put");
        // See `CoreLoop::kv_ttl_now_ms` for the precedence this resolves.
        let now_ms: u64 = self.kv_ttl_now_ms(task);
        // Each entry's pre-image, in `entries` order, for its write event. A
        // key the batch names twice sees the batch's own earlier value.
        let priors: Vec<Option<Vec<u8>>> = {
            let mut written: std::collections::HashMap<&[u8], &[u8]> =
                std::collections::HashMap::with_capacity(entries.len());
            entries
                .iter()
                .map(|(key, value)| {
                    let prior = match written.get(key.as_slice()) {
                        Some(earlier) => Some(earlier.to_vec()),
                        None => self.kv_engine.get(did, tid, collection, key, now_ms),
                    };
                    written.insert(key.as_slice(), value.as_slice());
                    prior
                })
                .collect()
        };
        let new_count = self.kv_engine.batch_put(KvBatchPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        });
        // One write event per entry: a batch INSERT fires the same row
        // triggers as the single-row form.
        for ((key, value), prior) in entries.iter().zip(&priors) {
            let op = if prior.is_some() {
                crate::event::WriteOp::Update
            } else {
                crate::event::WriteOp::Insert
            };
            self.emit_kv_write_event(task, collection, op, key, Some(value), prior.as_deref());
        }
        // One WAL record covers the whole batch; record every written key's
        // version against that single LSN.
        if task.wal_lsn().is_some() {
            for (key, _) in entries {
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
        }
        if let Some(spec) = returning {
            // One row per entry, in `entries` order — the order the statement
            // listed them, which is the order PostgreSQL returns them in. The
            // batch writes each value verbatim, so the entry bytes ARE the
            // stored post-images.
            let rows: Vec<crate::data::executor::handlers::returning_rows::KvStoredRow<'_>> =
                entries
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice()))
                    .collect();
            return self.kv_stored_returning_response(task, spec, rls_filters, &rows);
        }
        match response_codec::encode_count("inserted", new_count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

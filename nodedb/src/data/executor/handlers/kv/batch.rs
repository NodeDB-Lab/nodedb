// SPDX-License-Identifier: BUSL-1.1

//! KV batch read handler: BatchGet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Arguments for [`CoreLoop::execute_kv_batch_get`].
pub(in crate::data::executor) struct KvBatchGetArgs<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub keys: &'a [Vec<u8>],
    pub rls_filters: &'a [u8],
    /// The system-time cut every base read answers at.
    pub system_time: nodedb_types::SystemTimeScope,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_batch_get(
        &self,
        task: &ExecutionTask,
        args: KvBatchGetArgs<'_>,
    ) -> Response {
        let KvBatchGetArgs {
            did,
            tid,
            collection,
            keys,
            rls_filters,
            system_time,
        } = args;
        debug!(core = self.core_id, %collection, count = keys.len(), "kv batch get");
        let now_ms = current_ms();

        // Read-your-own-writes: consult this transaction's staging overlay
        // per key before falling back to base storage. `kv_overlay_body`
        // returns `None` immediately (no txn, or no overlay entry for this
        // key) so an autocommit `BatchGet` takes the exact base-only path it
        // always has.
        let results: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|key| {
                match system_time
                    .merges_txn_overlay()
                    .then(|| self.kv_overlay_body(task, tid, collection, key))
                    .flatten()
                {
                    Some(overlay_result) => overlay_result,
                    None => self
                        .kv_engine
                        .get_at(did, tid, collection, key, system_time, now_ms)
                        .map(|(value, _)| value),
                }
            })
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
                        return self.response_error(task, ErrorCode::from(e));
                    }
                },
                None => serde_json::Value::Null,
            };
            json_results.push(entry);
        }
        match response_codec::encode_json_vec_as_msgpack(&json_results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

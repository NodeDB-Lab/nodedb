// SPDX-License-Identifier: BUSL-1.1

//! KV sorted index (leaderboard) handlers.

use tracing::debug;

use super::sorted_index_compute::{BuildSortedIndexDefParams, build_sorted_index_def};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Parameters for `execute_kv_register_sorted_index`.
pub(in crate::data::executor) struct KvRegisterSortedIndexParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub index_name: &'a str,
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

/// Parameters for `execute_kv_sorted_index_range`.
pub(in crate::data::executor) struct KvSortedIndexRangeParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub index_name: &'a str,
    pub score_min: Option<&'a [u8]>,
    pub score_max: Option<&'a [u8]>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_register_sorted_index(
        &mut self,
        task: &ExecutionTask,
        params: KvRegisterSortedIndexParams<'_>,
    ) -> Response {
        let KvRegisterSortedIndexParams {
            did,
            tid,
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } = params;
        debug!(core = self.core_id, %collection, %index_name, "kv register sorted index");

        let def = match build_sorted_index_def(BuildSortedIndexDefParams {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        }) {
            Ok(def) => def,
            Err(e) => return self.response_error(task, e),
        };

        let backfilled = self
            .kv_engine
            .register_sorted_index(did, tid, collection, def);

        let result = serde_json::json!({
            "index": index_name,
            "backfilled": backfilled,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    pub(in crate::data::executor) fn execute_kv_drop_sorted_index(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv drop sorted index");

        if self.kv_engine.drop_sorted_index(did, tid, index_name) {
            let result = serde_json::json!({ "dropped": index_name });
            match response_codec::encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, e),
            }
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_rank(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        primary_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index rank");
        let now_ms = current_ms();

        let rank = self
            .kv_engine
            .sorted_index_rank(did, tid, index_name, primary_key, now_ms);
        self.sorted_rank_response(task, rank)
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_top_k(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        k: u32,
    ) -> Response {
        debug!(core = self.core_id, %index_name, k, "kv sorted index top_k");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_top_k(did, tid, index_name, k, now_ms)
        {
            Some(entries) => self.sorted_rows_response(task, entries),
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_range(
        &self,
        task: &ExecutionTask,
        params: KvSortedIndexRangeParams<'_>,
    ) -> Response {
        let KvSortedIndexRangeParams {
            did,
            tid,
            index_name,
            score_min,
            score_max,
        } = params;
        debug!(core = self.core_id, %index_name, "kv sorted index range");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_range(crate::engine::kv::SortedIndexRangeParams {
                database_id: did,
                tenant_id: tid,
                index_name,
                score_min,
                score_max,
                now_ms,
            }) {
            Some(entries) => self.sorted_rows_response(task, entries),
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_count(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index count");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_count(did, tid, index_name, now_ms)
        {
            Some(count) => self.sorted_count_response(task, count),
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_score(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        primary_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index score");

        let score = self
            .kv_engine
            .sorted_index_score(did, tid, index_name, primary_key);
        self.sorted_score_response(task, score)
    }
}

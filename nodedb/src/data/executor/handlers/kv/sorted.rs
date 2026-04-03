//! KV sorted index (leaderboard) handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::engine::kv::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
use crate::engine::kv::sorted_index::manager::SortedIndexDef;
use crate::engine::kv::sorted_index::window::WindowConfig;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_kv_register_sorted_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        index_name: &str,
        sort_columns: &[(String, String)],
        key_column: &str,
        window_type: &str,
        window_timestamp_column: &str,
        window_start_ms: u64,
        window_end_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, %index_name, "kv register sorted index");

        let columns: Vec<SortColumn> = sort_columns
            .iter()
            .map(|(name, dir)| SortColumn {
                name: name.clone(),
                direction: if dir.eq_ignore_ascii_case("DESC") {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
            })
            .collect();

        let window = match window_type.to_uppercase().as_str() {
            "DAILY" => WindowConfig::daily(window_timestamp_column),
            "WEEKLY" => WindowConfig::weekly(window_timestamp_column),
            "MONTHLY" => WindowConfig::monthly(window_timestamp_column),
            "CUSTOM" => {
                WindowConfig::custom(window_timestamp_column, window_start_ms, window_end_ms)
            }
            _ => WindowConfig::none(),
        };

        let def = SortedIndexDef {
            name: index_name.to_string(),
            collection: collection.to_string(),
            key_column: key_column.to_string(),
            encoder: SortKeyEncoder::new(columns),
            window,
        };

        let backfilled = self.kv_engine.register_sorted_index(tid, collection, def);

        let payload = serde_json::json!({
            "index": index_name,
            "backfilled": backfilled,
        })
        .to_string()
        .into_bytes();
        self.response_with_payload(task, payload)
    }

    pub(in crate::data::executor) fn execute_kv_drop_sorted_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv drop sorted index");

        if self.kv_engine.drop_sorted_index(tid, index_name) {
            let payload = serde_json::json!({ "dropped": index_name })
                .to_string()
                .into_bytes();
            self.response_with_payload(task, payload)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_rank(
        &self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
        primary_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index rank");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_rank(tid, index_name, primary_key, now_ms)
        {
            Some(rank) => {
                let payload = serde_json::json!({ "rank": rank }).to_string().into_bytes();
                self.response_with_payload(task, payload)
            }
            None => {
                let payload = serde_json::json!({ "rank": null }).to_string().into_bytes();
                self.response_with_payload(task, payload)
            }
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_top_k(
        &self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
        k: u32,
    ) -> Response {
        debug!(core = self.core_id, %index_name, k, "kv sorted index top_k");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_top_k(tid, index_name, k, now_ms)
        {
            Some(entries) => {
                let rows: Vec<serde_json::Value> = entries
                    .into_iter()
                    .map(|(rank, pk)| {
                        serde_json::json!({
                            "rank": rank,
                            "key": String::from_utf8_lossy(&pk),
                        })
                    })
                    .collect();
                let payload = serde_json::to_vec(&rows).unwrap_or_default();
                self.response_with_payload(task, payload)
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_range(
        &self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index range");
        let now_ms = current_ms();

        match self
            .kv_engine
            .sorted_index_range(tid, index_name, score_min, score_max, now_ms)
        {
            Some(entries) => {
                let rows: Vec<serde_json::Value> = entries
                    .into_iter()
                    .map(|(rank, pk)| {
                        serde_json::json!({
                            "rank": rank,
                            "key": String::from_utf8_lossy(&pk),
                        })
                    })
                    .collect();
                let payload = serde_json::to_vec(&rows).unwrap_or_default();
                self.response_with_payload(task, payload)
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_count(
        &self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index count");
        let now_ms = current_ms();

        match self.kv_engine.sorted_index_count(tid, index_name, now_ms) {
            Some(count) => {
                let payload = serde_json::json!({ "count": count })
                    .to_string()
                    .into_bytes();
                self.response_with_payload(task, payload)
            }
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_kv_sorted_index_score(
        &self,
        task: &ExecutionTask,
        tid: u32,
        index_name: &str,
        primary_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %index_name, "kv sorted index score");

        match self
            .kv_engine
            .sorted_index_score(tid, index_name, primary_key)
        {
            Some(sort_key) => {
                let b64 =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &sort_key);
                let payload = serde_json::json!({ "score": b64 }).to_string().into_bytes();
                self.response_with_payload(task, payload)
            }
            None => {
                let payload = serde_json::json!({ "score": null })
                    .to_string()
                    .into_bytes();
                self.response_with_payload(task, payload)
            }
        }
    }
}

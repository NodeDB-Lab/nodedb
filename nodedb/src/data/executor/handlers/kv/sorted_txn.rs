// SPDX-License-Identifier: BUSL-1.1

//! Sorted-index reads inside an explicit transaction.
//!
//! A transaction must see its own DDL and its own writes. An index it created
//! has no tree on this core before COMMIT. A committed index's tree holds none
//! of its staged writes. Either way the read runs over a transaction-local
//! tree, built from the collection's base rows with the transaction's staged
//! writes folded in. A committed index the transaction staged nothing for
//! answers from its registered tree, which already holds the same rows.

use tracing::debug;

use nodedb_physical::physical_plan::{SortedIndexRead, SortedIndexSpec};

use super::sorted_index_compute::{BuildSortedIndexDefParams, build_sorted_index_def};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::engine::kv::sorted_index::SortedIndex;
use crate::engine::kv::sorted_index::manager::SortedIndexDef;
use crate::types::{DatabaseId, TenantId};

/// Parameters for `execute_kv_sorted_index_txn_read`.
pub(in crate::data::executor) struct SortedIndexTxnReadParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub index_name: &'a str,
    pub pending: Option<&'a SortedIndexSpec>,
    pub read: &'a SortedIndexRead,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_sorted_index_txn_read(
        &self,
        task: &ExecutionTask,
        params: SortedIndexTxnReadParams<'_>,
    ) -> Response {
        let SortedIndexTxnReadParams {
            did,
            tid,
            collection,
            index_name,
            pending,
            read,
        } = params;
        debug!(core = self.core_id, %collection, %index_name, "kv sorted index txn read");
        let now_ms = current_ms();
        let coll_key = (
            DatabaseId::new(did),
            TenantId::new(tid),
            collection.to_string(),
        );
        let txn_id = task.request.txn_id;
        let staged = txn_id.is_some_and(|txn_id| {
            self.txn_overlays
                .get(&txn_id)
                .is_some_and(|overlay| overlay.stages_collection(&coll_key))
        });

        let def = match pending {
            Some(spec) => match pending_def(collection, index_name, spec) {
                Ok(def) => def,
                Err(e) => return self.response_error(task, e),
            },
            None if !staged => {
                return self.answer_from_registered(task, did, tid, index_name, read, now_ms);
            }
            None => match self.kv_engine.sorted_index_def(did, tid, index_name) {
                Some(def) => def.clone(),
                None => return self.response_error(task, ErrorCode::NotFound),
            },
        };

        let mut rows = self.kv_engine.collection_rows(did, tid, collection, now_ms);
        if let Some(txn_id) = txn_id {
            self.merge_kv_overlay_into_scan(
                txn_id,
                &coll_key,
                &mut rows,
                &|_key: &[u8], _value: &[u8]| true,
            );
        }
        let (index, _) = SortedIndex::build(def, rows.into_iter());

        match read {
            SortedIndexRead::Rank { primary_key } => {
                self.sorted_rank_response(task, index.rank(primary_key, now_ms))
            }
            SortedIndexRead::TopK { k } => self.sorted_rows_response(task, index.top_k(*k, now_ms)),
            SortedIndexRead::Range {
                score_min,
                score_max,
            } => self.sorted_rows_response(
                task,
                index.range(score_min.as_deref(), score_max.as_deref(), now_ms),
            ),
            SortedIndexRead::Count => self.sorted_count_response(task, index.count(now_ms)),
            SortedIndexRead::Score { primary_key } => {
                self.sorted_score_response(task, index.score(primary_key))
            }
        }
    }

    /// Answer from the tree registered on this core. The transaction staged
    /// nothing for the collection, so that tree holds exactly its view.
    fn answer_from_registered(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        read: &SortedIndexRead,
        now_ms: u64,
    ) -> Response {
        let engine = &self.kv_engine;
        match read {
            SortedIndexRead::Rank { primary_key } => {
                if engine.sorted_index_def(did, tid, index_name).is_none() {
                    return self.response_error(task, ErrorCode::NotFound);
                }
                let rank = engine.sorted_index_rank(did, tid, index_name, primary_key, now_ms);
                self.sorted_rank_response(task, rank)
            }
            SortedIndexRead::TopK { k } => {
                match engine.sorted_index_top_k(did, tid, index_name, *k, now_ms) {
                    Some(entries) => self.sorted_rows_response(task, entries),
                    None => self.response_error(task, ErrorCode::NotFound),
                }
            }
            SortedIndexRead::Range {
                score_min,
                score_max,
            } => match engine.sorted_index_range(crate::engine::kv::SortedIndexRangeParams {
                database_id: did,
                tenant_id: tid,
                index_name,
                score_min: score_min.as_deref(),
                score_max: score_max.as_deref(),
                now_ms,
            }) {
                Some(entries) => self.sorted_rows_response(task, entries),
                None => self.response_error(task, ErrorCode::NotFound),
            },
            SortedIndexRead::Count => {
                match engine.sorted_index_count(did, tid, index_name, now_ms) {
                    Some(count) => self.sorted_count_response(task, count),
                    None => self.response_error(task, ErrorCode::NotFound),
                }
            }
            SortedIndexRead::Score { primary_key } => {
                if engine.sorted_index_def(did, tid, index_name).is_none() {
                    return self.response_error(task, ErrorCode::NotFound);
                }
                let score = engine.sorted_index_score(did, tid, index_name, primary_key);
                self.sorted_score_response(task, score)
            }
        }
    }

    /// `{"rank": n}`, or `{"rank": null}` for a key the index does not hold.
    pub(in crate::data::executor) fn sorted_rank_response(
        &self,
        task: &ExecutionTask,
        rank: Option<u32>,
    ) -> Response {
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "rank": rank })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// One `{"rank", "key"}` row per `(rank, primary_key)` entry.
    pub(in crate::data::executor) fn sorted_rows_response(
        &self,
        task: &ExecutionTask,
        entries: Vec<(u32, Vec<u8>)>,
    ) -> Response {
        let rows: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|(rank, pk)| {
                serde_json::json!({
                    "rank": rank,
                    "key": String::from_utf8_lossy(&pk),
                })
            })
            .collect();
        match response_codec::encode_json_vec_as_msgpack(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// `{"score": base64}` of the sort key, or `{"score": null}` for a key the
    /// index does not hold.
    pub(in crate::data::executor) fn sorted_score_response(
        &self,
        task: &ExecutionTask,
        sort_key: Option<Vec<u8>>,
    ) -> Response {
        let score = sort_key.map(|sort_key| {
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &sort_key)
        });
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "score": score })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// `{"count": n}`.
    pub(in crate::data::executor) fn sorted_count_response(
        &self,
        task: &ExecutionTask,
        count: u32,
    ) -> Response {
        match response_codec::encode_count("count", count as usize) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}

/// The definition of an index the transaction created, built by the same code
/// that builds a registered one.
fn pending_def(
    collection: &str,
    index_name: &str,
    spec: &SortedIndexSpec,
) -> Result<SortedIndexDef, ErrorCode> {
    build_sorted_index_def(BuildSortedIndexDefParams {
        collection,
        index_name,
        sort_columns: &spec.sort_columns,
        key_column: &spec.key_column,
        window_type: &spec.window_type,
        window_timestamp_column: &spec.window_timestamp_column,
        window_start_ms: spec.window_start_ms,
        window_end_ms: spec.window_end_ms,
    })
}

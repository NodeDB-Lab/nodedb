// SPDX-License-Identifier: BUSL-1.1

//! Sorted-index reads outside a transaction, at their system-time scope.
//!
//! A read with no cut (`Current`, `AllVersions`) answers from the tree
//! registered on this core, which ranks the newest version of every row. A
//! read at a cut (`AsOf`, `Pinned`) answers from a tree built from the rows
//! visible at the cut, so it ranks exactly the rows a point read at that cut
//! sees. Expiry and time windows are judged at the cut's wall time.

use tracing::debug;

use nodedb_physical::physical_plan::{KvOp, SortedIndexRead};

use super::sorted::KvSortedIndexRangeParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvEngine;
use crate::engine::kv::sorted_index::SortedIndex;

/// One sorted-index read at the ordinal `at`.
struct SortedReadAt<'a> {
    did: u64,
    tid: u64,
    index_name: &'a str,
    read: SortedIndexRead,
    at: i64,
}

impl CoreLoop {
    /// Answer the sorted-index read `op` at its system-time scope. `op` is
    /// one of `SortedIndexRank`, `SortedIndexTopK`, `SortedIndexRange`,
    /// `SortedIndexCount`, `SortedIndexScore`.
    pub(super) fn dispatch_kv_sorted_read(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let (index_name, read, system_time) = match op {
            KvOp::SortedIndexRank {
                index_name,
                primary_key,
                system_time,
            } => (
                index_name,
                SortedIndexRead::Rank {
                    primary_key: primary_key.clone(),
                },
                system_time,
            ),
            KvOp::SortedIndexTopK {
                index_name,
                k,
                system_time,
            } => (index_name, SortedIndexRead::TopK { k: *k }, system_time),
            KvOp::SortedIndexRange {
                index_name,
                score_min,
                score_max,
                system_time,
            } => (
                index_name,
                SortedIndexRead::Range {
                    score_min: score_min.clone(),
                    score_max: score_max.clone(),
                },
                system_time,
            ),
            KvOp::SortedIndexCount {
                index_name,
                system_time,
            } => (index_name, SortedIndexRead::Count, system_time),
            KvOp::SortedIndexScore {
                index_name,
                primary_key,
                system_time,
            } => (
                index_name,
                SortedIndexRead::Score {
                    primary_key: primary_key.clone(),
                },
                system_time,
            ),
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "a KV op that is no sorted-index read reached the sorted-index \
                                 read dispatch"
                            .into(),
                    },
                );
            }
        };
        match system_time.as_of_ordinal() {
            Some(at) => self.sorted_read_at(
                task,
                SortedReadAt {
                    did,
                    tid,
                    index_name,
                    read,
                    at,
                },
            ),
            None => self.sorted_read_current(task, did, tid, index_name, read),
        }
    }

    /// Answer `read` from the tree registered on this core.
    fn sorted_read_current(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        index_name: &str,
        read: SortedIndexRead,
    ) -> Response {
        match read {
            SortedIndexRead::Rank { primary_key } => {
                self.execute_kv_sorted_index_rank(task, did, tid, index_name, &primary_key)
            }
            SortedIndexRead::TopK { k } => {
                self.execute_kv_sorted_index_top_k(task, did, tid, index_name, k)
            }
            SortedIndexRead::Range {
                score_min,
                score_max,
            } => self.execute_kv_sorted_index_range(
                task,
                KvSortedIndexRangeParams {
                    did,
                    tid,
                    index_name,
                    score_min: score_min.as_deref(),
                    score_max: score_max.as_deref(),
                },
            ),
            SortedIndexRead::Count => {
                self.execute_kv_sorted_index_count(task, did, tid, index_name)
            }
            SortedIndexRead::Score { primary_key } => {
                self.execute_kv_sorted_index_score(task, did, tid, index_name, &primary_key)
            }
        }
    }

    /// Answer a read at ordinal `at` from a tree built from the rows of the
    /// index's collection visible at `at`. `NotFound` when no index of that
    /// name is registered.
    fn sorted_read_at(&self, task: &ExecutionTask, params: SortedReadAt<'_>) -> Response {
        let SortedReadAt {
            did,
            tid,
            index_name,
            read,
            at,
        } = params;
        debug!(core = self.core_id, %index_name, at, "kv sorted index read at a cut");
        let Some(def) = self.kv_engine.sorted_index_def(did, tid, index_name) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        let rows = self
            .kv_engine
            .collection_rows_at(did, tid, &def.collection, at);
        let (index, _) = SortedIndex::build(def.clone(), rows.into_iter());
        let wall_ms = KvEngine::cut_wall_ms(at);
        match read {
            SortedIndexRead::Rank { primary_key } => {
                self.sorted_rank_response(task, index.rank(&primary_key, wall_ms))
            }
            SortedIndexRead::TopK { k } => self.sorted_rows_response(task, index.top_k(k, wall_ms)),
            SortedIndexRead::Range {
                score_min,
                score_max,
            } => self.sorted_rows_response(
                task,
                index.range(score_min.as_deref(), score_max.as_deref(), wall_ms),
            ),
            SortedIndexRead::Count => self.sorted_count_response(task, index.count(wall_ms)),
            SortedIndexRead::Score { primary_key } => {
                self.sorted_score_response(task, index.score(&primary_key))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::SystemTimeScope;

    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::kv::KvPutParams;
    use crate::engine::kv::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
    use crate::engine::kv::sorted_index::manager::SortedIndexDef;
    use crate::engine::kv::sorted_index::window::WindowConfig;
    use crate::engine::kv::test_support::row_surrogate;

    fn scored(player_id: &str, score: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({
            "player_id": player_id,
            "score": score,
        }))
        .expect("encode leaderboard row")
    }

    fn put_at(core: &mut CoreLoop, player_id: &str, score: i64, ordinal: i64) {
        let guard = core.kv_engine.open_write_scope(Some(ordinal));
        core.kv_engine
            .put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "players",
                key: player_id.as_bytes(),
                value: &scored(player_id, score),
                ttl_ms: 0,
                now_ms: 1_000,
                surrogate: row_surrogate(player_id.as_bytes()),
            })
            .expect("a bound row writes");
        core.kv_engine.close_write_scope(guard);
    }

    fn pinned(ordinal: i64) -> SystemTimeScope {
        SystemTimeScope::Pinned {
            ordinal,
            cut_marker: 0,
        }
    }

    /// The ranked keys a top-k read answers, as `(rank, key)` pairs.
    fn top_k(core: &CoreLoop, system_time: SystemTimeScope) -> Vec<(u64, String)> {
        let response = core.dispatch_kv_sorted_read(
            &make_default_task(),
            0,
            1,
            &KvOp::SortedIndexTopK {
                index_name: "lb".into(),
                k: 10,
                system_time,
            },
        );
        let docs = crate::data::executor::response_codec::decode_response_to_docs(&response)
            .expect("rows decode");
        docs.into_iter()
            .map(|(_, bytes)| {
                let row = nodedb_types::value_from_msgpack(&bytes).expect("row decodes");
                let nodedb_types::Value::Object(map) = row else {
                    panic!("a ranked row is a map: {row:?}");
                };
                let rank = match map.get("rank") {
                    Some(nodedb_types::Value::Integer(rank)) => *rank as u64,
                    other => panic!("rank: {other:?}"),
                };
                let key = match map.get("key") {
                    Some(nodedb_types::Value::String(key)) => key.clone(),
                    other => panic!("key: {other:?}"),
                };
                (rank, key)
            })
            .collect()
    }

    /// A top-k read at a cut ranks the rows visible at the cut. A current
    /// read ranks the newest version of every row.
    #[test]
    fn a_sorted_index_read_at_a_cut_ranks_the_rows_visible_there() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.kv_engine.register_sorted_index(
            0,
            1,
            "players",
            SortedIndexDef {
                name: "lb".into(),
                collection: "players".into(),
                key_column: "player_id".into(),
                encoder: SortKeyEncoder::new(vec![SortColumn {
                    name: "score".into(),
                    direction: SortDirection::Desc,
                }]),
                window: WindowConfig::none(),
            },
        );
        put_at(&mut core, "p1", 10, 100);
        put_at(&mut core, "p2", 20, 100);
        // p1 overtakes p2, and p3 joins, after the cut at 150.
        put_at(&mut core, "p1", 30, 200);
        put_at(&mut core, "p3", 5, 200);

        assert_eq!(
            top_k(&core, pinned(150)),
            vec![(1, "p2".to_string()), (2, "p1".to_string())]
        );
        assert_eq!(top_k(&core, pinned(99)), Vec::new());
        let current = vec![
            (1, "p1".to_string()),
            (2, "p2".to_string()),
            (3, "p3".to_string()),
        ];
        assert_eq!(top_k(&core, SystemTimeScope::Current), current);
        assert_eq!(top_k(&core, pinned(200)), current);
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Aggregate dispatch: input-sourced routing, grouping-set expansion, and the
//! per-shard fast paths (result cache, index-backed COUNT) before falling
//! back to streaming aggregation over a collection scan.

use tracing::debug;

use super::cache_key::{AggregateCacheKeyInputs, aggregate_cache_key};
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_query::msgpack_scan;

/// Borrowed inputs to [`CoreLoop::execute_aggregate`]: the target collection
/// (or input sub-plan), GROUP BY / aggregate / filter / HAVING / sub-group /
/// grouping-set / sort specs for one aggregate dispatch.
pub(in crate::data::executor) struct AggregateExecInputs<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    /// The scope the per-shard scan of `collection` reads at. Ignored when
    /// `input` is set: the sub-plan carries its own scope.
    pub system_time: nodedb_types::SystemTimeScope,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub having: &'a [u8],
    pub limit: usize,
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    pub grouping_sets: &'a [Vec<u32>],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        inputs: AggregateExecInputs<'_>,
    ) -> Response {
        let AggregateExecInputs {
            task,
            tid,
            collection,
            input,
            system_time,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
        } = inputs;

        // A per-shard read at any scope other than `Current` folds the
        // engine's own scan at that scope: the versions visible at the
        // scope's ordinal, with a transaction snapshot's staged writes. The
        // current-state fast paths below read the newest version only.
        let scoped_scan;
        let input = match input {
            Some(sub_plan) => Some(sub_plan),
            None if !system_time.is_current() => {
                scoped_scan = self.scoped_aggregate_scan(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    system_time,
                );
                Some(&scoped_scan)
            }
            None => None,
        };

        debug!(core = self.core_id, %collection, has_input = input.is_some(), group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Input-sourced aggregate: the rows come from executing the sub-plan
        // (a coordinator-materialized `ProviderScan` over catalog rows, a
        // derived-table body, a join, or a transaction's full engine scan),
        // not from a per-shard collection scan. The per-shard fast paths
        // (cache, index-backed COUNT) do not apply to those rows.
        if let Some(sub_plan) = input {
            let docs = match self.input_docs(task, sub_plan) {
                Ok(docs) => docs,
                Err(resp) => return *resp,
            };
            if !grouping_sets.is_empty() {
                return super::super::grouping_sets_exec::execute_grouping_sets(
                    self,
                    super::super::grouping_sets_exec::GroupingSetsParams {
                        task,
                        docs,
                        group_by,
                        aggregates,
                        filters,
                        having,
                        limit,
                        grouping_sets,
                        sort_keys,
                    },
                );
            }
            return self.aggregate_over_docs(
                super::streaming::over_docs::AggregateOverDocsParams {
                    task,
                    collection,
                    cache_tid: None,
                    docs,
                    group_by,
                    aggregates,
                    filters,
                    having,
                    limit,
                    sub_group_by,
                    sub_aggregates,
                    sort_keys,
                },
            );
        }

        // ROLLUP / CUBE / GROUPING SETS: one scan, one pass per set.
        if !grouping_sets.is_empty() {
            let docs = match self.scan_for_aggregate(task, tid, collection) {
                Ok(docs) => docs,
                Err(resp) => return *resp,
            };
            return super::super::grouping_sets_exec::execute_grouping_sets(
                self,
                super::super::grouping_sets_exec::GroupingSetsParams {
                    task,
                    docs,
                    group_by,
                    aggregates,
                    filters,
                    having,
                    limit,
                    grouping_sets,
                    sort_keys,
                },
            );
        }

        // Fast path: incremental aggregate cache.
        if filters.is_empty() && having.is_empty() {
            let cache_key = aggregate_cache_key(AggregateCacheKeyInputs {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                group_by,
                aggregates,
                sub_group_by,
                sub_aggregates,
                limit,
                sort_keys,
            });
            let current_kv_epoch =
                self.kv_engine
                    .write_epoch(task.request.database_id.as_u64(), tid, collection);
            match self.aggregate_cache.get(&cache_key) {
                Some(entry) if entry.kv_epoch == current_kv_epoch => {
                    debug!(core = self.core_id, %collection, "aggregate cache hit");
                    return self.response_with_payload(task, entry.payload.clone());
                }
                Some(_) => {
                    // Stale: a KV write since this was computed bumped the
                    // collection's write epoch. Drop it so the recompute
                    // below can re-insert at the current epoch without
                    // being blocked by the 256 cap.
                    self.aggregate_cache.remove(&cache_key);
                }
                None => {}
            }
        }

        // Fast path: index-backed COUNT/GROUP BY. A computed group key
        // (`field: None`) has no physical column to scan an index on, so this
        // path is skipped for it and aggregation falls through to the streaming
        // (expression-evaluating) path below.
        if group_by.len() == 1
            && group_by.iter().all(|s| s.field.is_some())
            && filters.is_empty()
            && having.is_empty()
            && aggregates.len() == 1
            && aggregates[0].expr.is_none()
            && aggregates[0].function == "count"
            && let Some(field) = group_by[0].field.as_deref()
        {
            let out_name = group_by[0].output_name.as_str();
            if let Ok(groups) = self.sparse.scan_index_groups(
                task.request.database_id.as_u64(),
                tid,
                collection,
                field,
            ) && !groups.is_empty()
            {
                let mut payload_buf = Vec::with_capacity(groups.len() * 64);
                let row_count = groups.len().min(limit);
                let count_key = aggregates[0]
                    .user_alias
                    .clone()
                    .unwrap_or_else(|| canonical_agg_key("count", "*"));
                msgpack_scan::write_array_header(&mut payload_buf, row_count);
                for (value, count) in groups.into_iter().take(limit) {
                    msgpack_scan::write_map_header(&mut payload_buf, 2);
                    msgpack_scan::write_kv_str(&mut payload_buf, out_name, &value);
                    msgpack_scan::write_kv_i64(&mut payload_buf, &count_key, count as i64);
                }
                return self.response_with_payload(task, payload_buf);
            }
        }

        // Streaming aggregation over a per-shard collection scan.
        let docs = match self.scan_for_aggregate(task, tid, collection) {
            Ok(docs) => docs,
            Err(resp) => return *resp,
        };
        self.aggregate_over_docs(super::streaming::over_docs::AggregateOverDocsParams {
            task,
            collection,
            cache_tid: Some(tid),
            docs,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::VectorOp;

    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::kv::KvPutParams;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    fn open_core() -> (CoreLoop, tempfile::TempDir) {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        (core, dir)
    }

    /// Plan content is irrelevant to `execute_aggregate` — only
    /// `task.request.database_id` is read from it, and `tid`/`collection`
    /// are passed separately via `AggregateExecInputs`.
    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(VectorOp::Search {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "kvcoll"),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                metric: nodedb_types::vector_distance::DistanceMetric::L2,
                filter_bitmap: None,
                field_name: String::new(),
                rls_filters: Vec::new(),
                inline_prefilter_plan: None,
                ann_options: Default::default(),
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            system_ordinal: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    fn count_star() -> AggregateSpec {
        AggregateSpec {
            function: "count".to_string(),
            alias: "count".to_string(),
            user_alias: None,
            field: "*".to_string(),
            expr: None,
            param: None,
        }
    }

    fn count_star_inputs<'a>(
        task: &'a ExecutionTask,
        aggregates: &'a [AggregateSpec],
    ) -> AggregateExecInputs<'a> {
        AggregateExecInputs {
            task,
            tid: 1,
            collection: "kvcoll",
            input: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            group_by: &[],
            aggregates,
            filters: &[],
            having: &[],
            limit: usize::MAX,
            sub_group_by: &[],
            sub_aggregates: &[],
            grouping_sets: &[],
            sort_keys: &[],
        }
    }

    fn put_kv_row(core: &mut CoreLoop, key: &[u8]) {
        core.kv_engine
            .put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "kvcoll",
                key,
                value: b"v",
                ttl_ms: 0,
                now_ms: 1_000,
                surrogate: crate::engine::kv::test_support::row_surrogate(key),
            })
            .expect("a bound row writes");
    }

    /// An aggregate cache entry stamped with an older KV write epoch is a
    /// miss once the collection's write epoch has moved on — this is the fix
    /// for `SELECT COUNT(*)` over a `WITH (engine='kv')` collection serving a
    /// stale count after a KV write the cache had no explicit invalidation
    /// hook for.
    #[test]
    fn stale_kv_epoch_cache_entry_is_a_miss() {
        let (mut core, _dir) = open_core();
        let task = make_task();
        put_kv_row(&mut core, b"k1");

        let aggregates = vec![count_star()];

        let first = core.execute_aggregate(count_star_inputs(&task, &aggregates));
        assert_eq!(first.status, Status::Ok);
        assert_eq!(core.aggregate_cache.len(), 1);
        let cache_key = core.aggregate_cache.keys().next().unwrap().clone();
        let epoch_at_insert = core.aggregate_cache.get(&cache_key).unwrap().kv_epoch;
        assert_eq!(epoch_at_insert, core.kv_engine.write_epoch(0, 1, "kvcoll"));

        // A further KV write bumps the table's write epoch without going
        // through `invalidate_aggregate_cache_for_collection` — the epoch
        // stamp is the only thing that can catch this.
        put_kv_row(&mut core, b"k2");
        assert!(core.kv_engine.write_epoch(0, 1, "kvcoll") > epoch_at_insert);

        let second = core.execute_aggregate(count_star_inputs(&task, &aggregates));
        assert_eq!(second.status, Status::Ok);
        let refreshed_epoch = core.aggregate_cache.get(&cache_key).unwrap().kv_epoch;
        assert_eq!(
            refreshed_epoch,
            core.kv_engine.write_epoch(0, 1, "kvcoll"),
            "the stale entry must be evicted and recomputed at the current epoch"
        );
        assert_ne!(
            refreshed_epoch, epoch_at_insert,
            "serving the stale entry unchanged would reproduce the bug"
        );
    }

    /// HAVING bytes that do not decode fail the aggregate. Dropping the
    /// predicate would return every group.
    #[test]
    fn undecodable_having_errors() {
        let (mut core, _dir) = open_core();
        let task = make_task();
        put_kv_row(&mut core, b"k1");

        let aggregates = vec![count_star()];
        let garbage = [0xc1u8, 0xff, 0x00];
        let mut inputs = count_star_inputs(&task, &aggregates);
        inputs.having = &garbage;

        let response = core.execute_aggregate(inputs);
        assert_eq!(response.status, Status::Error);
        assert!(
            core.aggregate_cache.is_empty(),
            "a refused aggregate is never cached"
        );
    }

    /// WHERE bytes that do not decode fail the aggregate. Dropping the
    /// predicate would aggregate every row.
    #[test]
    fn undecodable_filter_errors() {
        let (mut core, _dir) = open_core();
        let task = make_task();
        put_kv_row(&mut core, b"k1");

        let aggregates = vec![count_star()];
        let garbage = [0xc1u8, 0xff, 0x00];
        let mut inputs = count_star_inputs(&task, &aggregates);
        inputs.filters = &garbage;

        let response = core.execute_aggregate(inputs);
        assert_eq!(response.status, Status::Error);
    }

    /// A grouping-set aggregate with undecodable HAVING bytes fails.
    #[test]
    fn undecodable_grouping_set_having_errors() {
        let (mut core, _dir) = open_core();
        let task = make_task();
        put_kv_row(&mut core, b"k1");

        let aggregates = vec![count_star()];
        let garbage = [0xc1u8, 0xff, 0x00];
        let sets = vec![Vec::<u32>::new()];
        let mut inputs = count_star_inputs(&task, &aggregates);
        inputs.having = &garbage;
        inputs.grouping_sets = &sets;

        let response = core.execute_aggregate(inputs);
        assert_eq!(response.status, Status::Error);
    }

    /// A per-shard aggregate at a transaction snapshot folds the KV scan at
    /// the snapshot, never the cached current-state count.
    #[test]
    fn a_snapshot_aggregate_reads_the_engine_scan_at_its_scope() {
        let (mut core, _dir) = open_core();
        put_kv_row(&mut core, b"k1");
        let scope = nodedb_types::SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 0,
        };
        let plan = core.scoped_aggregate_scan(0, 1, "kvcoll", scope);
        let PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Scan { system_time, .. }) = plan
        else {
            panic!("a KV collection reads a KV scan");
        };
        assert_eq!(system_time, scope);
        let unknown = core.scoped_aggregate_scan(0, 1, "docs", scope);
        assert!(matches!(
            unknown,
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan { .. })
        ));

        let task = make_task();
        let aggregates = vec![count_star()];
        let mut inputs = count_star_inputs(&task, &aggregates);
        inputs.system_time = scope;
        let response = core.execute_aggregate(inputs);
        assert_eq!(response.status, Status::Ok);
        assert!(
            core.aggregate_cache.is_empty(),
            "a snapshot aggregate never fills the current-state cache"
        );
    }
}

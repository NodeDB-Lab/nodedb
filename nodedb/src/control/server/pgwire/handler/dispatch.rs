//! Core dispatch mechanics: single-task dispatch, Raft replication, and local Data Plane submission.

use std::sync::Arc;
use std::time::Instant;

use crate::bridge::envelope::{Priority, Request, Response};
use crate::control::planner::physical::PhysicalTask;
use crate::types::{Lsn, ReadConsistency};
use sonic_rs;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, write operations are proposed to Raft first and only
    /// executed on the Data Plane after quorum commit. Reads bypass Raft.
    pub(super) async fn dispatch_task(&self, task: PhysicalTask) -> crate::Result<Response> {
        let tenant_id = task.tenant_id;
        let result = self.dispatch_task_inner(task).await;
        // Advance per-tenant observed write-HLC high-water on any
        // successful dispatch (local, raft-replicated, or broadcast).
        // Used by RESTORE's staleness gate. Backup captures envelope
        // watermark AFTER its own fan-out, so envelope.wm dominates
        // tenant_wm on a fresh backup.
        if let Ok(ref resp) = result
            && resp.status == crate::bridge::envelope::Status::Ok
        {
            self.state.advance_tenant_write_hlc(tenant_id.as_u32());
        }
        result
    }

    async fn dispatch_task_inner(&self, task: PhysicalTask) -> crate::Result<Response> {
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                crate::bridge::physical_plan::DocumentOp::InsertSelect { .. }
            )
        ) {
            return crate::control::server::dispatch_utils::broadcast_count_to_all_cores(
                &self.state,
                task.tenant_id,
                task.plan,
                0,
                "inserted",
            )
            .await;
        }

        // `DROP ARRAY` must reach every Data-Plane core so each can release
        // its per-core store and remove the on-disk segment dir; otherwise
        // a follow-up `CREATE ARRAY` of the same name carries stale state.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Array(
                crate::bridge::physical_plan::ArrayOp::DropArray { .. }
            )
        ) {
            return crate::control::server::dispatch_utils::broadcast_count_to_all_cores(
                &self.state,
                task.tenant_id,
                task.plan,
                0,
                "dropped",
            )
            .await;
        }

        // Broadcast scans to all cores — data is distributed across cores.
        if task.plan.is_broadcast_scan() {
            return crate::control::server::dispatch_utils::broadcast_to_all_cores(
                &self.state,
                task.tenant_id,
                task.plan,
                0,
            )
            .await;
        }

        // Cross-shard HashJoin: two-phase execution.
        if let crate::bridge::envelope::PhysicalPlan::Query(
            crate::bridge::physical_plan::QueryOp::HashJoin {
                ref left_collection,
                ref right_collection,
                ref left_alias,
                ref right_alias,
                ref on,
                ref join_type,
                limit,
                ref post_group_by,
                ref post_aggregates,
                ref projection,
                ref post_filters,
                ref inline_left,
                ref inline_right,
                inline_left_bitmap: _,
                inline_right_bitmap: _,
            },
        ) = task.plan
        {
            // Multi-way join: execute inner join first, gather right side,
            // then send both as a BroadcastJoin to a single core.
            if let Some(inner_plan) = inline_left {
                // Step 1: Execute the inner join via recursive dispatch.
                let inner_task = crate::control::planner::physical::PhysicalTask {
                    tenant_id: task.tenant_id,
                    vshard_id: task.vshard_id,
                    plan: inner_plan.as_ref().clone(),
                    post_set_op: crate::control::planner::physical::PostSetOp::None,
                };
                let inner_resp = Box::pin(self.dispatch_task(inner_task)).await?;
                let left_data: Vec<u8> = inner_resp.payload.as_ref().to_vec();

                // Step 2: Broadcast-scan the right collection.
                let right_scan = crate::bridge::envelope::PhysicalPlan::Document(
                    crate::bridge::physical_plan::DocumentOp::Scan {
                        collection: right_collection.clone(),
                        filters: Vec::new(),
                        limit: (limit * 10).min(50000),
                        offset: 0,
                        sort_keys: Vec::new(),
                        distinct: false,
                        projection: Vec::new(),
                        computed_columns: Vec::new(),
                        window_functions: Vec::new(),
                        system_as_of_ms: None,
                        valid_at_ms: None,
                        prefilter: None,
                    },
                );
                let right_data = crate::control::server::dispatch_utils::broadcast_raw(
                    &self.state,
                    task.tenant_id,
                    right_scan,
                    0,
                )
                .await?;

                // Step 3: Dispatch a HashJoin to core 0 with both sides embedded
                // as inline data (no collection scanning needed).
                let on_keys: Vec<(String, String)> =
                    on.iter().map(|(l, r)| (l.clone(), r.clone())).collect();
                let join_plan = crate::bridge::envelope::PhysicalPlan::Query(
                    crate::bridge::physical_plan::QueryOp::InlineHashJoin {
                        left_data,
                        right_data,
                        right_alias: right_alias.clone(),
                        on: on_keys,
                        join_type: join_type.clone(),
                        limit,
                        projection: projection.clone(),
                        post_filters: post_filters.clone(),
                    },
                );
                let join_task = crate::control::planner::physical::PhysicalTask {
                    tenant_id: task.tenant_id,
                    vshard_id: task.vshard_id,
                    plan: join_plan,
                    post_set_op: crate::control::planner::physical::PostSetOp::None,
                };
                let mut resp = self.dispatch_local(join_task).await?;

                let has_post_agg = !post_group_by.is_empty() || !post_aggregates.is_empty();
                if has_post_agg {
                    resp = crate::control::server::post_aggregate::apply_post_aggregation(
                        resp,
                        post_group_by,
                        post_aggregates,
                    )?;
                }
                return Ok(resp);
            }
            // Phase 1: broadcast scan the right collection across all cores.
            // Uses broadcast_raw to get raw binary payloads (no JSON wrapping).
            let broadcast_data = if let Some(right_plan) = inline_right {
                self.materialize_inline_join_side(task.tenant_id, task.vshard_id, right_plan)
                    .await?
            } else {
                let right_scan = crate::bridge::envelope::PhysicalPlan::Document(
                    crate::bridge::physical_plan::DocumentOp::Scan {
                        collection: right_collection.clone(),
                        filters: Vec::new(),
                        limit: (limit * 10).min(50000),
                        offset: 0,
                        sort_keys: Vec::new(),
                        distinct: false,
                        projection: Vec::new(),
                        computed_columns: Vec::new(),
                        window_functions: Vec::new(),
                        system_as_of_ms: None,
                        valid_at_ms: None,
                        prefilter: None,
                    },
                );
                crate::control::server::dispatch_utils::broadcast_raw(
                    &self.state,
                    task.tenant_id,
                    right_scan,
                    0,
                )
                .await?
            };

            tracing::warn!(
                broadcast_bytes = broadcast_data.len(),
                right = %right_collection,
                left = %left_collection,
                "two-phase join: phase 1 complete"
            );

            // Phase 2: dispatch BroadcastJoin to all cores (each core has a
            // shard of the left collection; the right side is fully embedded).
            let on_keys: Vec<(String, String)> =
                on.iter().map(|(l, r)| (l.clone(), r.clone())).collect();

            let has_post_agg = !post_group_by.is_empty() || !post_aggregates.is_empty();
            let post_group_by = post_group_by.clone();
            let post_aggregates = post_aggregates.clone();

            let broadcast_plan = crate::bridge::envelope::PhysicalPlan::Query(
                crate::bridge::physical_plan::QueryOp::BroadcastJoin {
                    large_collection: left_collection.clone(),
                    small_collection: right_collection.clone(),
                    large_alias: left_alias.clone(),
                    small_alias: right_alias.clone(),
                    broadcast_data,
                    on: on_keys,
                    join_type: join_type.clone(),
                    limit,
                    post_group_by: Vec::new(),
                    post_aggregates: Vec::new(),
                    projection: projection.clone(),
                    post_filters: post_filters.clone(),
                },
            );
            let mut resp = crate::control::server::dispatch_utils::broadcast_to_all_cores(
                &self.state,
                task.tenant_id,
                broadcast_plan,
                0,
            )
            .await?;

            // Post-join aggregation: if the original query had GROUP BY on join
            // results, aggregate them now in the Control Plane.
            if has_post_agg {
                resp = crate::control::server::post_aggregate::apply_post_aggregation(
                    resp,
                    &post_group_by,
                    &post_aggregates,
                )?;
            }

            return Ok(resp);
        }

        if let Some(async_proposer) = self.state.async_raft_proposer.get()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )
        {
            return self.dispatch_replicated_write(entry, async_proposer).await;
        }

        self.dispatch_local(task).await
    }

    /// Dispatch a write through Raft: propose → register waiter → await apply.
    ///
    /// The `AsyncRaftProposer` handles propose + waiter registration in one
    /// step. The `ProposeTracker` is race-safe: if the entry commits and
    /// applies on this node before `register()` is called, the result is
    /// stored and `register()` picks it up immediately.
    async fn dispatch_replicated_write(
        &self,
        entry: crate::control::wal_replication::ReplicatedEntry,
        proposer: &Arc<crate::control::wal_replication::AsyncRaftProposer>,
    ) -> crate::Result<Response> {
        let data = entry.to_bytes();
        let vshard_id = entry.vshard_id;

        let request_id = self.next_request_id();

        let payload = proposer(vshard_id, data)
            .await
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("raft propose failed: {e}"),
            })?;

        Ok(Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::new(0),
            error_code: None,
        })
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// For write operations, the WAL is appended **before** dispatching to the
    /// Data Plane. This ensures durability: if the process crashes after WAL
    /// append but before Data Plane execution, the write is replayed on recovery.
    /// Reads bypass the WAL entirely.
    async fn dispatch_local(&self, task: PhysicalTask) -> crate::Result<Response> {
        self.wal_append_if_write(task.tenant_id, task.vshard_id, &task.plan)?;
        self.submit_to_data_plane(task.tenant_id, task.vshard_id, task.plan)
            .await
    }

    /// Dispatch a task to the Data Plane WITHOUT individual WAL append.
    ///
    /// Used by COMMIT to dispatch buffered transaction tasks after the
    /// entire transaction has been written as a single `RecordType::Transaction`
    /// WAL record. Skipping per-task WAL avoids double-writing.
    pub(super) async fn dispatch_task_no_wal(&self, task: PhysicalTask) -> crate::Result<Response> {
        self.submit_to_data_plane(task.tenant_id, task.vshard_id, task.plan)
            .await
    }

    /// Build a `Request`, register with the tracker, dispatch to the Data Plane,
    /// and await the response. Shared by `dispatch_local` and `dispatch_task_no_wal`.
    async fn submit_to_data_plane(
        &self,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
        plan: crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<Response> {
        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan,
            deadline: Instant::now()
                + std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        let rx = self.state.tracker.register_oneshot(request_id);

        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        tokio::time::timeout(
            std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            rx,
        )
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })
    }

    async fn materialize_inline_join_side(
        &self,
        tenant_id: crate::types::TenantId,
        fallback_vshard_id: crate::types::VShardId,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<Vec<u8>> {
        let vshard_id = super::plan::extract_collection(plan)
            .map(crate::types::VShardId::from_collection)
            .unwrap_or(fallback_vshard_id);
        let task = crate::control::planner::physical::PhysicalTask {
            tenant_id,
            vshard_id,
            plan: plan.clone(),
            post_set_op: crate::control::planner::physical::PostSetOp::None,
        };
        let resp = Box::pin(self.dispatch_task(task)).await?;
        normalize_join_broadcast_payload(resp.payload.as_ref())
    }
}

fn normalize_join_broadcast_payload(payload: &[u8]) -> crate::Result<Vec<u8>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    match payload[0] {
        b'[' | b'{' => {
            let json: serde_json::Value =
                sonic_rs::from_slice(payload).map_err(|e| crate::Error::Codec {
                    detail: format!("join broadcast JSON decode: {e}"),
                })?;
            nodedb_types::json_to_msgpack(&json).map_err(|e| crate::Error::Codec {
                detail: format!("join broadcast msgpack encode: {e}"),
            })
        }
        _ => Ok(payload.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_join_broadcast_payload;

    #[test]
    fn normalize_join_broadcast_payload_converts_json_arrays_to_msgpack() {
        let payload = br#"[{"avg_amount":43.598}]"#;

        let normalized = normalize_join_broadcast_payload(payload).unwrap();
        let decoded = nodedb_types::json_from_msgpack(&normalized).unwrap();

        assert_eq!(decoded, serde_json::json!([{ "avg_amount": 43.598 }]));
    }

    #[test]
    fn normalize_join_broadcast_payload_keeps_msgpack_payloads() {
        let payload =
            nodedb_types::json_to_msgpack(&serde_json::json!([{ "avg_amount": 43.598 }])).unwrap();

        let normalized = normalize_join_broadcast_payload(&payload).unwrap();

        assert_eq!(normalized, payload);
    }
}

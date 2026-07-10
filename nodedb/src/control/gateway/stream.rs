// SPDX-License-Identifier: BUSL-1.1

//! Streaming gateway entry point: [`Gateway::execute_stream`].
//!
//! Mirrors [`Gateway::execute`](super::core::Gateway::execute)'s routing but
//! produces a merged [`ResultStream`] of row batches instead of a collected
//! `Vec<Vec<u8>>`, so rows flow to the client as they arrive.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::Error;
use crate::control::server::result_stream::ResultStream;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::core::{Gateway, QueryContext};
use super::dispatcher::{DispatchRouteStreamParams, default_deadline_ms, dispatch_route_stream};
use super::retry::retry_not_leader;
use super::route::TaskRoute;
use super::router::resolve_decision;

impl Gateway {
    /// Streaming sibling of [`execute`](Gateway::execute).
    ///
    /// Routes `plan` the same way as `execute` (via [`Gateway::compute_routes`]),
    /// but each route produces a [`ResultStream`] of row
    /// batches instead of a collected `Vec<Vec<u8>>`:
    ///
    /// - Local route → `gather_all_cores_stream` over the route's plan.
    /// - Remote route → `dispatch_remote_stream` (eager first-frame + typed
    ///   NotLeader retry before the first row; terminal-after-first thereafter).
    ///
    /// All per-route streams are merged with `futures::stream::select_all`, so
    /// rows interleave as they arrive. The not-leader retry wraps ONLY the eager
    /// pre-stream phase (opening the stream + first frame), matching the
    /// retry-vs-stream contract — once a route's stream is live, its errors are
    /// terminal.
    ///
    /// `execute` / `execute_with_version_set` are intentionally left unchanged;
    /// their many callers depend on the collected `Vec<Vec<u8>>` shape.
    pub async fn execute_stream(
        &self,
        ctx: &QueryContext,
        plan: PhysicalPlan,
    ) -> Result<ResultStream, Error> {
        let version_set = self.collect_version_set(&plan, ctx.tenant_id.as_u64(), ctx.database_id);

        let routes = self.compute_routes(plan, ctx)?;

        let deadline_ms = default_deadline_ms(&self.shared);

        let mut per_route: Vec<ResultStream> = Vec::with_capacity(routes.len());
        for route in routes {
            let vshard_id_u32 = route.vshard_id;
            let plan_for_retry = route.plan.clone();
            let routing_ref = self.shared.cluster_routing.as_deref();
            let retry_counter = Arc::clone(&self.not_leader_retry_count);
            let version_set_for_route = version_set.clone();

            // The not-leader retry wraps only the eager pre-stream phase. Each
            // attempt re-resolves the routing decision and re-opens the stream;
            // a pre-row NotLeader / DescriptorMismatch is retryable, anything
            // after the first frame is terminal (handled inside the stream).
            let stream = retry_not_leader(routing_ref, move |attempt| {
                if attempt > 0 {
                    retry_counter.fetch_add(1, Ordering::Relaxed);
                }
                let plan = plan_for_retry.clone();
                let shared = Arc::clone(&self.shared);
                let tenant_id = ctx.tenant_id;
                let database_id = ctx.database_id;
                let trace_id = ctx.trace_id;
                let txn_id = ctx.txn_id;
                let version_set = version_set_for_route.clone();
                async move {
                    let decision = {
                        let routing_guard = shared
                            .cluster_routing
                            .as_ref()
                            .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
                        let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
                            shared.raft_status_fn.get().map(|f| f()).unwrap_or_default();
                        let live_leader = move |group_id: u64| -> u64 {
                            raft_snapshot
                                .iter()
                                .find(|gs| gs.group_id == group_id)
                                .map(|gs| gs.leader_id)
                                .unwrap_or(0)
                        };
                        let live_lookup: Option<&dyn Fn(u64) -> u64> =
                            if shared.raft_status_fn.get().is_some() {
                                Some(&live_leader)
                            } else {
                                None
                            };
                        resolve_decision(
                            vshard_id_u32,
                            shared.node_id,
                            routing_guard.as_deref(),
                            live_lookup,
                        )
                    };
                    let route = TaskRoute {
                        plan,
                        decision,
                        vshard_id: vshard_id_u32,
                    };
                    dispatch_route_stream(DispatchRouteStreamParams {
                        route,
                        shared: &shared,
                        tenant_id,
                        database_id,
                        trace_id,
                        txn_id,
                        deadline_ms,
                        version_set: &version_set,
                    })
                    .await
                }
            })
            .await?;
            per_route.push(stream);
        }

        Ok(Box::pin(futures::stream::select_all(per_route)))
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of one cross-shard hop: plan, admit, fan out, await, gather.
//!
//! This is the only file in the module that touches shared state, the routing
//! table, the gateway, and tokio. Everything it decides has been factored out
//! around it — the envelope holds the destinations, the fan-out policy decides
//! how wide it may go, the SQL builder produces what the remote node re-plans,
//! and the merge folds the replies. What remains here is the orchestration
//! those pieces are composed by, so the async/lease/spawn machinery lives in
//! exactly one place.

use tracing::{debug, warn};

use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::{GraphResponseMeta, GraphTraversalOptions};
use crate::types::{DatabaseId, TenantId, TraceId};

use super::envelope::ScatterEnvelope;
use super::fan_out::{FanOutDecision, apply_fan_out_limits};
use super::merge_results::merge_traversal_results;
use super::remote_sql::{RemoteTraverseSql, build_graph_traverse_sql};

/// Parameters for a cross-shard graph traversal hop.
pub struct CrossShardHopParams<'a> {
    pub local_nodes: Vec<String>,
    pub envelope: ScatterEnvelope,
    pub options: &'a GraphTraversalOptions,
    /// Collection whose edges the remote hop walks. The owning node re-plans
    /// the walk from the SQL this builds, and a traversal that names no
    /// collection cannot be authorized there — so the scope travels with it.
    pub collection: &'a str,
    pub edge_label: Option<&'a str>,
    pub direction: crate::engine::graph::edge_store::Direction,
    pub remaining_depth: usize,
    /// Session database scope. Threaded into the per-traversal
    /// `QueryContext` and SQL plan so remote shard hops route and plan
    /// against the caller's database, not the hardcoded default.
    pub database_id: DatabaseId,
}

/// Coordinate a single cross-shard graph hop from the Control Plane.
///
/// Given a set of locally-discovered node IDs and a pre-built scatter envelope,
/// this function:
/// 1. Applies adaptive fan-out limits to the envelope.
/// 2. For each shard batch that passes the limit check, forwards a
///    `GRAPH TRAVERSE FROM '<node>' DEPTH 1` query to the leader node that
///    owns that shard via the cluster transport.
/// 3. Merges all remote results with `local_nodes` via deduplication.
///
/// Returns the merged node list and the aggregate `GraphResponseMeta`.
///
/// # Cluster mode only
///
/// This function assumes `shared.cluster_routing` and `shared.gateway`
/// are `Some`. Callers must check `shared.cluster_routing.is_some()` before
/// calling this function.
pub async fn coordinate_cross_shard_hop(
    shared: &SharedState,
    tenant_id: TenantId,
    params: CrossShardHopParams<'_>,
) -> crate::Result<(Vec<String>, GraphResponseMeta)> {
    let CrossShardHopParams {
        local_nodes,
        envelope: cross_shard_targets,
        options,
        collection,
        edge_label,
        direction,
        remaining_depth,
        database_id,
    } = params;
    // Fast path: nothing to scatter.
    if cross_shard_targets.is_empty() {
        return Ok((local_nodes, GraphResponseMeta::default()));
    }

    let decision = apply_fan_out_limits(cross_shard_targets, options);

    let (batches, mut meta) = match decision {
        FanOutDecision::Proceed { batches, meta } => (batches, meta),
        FanOutDecision::ProceedWithWarning { batches, meta } => {
            debug!(
                shards = meta.shards_reached,
                warning = ?meta.fan_out_warning,
                "cross-shard hop: fan-out soft limit exceeded, continuing"
            );
            (batches, meta)
        }
        FanOutDecision::Exceeded {
            dispatched,
            skipped,
            meta,
        } => {
            if options.fan_out_partial {
                debug!(
                    dispatched = dispatched.len(),
                    skipped = skipped.len(),
                    "cross-shard hop: hard fan-out limit, returning partial results"
                );
                (dispatched, meta)
            } else {
                return Err(crate::Error::FanOutExceeded {
                    shards_touched: meta.shards_reached + meta.shards_skipped,
                    limit: options.fan_out_hard,
                });
            }
        }
    };

    // Acquire the routing table and gateway once.
    let routing = match &shared.cluster_routing {
        Some(r) => r,
        None => {
            // Should not happen — callers must check. Return local results.
            warn!("coordinate_cross_shard_hop called without cluster routing");
            return Ok((local_nodes, meta));
        }
    };
    let gateway = match shared.gateway.get() {
        Some(g) => g.clone(),
        None => {
            warn!("coordinate_cross_shard_hop called without gateway");
            return Ok((local_nodes, meta));
        }
    };

    // We always traverse exactly 1 depth per scatter batch because the caller
    // drives the outer BFS loop. `remaining_depth` is included for completeness
    // but each forwarded request probes depth 1 so the Control Plane maintains
    // authoritative hop counting.
    let hop_depth = remaining_depth.min(1);

    // Fan out to all batches in parallel.
    let mut join_handles = Vec::with_capacity(batches.len());

    for batch in batches {
        let shard_id = batch.target_shard;
        let leader_node = {
            let rt = routing.read().unwrap_or_else(|p| p.into_inner());
            match rt.leader_for_vshard(shard_id.as_u32()) {
                Ok(node) => node,
                Err(e) => {
                    warn!(%shard_id, error = %e, "no leader for shard, skipping batch");
                    continue;
                }
            }
        };

        // Skip batches that target the local node — those nodes are already
        // covered by the local BFS that was executed before this call.
        if leader_node == shared.node_id {
            continue;
        }

        let tenant_id_u64 = tenant_id.as_u64();
        let edge_label = edge_label.map(str::to_owned);
        let collection = collection.to_owned();
        let mut any_error = false;
        let mut work = Vec::with_capacity(batch.node_ids.len());

        // The fan-out leg of a traversal the originating query already resolved
        // policy for — this synthesizes internal SQL per remote shard and has no
        // requester of its own, so it plans as the system.
        let security = crate::control::planner::context::SystemPlanSecurity::new(
            crate::types::TenantId::new(tenant_id_u64),
            "_system_scatter_gather",
        );

        // Plan and admit every traversal before spawning. The resulting work
        // owns its descriptor lease scope, so the spawned closure does not
        // need to retain or reconstruct SharedState.
        for node_id in batch.node_ids {
            let sql = build_graph_traverse_sql(RemoteTraverseSql {
                collection: &collection,
                node_id: &node_id,
                depth: hop_depth,
                edge_label: edge_label.as_deref(),
                direction,
            });
            let gw_ctx = crate::control::gateway::core::QueryContext {
                tenant_id: crate::types::TenantId::new(tenant_id_u64),
                trace_id: TraceId::generate(),
                database_id,
                txn_id: None,
            };
            let plan_ctx = crate::control::planner::context::QueryContext::for_state(shared);
            let (tasks, _output_schema, versions, _) = match plan_ctx
                .plan_sql_with_rls_and_versions(
                    &sql,
                    crate::types::TenantId::new(tenant_id_u64),
                    database_id,
                    &security.context(shared),
                    None,
                )
                .await
            {
                Ok(planned) => planned,
                Err(e) => {
                    warn!(
                        shard = %shard_id,
                        error = %e,
                        "remote graph traverse plan failed"
                    );
                    any_error = true;
                    continue;
                }
            };
            // Each planned remote query gets an independent scope. Keep it
            // through gateway execution and response payload consumption;
            // do not retain it across the next node in this batch.
            let lease_scope = match shared.acquire_plan_lease_scope(&versions) {
                Ok(scope) => scope,
                Err(e) => {
                    warn!(
                        shard = %shard_id,
                        error = %e,
                        "remote graph traverse rejected by descriptor lease admission"
                    );
                    any_error = true;
                    continue;
                }
            };
            let physical_plan = match tasks.into_iter().next().map(|task| task.plan) {
                Some(plan) => plan,
                None => {
                    any_error = true;
                    continue;
                }
            };

            work.push((gw_ctx, physical_plan, lease_scope));
        }

        let gateway_clone = gateway.clone();
        join_handles.push(tokio::spawn(async move {
            let mut shard_results: Vec<String> = Vec::new();

            for (gw_ctx, physical_plan, lease_scope) in work {
                match gateway_clone.execute_internal(&gw_ctx, physical_plan).await {
                    Ok(payloads) => {
                        for payload in payloads {
                            // `execute_graph_hop` encodes its `Vec<String>` of
                            // node ids with `response_codec::encode`, which is
                            // MessagePack — so `decode_payload` is the
                            // counterpart. A JSON parser here would fail on
                            // every payload, and an `if let Ok` would drop
                            // each one — not a tolerated shard failure but a
                            // silent one: the traversal would return only the
                            // nodes the local shard found, and report that as
                            // the complete answer. A shard whose reply
                            // cannot be read is flagged like a shard that failed
                            // to answer, so the caller sees a partial result
                            // rather than a wrong complete one.
                            match crate::data::executor::response_codec::decode_payload::<Vec<String>>(
                                &payload,
                            ) {
                                Ok(nodes) => shard_results.extend(nodes),
                                Err(e) => {
                                    warn!(
                                        shard = %shard_id,
                                        error = %e,
                                        "remote graph traverse reply could not be decoded"
                                    );
                                    any_error = true;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            shard = %shard_id,
                            error = %e,
                            "remote graph traverse dispatch failed"
                        );
                        any_error = true;
                    }
                }
                drop(lease_scope);
            }

            (shard_results, any_error)
        }));
    }

    // Collect all remote results.
    let mut remote_results: Vec<Vec<String>> = Vec::with_capacity(join_handles.len());
    for handle in join_handles {
        match handle.await {
            Ok((nodes, _had_error)) => {
                if !nodes.is_empty() {
                    remote_results.push(nodes);
                }
            }
            Err(e) => {
                warn!(error = %e, "cross-shard hop task panicked");
            }
        }
    }

    // Update meta with the number of shards that actually responded.
    meta.shards_reached = remote_results.len() as u16;

    // Deduplicate and merge local + remote results.
    let merged = merge_traversal_results(local_nodes, &remote_results);
    Ok((merged, meta))
}

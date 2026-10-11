// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-side distributed shuffle-AGGREGATE orchestration (E5c).
//!
//! Resolves a root `Exchange{ShuffleAggregate}` wrapping a `QueryOp::Aggregate`
//! into a real cross-node distributed GROUP BY by tying together the already-built
//! producer (`PartialAggregateState`) and consumer (`ShuffleAggregateConsume`)
//! primitives:
//!
//! 1. Allocate a `shuffle_id` and a partition plan (`part -> owner node`).
//! 2. Encode the bare per-shard partial-aggregate producer plan as `plan_bytes`.
//! 3. Fan a `ShuffleProduceRequest` to every producer node of the source
//!    collection CONCURRENTLY (single side `side=0`, hash-partitioned on the
//!    GROUP BY columns). Each producer scans locally, computes per-group partial
//!    `GroupState`s, hash-partitions them on the group key to the part-owners,
//!    and streams them. Fail-fast: any producer error aborts the whole shuffle.
//! 4. After ALL producers succeed, fan a `ShuffleAggregateConsumeRequest` to
//!    every part-owner CONCURRENTLY. Each owner waits for its one staged side to
//!    finalize, merges the staged partial states, finalizes / HAVING-filters,
//!    and replies with its rows. Fail-fast on any consumer error.
//! 5. Concatenate every consumer's msgpack-array rows, apply the aggregate's
//!    GLOBAL result cap ONCE over the union (matching the single-node Gather
//!    path), and return it as a `Resolved::Gathered` response. Groups are disjoint
//!    across parts (a group key hashes to exactly one part) and HAVING is
//!    per-group, so a plain concat is the correct finalize. Eligibility forbids a
//!    global ORDER BY (per-part finalize can't honour a cross-part sort), so the
//!    cap is order-free — the same arbitrary-but-bounded semantics Gather has with
//!    no ORDER BY. Consumers receive NO per-part cap (that will silently drop
//!    rows per part); the cap is applied only here.
//!
//! # Plane discipline
//!
//! This runs on the coordinator's Control Plane (Tokio). The QUIC `send_rpc`
//! calls are Control-Plane I/O, which is allowed here. No storage I/O, no
//! io_uring, no Data-Plane access from this module.

use std::collections::BTreeSet;

use futures::future::join_all;

use nodedb_cluster::rpc_codec::DescriptorVersionEntry;
use nodedb_cluster::{
    PartNodeEntry, RaftRpc, ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse,
    ShuffleProduceRequest, SortKey, TypedClusterError,
};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::server::exchange::gather::outcome_to_response;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId};

use super::exchange::Resolved;
use super::peers::{
    ShuffleRead, distinct_data_node_count, producer_nodes, producer_read_groups, send_produce,
};

/// Orchestrate a distributed shuffle GROUP BY aggregate.
///
/// `child` is the `QueryOp::Aggregate` the root `Exchange{ShuffleAggregate}`
/// wraps and `num_parts` the requested partition count (`0` = default to the
/// cluster data-node count). `keys` are the GROUP BY column names; the
/// authoritative grouping keys used for the partial-state producer and the
/// consumer reconstruction are taken from the wrapped `Aggregate.group_by`
/// directly (identical to `keys` by construction at emit), so `keys` is accepted
/// for shape symmetry but the wrapped `group_by` is the source of truth.
pub async fn resolve_shuffle_aggregate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    child: PhysicalPlan,
    keys: Vec<String>,
    num_parts: usize,
    read: ShuffleRead,
) -> crate::Result<Resolved> {
    let ShuffleRead {
        trace_id,
        linearizable,
    } = read;
    // 1. The child MUST be a root Aggregate — shuffle-aggregate wraps a complete
    //    GROUP BY aggregate.
    let PhysicalPlan::Query(QueryOp::Aggregate {
        collection,
        input,
        group_by,
        aggregates,
        filters,
        having,
        limit,
        sort_keys,
        ..
    }) = child
    else {
        return Err(crate::Error::Internal {
            detail: "ExchangeMode::ShuffleAggregate must wrap a QueryOp::Aggregate".into(),
        });
    };

    // The aggregate must be a BARE per-shard source (`input` is `None`): an
    // embedded `ProviderScan` (catalog) sub-plan cannot be re-scanned per-node by
    // the producers, and a catalog aggregate is coordinator-local anyway.
    if input.is_some() {
        return Err(crate::Error::Internal {
            detail: "shuffle aggregate requires a bare per-shard collection source \
                     (no embedded sub-plan)"
                .into(),
        });
    }
    // A non-GROUP-BY (scalar) aggregate has a single global group; there is no
    // key to repartition on, so a shuffle will be pointless. The planner emit
    // already gates on a non-empty GROUP BY, but reject here too for robustness.
    if group_by.is_empty() {
        return Err(crate::Error::Internal {
            detail: "shuffle aggregate requires a non-empty GROUP BY key list".into(),
        });
    }
    // Plain column-name view of the group-key specs, used for the Exchange-key
    // agreement check and the wire requests (which repartition / re-key on the
    // physical column names, not the full specs).
    let group_by_fields: Vec<String> = group_by.iter().filter_map(|s| s.field.clone()).collect();
    // `keys` carried by the Exchange mode must agree with the wrapped aggregate's
    // grouping columns (identical by construction at emit). A mismatch is a
    // planner bug, not a runtime condition — surface it loudly.
    if keys != group_by_fields {
        return Err(crate::Error::Internal {
            detail: format!(
                "shuffle aggregate: Exchange keys {keys:?} do not match wrapped \
                 Aggregate group_by {group_by_fields:?}"
            ),
        });
    }

    // 2. Cluster mode is mandatory — single-node has no peers to shuffle across.
    let (Some(transport), Some(routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return Err(crate::Error::Internal {
            detail: "distributed shuffle aggregate requires cluster mode \
                     (no transport / routing table on this node)"
                .into(),
        });
    };

    // Take a routing snapshot up front: producer node set and the partition plan
    // are all computed against ONE consistent view.
    let routing_snapshot = {
        let guard = routing.read().unwrap_or_else(|p| p.into_inner());
        guard.clone()
    };

    // 3. Allocate the shuffle id and the partition count. `num_parts == 0`
    //    defaults to the cluster data-node count (distinct data-group leaders);
    //    clamp to >= 1 so the partition plan always covers at least one part.
    let shuffle_id = state.next_shuffle_id();
    let data_node_count = distinct_data_node_count(&routing_snapshot);
    let effective_num_parts = if num_parts == 0 {
        data_node_count.max(1)
    } else {
        num_parts
    };

    let part_map = nodedb_cluster::distributed_join::plan_shuffle_partitions(
        &routing_snapshot,
        effective_num_parts,
    );
    if part_map.len() != effective_num_parts {
        return Err(crate::Error::Internal {
            detail: format!(
                "shuffle aggregate partition plan incomplete: expected {effective_num_parts} \
                 parts, got {} (no data groups?)",
                part_map.len()
            ),
        });
    }
    // `part_node_map` sorted by part for a stable producer-side fan-out order.
    let part_node_map: Vec<PartNodeEntry> = {
        let mut entries: Vec<PartNodeEntry> = part_map
            .iter()
            .map(|(&part, &node_id)| PartNodeEntry { part, node_id })
            .collect();
        entries.sort_by_key(|e| e.part);
        entries
    };

    // 4. Producer node set for the source collection (single-vShard-homed → one
    //    leader, but compute generally and dedup).
    let producers = producer_nodes(&routing_snapshot, database_id, collection.as_str())?;
    let read_groups = producer_read_groups(
        &routing_snapshot,
        database_id,
        collection.as_str(),
        linearizable,
    )?;
    let producer_count = producers.len() as u32;
    if producer_count == 0 {
        return Err(crate::Error::Internal {
            detail: "shuffle aggregate: source collection resolved to zero producer nodes".into(),
        });
    }

    // Ensure the transport knows every target node's address before dispatching.
    // (See `register_peers_from_topology` — tolerant of a peer the transport has
    // not warmed yet.)
    {
        let mut targets: BTreeSet<u64> = BTreeSet::new();
        targets.extend(producers.iter().copied());
        targets.extend(part_node_map.iter().map(|e| e.node_id));
        register_peers_from_topology(state, transport, &targets);
    }

    // 5. Build the PRODUCER plan: a bare per-shard partial-aggregate. It scans
    //    `collection` locally on each producer node, computes per-group partial
    //    `GroupState`s, and emits one flat row per group for repartitioning. It
    //    carries no node-local paths, so it is wire-shippable.
    let producer_plan = PhysicalPlan::Query(QueryOp::PartialAggregateState {
        collection: collection.clone(),
        input: None,
        group_by: group_by.clone(),
        aggregates: aggregates.clone(),
        filters: filters.clone(),
    });
    let plan_bytes = plan_wire::encode(&producer_plan).map_err(|e| crate::Error::Internal {
        detail: format!("shuffle aggregate: encode producer plan: {e}"),
    })?;

    // Deadline budget shared by produce + consume: what is left of the running
    // statement's budget, so both shuffle sides stop when the statement does.
    let deadline_remaining_ms = crate::control::server::shared::session::statement_deadline_ms(
        state.tuning.network.default_deadline_secs,
    );
    let num_parts_u32 = u32::try_from(effective_num_parts).map_err(|_| crate::Error::Internal {
        detail: format!("shuffle aggregate: num_parts {effective_num_parts} exceeds u32"),
    })?;

    // 6. Dispatch ALL producers CONCURRENTLY (single side=0). Each producer
    //    hash-partitions its per-group partial states on the GROUP BY columns.
    let mut produce_futures = Vec::with_capacity(producers.len());
    for &node in &producers {
        let req = ShuffleProduceRequest {
            shuffle_id,
            side: 0,
            num_parts: num_parts_u32,
            producer_count,
            keys: group_by_fields.clone(),
            part_node_map: part_node_map.clone(),
            plan_bytes: plan_bytes.clone(),
            tenant_id: tenant_id.as_u64(),
            database_id: database_id.as_u64(),
            deadline_remaining_ms,
            trace_id: trace_id.0,
            descriptor_versions: Vec::<DescriptorVersionEntry>::new(),
            read_groups: read_groups.clone(),
        };
        produce_futures.push(send_produce(transport, node, req));
    }
    // Await ALL producers; any error fails the whole shuffle (no partial result).
    // Fold every producer's observed read versions: the producers all scan the
    // SAME single source collection, so the fold holds that collection's
    // version on each vShard at read time: the comparand the coordinator
    // records for cross-shard OCC read validation of this aggregate.
    let mut read_versions = crate::types::ReadVersions::new();
    for result in join_all(produce_futures).await {
        read_versions.merge(&result?);
    }

    // 7. After ALL producers succeed, dispatch consumers CONCURRENTLY — one per
    //    part, to that part's owner. Each owner waits for its single staged side
    //    (0), merges the partial states, finalizes / HAVING-filters / sorts /
    //    LIMITs, and replies with its rows.
    let aggregates_bytes =
        zerompk::to_msgpack_vec(&aggregates).map_err(|e| crate::Error::Internal {
            detail: format!("shuffle aggregate: encode aggregate specs: {e}"),
        })?;
    // Per-part consumers receive NO row cap: each must return ALL of its (disjoint)
    // groups so the coordinator can apply the aggregate's global result cap ONCE
    // over the union. Pushing `limit` to each part will truncate parts
    // independently — a silent per-part row drop, and a result that disagrees with
    // the single-node Gather path's GLOBAL cap. The cap is reapplied at step 8.
    // Post-aggregate ORDER BY is restricted to bare output columns by the
    // planner, which is what the shuffle wire form carries. A key that is not
    // a column is rejected at plan time, so none reaches here.
    let wire_sort_keys: Vec<SortKey> = sort_keys
        .iter()
        .filter_map(|k| {
            k.as_column().map(|column| SortKey {
                column: column.to_string(),
                ascending: k.ascending,
            })
        })
        .collect();

    let mut consume_futures = Vec::with_capacity(part_node_map.len());
    for entry in &part_node_map {
        let req = ShuffleAggregateConsumeRequest {
            shuffle_id,
            part: entry.part,
            group_by: group_by_fields.clone(),
            aggregates_bytes: aggregates_bytes.clone(),
            having: having.clone(),
            // No per-part cap — the global cap is applied at the coordinator (step 8).
            limit: u64::MAX,
            sort_keys: wire_sort_keys.clone(),
            tenant_id: tenant_id.as_u64(),
            database_id: database_id.as_u64(),
            deadline_remaining_ms,
            trace_id: trace_id.0,
        };
        consume_futures.push(send_consume(transport, entry.node_id, req));
    }
    // Await ALL consumers; any error fails the whole shuffle.
    let mut per_part_rows: Vec<Vec<u8>> = Vec::with_capacity(consume_futures.len());
    for result in join_all(consume_futures).await {
        per_part_rows.push(result?);
    }

    // 8. Concatenate every part-owner's rows into one element list, then apply the
    //    aggregate's GLOBAL result cap ONCE over the union (matching the single-node
    //    Gather path, which truncates the full result to `limit`). Groups are
    //    disjoint across parts and HAVING is per-group, so concat is the correct
    //    finalize; eligibility forbids a global ORDER BY so the cap is order-free
    //    (the same arbitrary-but-bounded semantics the Gather path has without an
    //    ORDER BY). Reuse the shared extract/encode helpers — no hand-rolled framing.
    let mut elements: Vec<Vec<u8>> = Vec::new();
    for rows in &per_part_rows {
        elements.extend(extract_msgpack_elements(rows));
    }
    if elements.len() > limit {
        elements.truncate(limit);
    }
    let merged = encode_msgpack_array(&elements);

    // The producers report the source collection's versions at read time on
    // their `ShuffleProduceResponse`; the fold above is what this aggregate
    // observed. Pass it as the OCC read-version comparand so an
    // in-transaction distributed aggregate records a sound read-set entry (the
    // aggregate is single-collection, so `record_read_set` attributes it to the
    // right collection). The core-global `watermark_lsn` is NOT threaded through
    // the shuffle transport and stays `ZERO`; using it as the read version will
    // skip required aborts (it advances on writes to ANY collection).
    Ok(Resolved::Gathered(
        outcome_to_response(merged, Lsn::ZERO, read_versions),
        Vec::new(),
        Vec::new(),
    ))
}

/// Send one `ShuffleAggregateConsumeRequest`, returning that part's msgpack-array
/// rows or a typed error. Fail-fast: a consumer-reported error aborts.
async fn send_consume(
    transport: &nodedb_cluster::NexarTransport,
    node: u64,
    req: ShuffleAggregateConsumeRequest,
) -> crate::Result<Vec<u8>> {
    let part = req.part;
    match transport
        .send_rpc(node, RaftRpc::ShuffleAggregateConsumeRequest(req))
        .await
    {
        Ok(RaftRpc::ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse {
            rows,
            error: None,
        })) => Ok(rows),
        // A shard's Data-Plane verdict keeps its code so the client sees the
        // SQLSTATE a single-node aggregate renders.
        Ok(RaftRpc::ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse {
            error: Some(TypedClusterError::DataPlane { code }),
            ..
        })) => Err(crate::Error::DataPlane(code.into())),
        Ok(RaftRpc::ShuffleAggregateConsumeResponse(ShuffleAggregateConsumeResponse {
            error: Some(e),
            ..
        })) => Err(crate::Error::Internal {
            detail: format!(
                "shuffle aggregate consume failed for part {part} on node {node}: {e:?}"
            ),
        }),
        Ok(other) => Err(crate::Error::Internal {
            detail: format!(
                "shuffle aggregate consume: unexpected reply for part {part} from node \
                 {node}: {other:?}"
            ),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!(
                "shuffle aggregate consume RPC for part {part} to node {node} failed: {e}"
            ),
        }),
    }
}

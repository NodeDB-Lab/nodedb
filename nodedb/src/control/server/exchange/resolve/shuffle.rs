// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-side distributed shuffle-JOIN orchestration (E4b-2).
//!
//! Resolves a root `Exchange{Shuffle}` wrapping a `QueryOp::HashJoin` into a
//! real cross-node grace hash join by tying together the already-built producer
//! (E4a) and consumer (E4b-1) primitives:
//!
//! 1. Allocate a `shuffle_id` and a partition plan (`part -> owner node`).
//! 2. Encode each side's bare full-collection scan as `plan_bytes`.
//! 3. Fan a `ShuffleProduceRequest` to every producer node of each side
//!    CONCURRENTLY (build side `side=0`, probe side `side=1`). Each producer
//!    scans locally, hash-partitions its rows on the per-side keys, and streams
//!    them to the part-owners. Fail-fast: any producer error aborts the whole
//!    shuffle (no partial join).
//! 4. After ALL producers succeed, fan a `ShuffleConsumeRequest` to every
//!    part-owner CONCURRENTLY. Each owner waits for both staged sides to
//!    finalize, runs the node-local grace join, and replies with its rows.
//!    Fail-fast on any consumer error.
//! 5. Concatenate every consumer's msgpack-array rows into one merged array and
//!    return it as a `Resolved::Gathered` response.
//!
//! # Plane discipline
//!
//! This runs on the coordinator's Control Plane (Tokio). The QUIC `send_rpc`
//! calls are Control-Plane I/O, which is allowed here. No storage I/O, no
//! io_uring, no Data-Plane access from this module.

use std::collections::BTreeSet;

use futures::future::{join, join_all};

use nodedb_cluster::rpc_codec::DescriptorVersionEntry;
use nodedb_cluster::{
    JoinKeyPair, PartNodeEntry, RaftRpc, ShuffleConsumeRequest, ShuffleConsumeResponse,
    ShuffleProduceRequest, TypedClusterError,
};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::server::exchange::full_scan::{ScanSide, full_scan_plan_for_collection};
use crate::control::server::exchange::gather::outcome_to_response;
use crate::control::server::payload_merge::merge_msgpack_arrays;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId};

use super::capture::DistributedReadCapture;
use super::exchange::Resolved;
use super::peers::{
    ShuffleRead, distinct_data_node_count, producer_nodes, producer_read_groups, send_produce,
};

/// Orchestrate a distributed shuffle hash join.
///
/// `child` is the `QueryOp::HashJoin` the root `Exchange{Shuffle}` wraps and
/// `num_parts` the requested partition count (`0` = default to the cluster
/// data-node count). `_keys` is the `Exchange{Shuffle}.keys` copy of the join's
/// equi-keys; the authoritative per-side hash keys are derived from the wrapped
/// `HashJoin.on` directly (the two are identical by construction at emit), so
/// `_keys` is accepted for shape symmetry but intentionally unused.
pub async fn resolve_shuffle_join(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    child: PhysicalPlan,
    _keys: Vec<(String, String)>,
    num_parts: usize,
    read: ShuffleRead,
) -> crate::Result<Resolved> {
    let ShuffleRead {
        trace_id,
        linearizable,
    } = read;
    // 1. The child MUST be a HashJoin — shuffle wraps a complete hash join.
    let PhysicalPlan::Query(QueryOp::HashJoin {
        left_collection,
        right_collection,
        left_alias,
        right_alias,
        on,
        join_type,
        limit,
        left_input,
        right_input,
        left_rls_filters,
        right_rls_filters,
        left_scan_filters,
        right_scan_filters,
        ..
    }) = child
    else {
        return Err(crate::Error::Internal {
            detail: "ExchangeMode::Shuffle must wrap a QueryOp::HashJoin".into(),
        });
    };

    // Both inputs must be BARE name scans (the emit leaves them `None`). An
    // embedded `ProviderScan` / nested-join sub-plan cannot be re-scanned
    // per-node by the producers.
    if left_input.is_some() || right_input.is_some() {
        return Err(crate::Error::Internal {
            detail: "shuffle join requires both inputs as bare collection scans \
                     (no embedded sub-plan)"
                .into(),
        });
    }
    if on.is_empty() {
        return Err(crate::Error::Internal {
            detail: "shuffle join requires a non-empty equi-join key list".into(),
        });
    }

    // 2. Cluster mode is mandatory — single-node has no peers to shuffle across.
    let (Some(transport), Some(routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return Err(crate::Error::Internal {
            detail: "distributed shuffle join requires cluster mode \
                     (no transport / routing table on this node)"
                .into(),
        });
    };

    // Take a routing snapshot up front: producer/consumer node sets and the
    // partition plan are all computed against ONE consistent view.
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
                "shuffle partition plan incomplete: expected {effective_num_parts} parts, \
                 got {} (no data groups?)",
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

    // 4. Per-side hash keys: the LEFT column of each `on` pair partitions the
    //    probe (left) side, the RIGHT column the build (right) side, so matching
    //    rows co-locate on the same part.
    let probe_keys: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
    let build_keys: Vec<String> = on.iter().map(|(_, r)| r.clone()).collect();

    // 5. Producer node sets per side (collections are single-vShard-homed → one
    //    leader each, but compute generally and dedup).
    let build_nodes = producer_nodes(&routing_snapshot, database_id, right_collection.as_str())?;
    let probe_nodes = producer_nodes(&routing_snapshot, database_id, left_collection.as_str())?;
    let build_read_groups = producer_read_groups(
        &routing_snapshot,
        database_id,
        right_collection.as_str(),
        linearizable,
    )?;
    let probe_read_groups = producer_read_groups(
        &routing_snapshot,
        database_id,
        left_collection.as_str(),
        linearizable,
    )?;
    let build_producer_count = build_nodes.len() as u32;
    let probe_producer_count = probe_nodes.len() as u32;
    if build_producer_count == 0 || probe_producer_count == 0 {
        return Err(crate::Error::Internal {
            detail: "shuffle join: a join side resolved to zero producer nodes".into(),
        });
    }

    // Ensure the transport knows every target node's address before dispatching.
    // In production the WarmPeers startup phase registers every peer from the
    // topology, but a node that joined after that phase — or a coordinator that
    // never had to reach a given peer — can still lack it in the transport's
    // address map, and `send_rpc` to an unregistered peer fails with
    // NodeUnreachable even though the topology knows the node. Resolve each
    // producer/consumer node's address from the live topology and register it
    // (idempotent) up front so the fan-out below never spuriously fails.
    {
        let mut targets: BTreeSet<u64> = BTreeSet::new();
        targets.extend(build_nodes.iter().copied());
        targets.extend(probe_nodes.iter().copied());
        targets.extend(part_node_map.iter().map(|e| e.node_id));
        register_peers_from_topology(state, transport, &targets);
    }

    // 6. Encode each side's full-collection scan. The producer cannot scan
    //    by name across nodes, so a missing catalog entry is a hard error here
    //    (unlike the broadcast-gather path's graceful name-scan fallback).
    // Each producer scans its side under that side's own injected read policy:
    // the shuffle stages rows per side before any match, so an excluded row
    // never reaches a partner on any part.
    let build_scan = require_scan_plan(
        state,
        database_id,
        tenant_id,
        ScanSide::join_side(
            right_collection.as_str(),
            &right_rls_filters,
            &right_scan_filters,
        ),
    )?;
    let probe_scan = require_scan_plan(
        state,
        database_id,
        tenant_id,
        ScanSide::join_side(
            left_collection.as_str(),
            &left_rls_filters,
            &left_scan_filters,
        ),
    )?;
    let build_plan_bytes = plan_wire::encode(&build_scan).map_err(|e| crate::Error::Internal {
        detail: format!("shuffle join: encode build scan: {e}"),
    })?;
    let probe_plan_bytes = plan_wire::encode(&probe_scan).map_err(|e| crate::Error::Internal {
        detail: format!("shuffle join: encode probe scan: {e}"),
    })?;

    // Deadline budget shared by produce + consume: what is left of the running
    // statement's budget, so both shuffle sides stop when the statement does.
    let deadline_remaining_ms = crate::control::server::shared::session::statement_deadline_ms(
        state.tuning.network.default_deadline_secs,
    );
    let num_parts_u32 = u32::try_from(effective_num_parts).map_err(|_| crate::Error::Internal {
        detail: format!("shuffle join: num_parts {effective_num_parts} exceeds u32"),
    })?;

    // 7. Dispatch ALL producers CONCURRENTLY (build side=0, probe side=1), but
    //    keep the two sides' futures SEPARATE so each side's observed
    //    per-collection read-version can be max-folded independently.
    let mut build_produce_futures = Vec::with_capacity(build_nodes.len());
    for &node in &build_nodes {
        let req = ShuffleProduceRequest {
            shuffle_id,
            side: 0,
            num_parts: num_parts_u32,
            producer_count: build_producer_count,
            keys: build_keys.clone(),
            part_node_map: part_node_map.clone(),
            plan_bytes: build_plan_bytes.clone(),
            tenant_id: tenant_id.as_u64(),
            database_id: database_id.as_u64(),
            deadline_remaining_ms,
            trace_id: trace_id.0,
            descriptor_versions: Vec::<DescriptorVersionEntry>::new(),
            read_groups: build_read_groups.clone(),
        };
        build_produce_futures.push(send_produce(transport, node, req));
    }
    let mut probe_produce_futures = Vec::with_capacity(probe_nodes.len());
    for &node in &probe_nodes {
        let req = ShuffleProduceRequest {
            shuffle_id,
            side: 1,
            num_parts: num_parts_u32,
            producer_count: probe_producer_count,
            keys: probe_keys.clone(),
            part_node_map: part_node_map.clone(),
            plan_bytes: probe_plan_bytes.clone(),
            tenant_id: tenant_id.as_u64(),
            database_id: database_id.as_u64(),
            deadline_remaining_ms,
            trace_id: trace_id.0,
            descriptor_versions: Vec::<DescriptorVersionEntry>::new(),
            read_groups: probe_read_groups.clone(),
        };
        probe_produce_futures.push(send_produce(transport, node, req));
    }
    // Await ALL producers (both sides CONCURRENTLY); any error fails the whole
    // shuffle (no partial join). Fold each side's producers' observed read
    // versions independently: build ↔ right_collection, probe ↔
    // left_collection. Each side's producers scan the SAME single collection,
    // so the fold holds that collection's version on each vShard at read time:
    // the OCC read-version comparand recorded for that side.
    let (build_results, probe_results) = join(
        join_all(build_produce_futures),
        join_all(probe_produce_futures),
    )
    .await;
    let mut build_rv = crate::types::ReadVersions::new();
    for result in build_results {
        build_rv.merge(&result?);
    }
    let mut probe_rv = crate::types::ReadVersions::new();
    for result in probe_results {
        probe_rv.merge(&result?);
    }

    // 8. After ALL producers succeed, dispatch consumers CONCURRENTLY — one per
    //    part, to that part's owner. The consumer waits for both build (side 0)
    //    and probe (side 1) barriers; the per-side producer_count it waits on is
    //    carried by the producers' own ShufflePush frames (not by this request),
    //    so ShuffleConsumeRequest has no producer_count field.
    let on_pairs: Vec<JoinKeyPair> = on
        .iter()
        .map(|(l, r)| JoinKeyPair {
            left: l.clone(),
            right: r.clone(),
        })
        .collect();
    let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
    let probe_qualifier = left_alias.unwrap_or_default();
    let index_qualifier = right_alias.unwrap_or_default();

    let mut consume_futures = Vec::with_capacity(part_node_map.len());
    for entry in &part_node_map {
        let req = ShuffleConsumeRequest {
            shuffle_id,
            part: entry.part,
            on: on_pairs.clone(),
            join_type: join_type.clone(),
            limit: limit_u64,
            probe_qualifier: probe_qualifier.clone(),
            index_qualifier: index_qualifier.clone(),
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

    // 9. Merge every part-owner's msgpack-array rows into one array (reuse the
    //    shared gather merge helper — never hand-roll msgpack framing).
    let merged = merge_msgpack_arrays(&per_part_rows);

    // The per-node consumer registries self-clean their inboxes once the part's
    // staged sides are consumed; there is no coordinator-side cleanup hook to
    // call here. No leak: each `(shuffle_id, part, side)` inbox is dropped by
    // the part-owner after its consume completes.
    //
    // Two per-side read captures carry each side's REAL observed read version:
    // the probe capture (left_collection) at `probe_rv`, the build capture
    // (right_collection) at `build_rv` — folded from the producers'
    // `ShuffleProduceResponse.read_versions`. The record seam records one
    // read-set entry per capture, re-homing and revalidating each side's vshard
    // independently, so a concurrent write to EITHER side between the in-txn read
    // and commit is detected (the build side is recorded too). The response's own scalar stays `ZERO`: the captures
    // carry the versions, and also setting the scalar will double-record the
    // left side. The core-global watermark is not threaded through the shuffle
    // transport and likewise stays `ZERO`.
    let captures = vec![
        DistributedReadCapture {
            scan_plan: probe_scan,
            read_versions: probe_rv,
        },
        DistributedReadCapture {
            scan_plan: build_scan,
            read_versions: build_rv,
        },
    ];
    Ok(Resolved::Gathered(
        outcome_to_response(merged, Lsn::ZERO, crate::types::ReadVersions::new()),
        Vec::new(),
        captures,
    ))
}

/// Send one `ShuffleConsumeRequest`, returning that part's msgpack-array rows or
/// a typed error. Fail-fast: a consumer-reported error aborts.
async fn send_consume(
    transport: &nodedb_cluster::NexarTransport,
    node: u64,
    req: ShuffleConsumeRequest,
) -> crate::Result<Vec<u8>> {
    let part = req.part;
    match transport
        .send_rpc(node, RaftRpc::ShuffleConsumeRequest(req))
        .await
    {
        Ok(RaftRpc::ShuffleConsumeResponse(ShuffleConsumeResponse { rows, error: None })) => {
            Ok(rows)
        }
        // A shard's Data-Plane verdict keeps its code so the client sees the
        // SQLSTATE a single-node join renders.
        Ok(RaftRpc::ShuffleConsumeResponse(ShuffleConsumeResponse {
            error: Some(TypedClusterError::DataPlane { code }),
            ..
        })) => Err(crate::Error::DataPlane(code.into())),
        Ok(RaftRpc::ShuffleConsumeResponse(ShuffleConsumeResponse { error: Some(e), .. })) => {
            Err(crate::Error::Internal {
                detail: format!("shuffle consume failed for part {part} on node {node}: {e:?}"),
            })
        }
        Ok(other) => Err(crate::Error::Internal {
            detail: format!(
                "shuffle consume: unexpected reply for part {part} from node {node}: {other:?}"
            ),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!("shuffle consume RPC for part {part} to node {node} failed: {e}"),
        }),
    }
}

/// Build a full-collection scan plan for one join side under its own read
/// policy, erroring if the catalog has no record for it (the shuffle producer
/// cannot scan by name across nodes, so a missing entry is fatal — unlike the
/// broadcast path's name-scan fallback).
fn require_scan_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
) -> crate::Result<PhysicalPlan> {
    let collection = side.collection().to_string();
    full_scan_plan_for_collection(state, database_id, tenant_id, side)?.ok_or_else(|| {
        crate::Error::Internal {
            detail: format!(
                "shuffle join: no catalog entry for collection '{collection}' on coordinator"
            ),
        }
    })
}

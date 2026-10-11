// SPDX-License-Identifier: BUSL-1.1

//! Shared coordinator-side peer/partition helpers for the distributed shuffle
//! resolvers (`shuffle` = shuffle-join, `shuffle_aggregate` = shuffle GROUP BY).
//!
//! Both resolvers fan producer/consumer RPCs across the cluster and need the
//! same primitives: resolve a collection's owner nodes, count the cluster's
//! data nodes for the default partition count, and send a produce request.
//! They live here (rather than duplicated in each resolver) so the two paths
//! share one implementation.

use std::collections::BTreeSet;

use nodedb_cluster::{
    METADATA_GROUP_ID, RaftRpc, RoutingTable, ShuffleProduceRequest, ShuffleProduceResponse,
};

use crate::types::{DatabaseId, TraceId};

/// Producer nodes that own `collection`'s data. `collection` is the plan's
/// database-qualified name. Resolve its canonical key's vShard → owning
/// group → leader. A user collection is single-vShard-homed, so this is one
/// node; returned as a deduped sorted vec for generality.
pub(super) fn producer_nodes(
    routing: &RoutingTable,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<Vec<u64>> {
    let group = collection_group(routing, database_id, collection)?;
    let leader = routing
        .group_info(group)
        .map(|g| g.leader)
        .filter(|&l| l != 0)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("shuffle: no leader for group {group} ({collection})"),
        })?;
    Ok(vec![leader])
}

/// The Raft group that homes `collection`.
fn collection_group(
    routing: &RoutingTable,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<u64> {
    let vshard = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?
        .vshard()
        .as_u32();
    routing
        .group_for_vshard(vshard)
        .map_err(|e| crate::Error::Internal {
            detail: format!("shuffle: no group for vshard {vshard} ({collection}): {e}"),
        })
}

/// How a shuffle's producers read: the statement's trace, and whether each
/// producer scan is a linearizable read.
#[derive(Debug, Clone, Copy)]
pub struct ShuffleRead {
    pub trace_id: TraceId,
    pub linearizable: bool,
}

/// Groups a producer scanning `collection` confirms before it reads: the
/// collection's group for a linearizable read, none otherwise.
pub(super) fn producer_read_groups(
    routing: &RoutingTable,
    database_id: DatabaseId,
    collection: &str,
    linearizable: bool,
) -> crate::Result<Vec<u64>> {
    if !linearizable {
        return Ok(Vec::new());
    }
    collection_group(routing, database_id, collection).map(|group| vec![group])
}

/// Count distinct data-group leaders (the cluster's data-node count), excluding
/// the metadata group, which owns no vShards.
pub(super) fn distinct_data_node_count(routing: &RoutingTable) -> usize {
    let mut nodes: BTreeSet<u64> = BTreeSet::new();
    for group_id in routing.group_ids() {
        if group_id == METADATA_GROUP_ID {
            continue;
        }
        if let Some(info) = routing.group_info(group_id)
            && info.leader != 0
        {
            nodes.insert(info.leader);
        }
    }
    nodes.len()
}

/// Send one `ShuffleProduceRequest` and map the reply / RPC error to a typed
/// coordinator error, returning the producer's observed read versions on a
/// clean produce. Fail-fast: a producer-reported terminal error aborts. Shared
/// by both the shuffle-JOIN and shuffle-AGGREGATE resolvers, which each fold
/// the returned versions over their producers.
pub(super) async fn send_produce(
    transport: &nodedb_cluster::NexarTransport,
    node: u64,
    req: ShuffleProduceRequest,
) -> crate::Result<crate::types::ReadVersions> {
    match transport
        .send_rpc(node, RaftRpc::ShuffleProduceRequest(req))
        .await
    {
        Ok(RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse {
            error: None,
            read_versions,
        })) => Ok(crate::types::ReadVersions::from_wire(&read_versions)),
        Ok(RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: Some(e), .. })) => {
            Err(crate::Error::Internal {
                detail: format!("shuffle produce failed on node {node}: {e:?}"),
            })
        }
        Ok(other) => Err(crate::Error::Internal {
            detail: format!("shuffle produce: unexpected reply from node {node}: {other:?}"),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!("shuffle produce RPC to node {node} failed: {e}"),
        }),
    }
}

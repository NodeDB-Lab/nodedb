// SPDX-License-Identifier: BUSL-1.1

//! Forward a partition read to a node that holds a replica of it.
//!
//! The request is a typed Control-Plane operation over the authenticated
//! cluster RPC transport. It carries the caller's committed cursor. The
//! receiving node reads its local buffer after that cursor, and every replica
//! positions events alike, so the cursor means the same on both nodes.

use std::collections::HashSet;
use std::sync::Arc;

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{
    ClusterEventOp, MAX_REMOTE_CDC_COMMITTED_OFFSETS, PhysicalPlan, wire as plan_wire,
};

use crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name;
use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::offset::CdcOffset;

use super::error::ConsumeError;
use super::params::{ConsumeParams, ConsumeResult, batch_tails};

/// What a node answers to a forwarded consume.
#[derive(Debug, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum RemoteConsumeReply {
    Events(Vec<CdcEvent>),
    /// The caller's cursor lies below the events the node holds.
    OutOfRange {
        partition_id: u32,
        available_from: CdcOffset,
    },
}

impl RemoteConsumeReply {
    /// The reply for a local consume outcome. `Err` is an error the caller
    /// cannot act on as a typed reply.
    pub fn from_result(result: Result<ConsumeResult, ConsumeError>) -> Result<Self, ConsumeError> {
        match result {
            Ok(result) => Ok(Self::Events(
                result
                    .events
                    .iter()
                    .map(|event| event.as_ref().clone())
                    .collect(),
            )),
            Err(ConsumeError::OffsetOutOfRange {
                partition_id,
                available_from,
            }) => Ok(Self::OutOfRange {
                partition_id,
                available_from,
            }),
            // A node that has buffered no event of the stream yet holds none
            // after any cursor.
            Err(ConsumeError::BufferEmpty(_)) => Ok(Self::Events(Vec::new())),
            Err(error) => Err(error),
        }
    }
}

/// The node to forward a read of `partition_id` to, when this node holds no
/// voting replica of the partition's data group. `None` on every voting
/// replica, which serves the read from its own buffer, and for a partition
/// the routing names no group for.
pub(super) fn remote_partition_leader(
    state: &SharedState,
    partition_id: u32,
) -> Result<Option<u64>, ConsumeError> {
    let Some(members) = partition_members(state, partition_id)? else {
        return Ok(None);
    };
    if members.voters.contains(&state.node_id) {
        return Ok(None);
    }
    Ok(match members.leader {
        0 => members.voters.first().copied(),
        leader => Some(leader),
    })
}

/// Another voting replica of `partition_id`, the leader first, for a read
/// this node cannot serve. `None` when this node is the only one.
pub(super) fn other_replica(
    state: &SharedState,
    partition_id: u32,
) -> Result<Option<u64>, ConsumeError> {
    let Some(members) = partition_members(state, partition_id)? else {
        return Ok(None);
    };
    if members.leader != 0 && members.leader != state.node_id {
        return Ok(Some(members.leader));
    }
    Ok(members
        .voters
        .into_iter()
        .find(|node| *node != state.node_id))
}

/// A voting replica of `partition_id` other than `excluded` and this node,
/// the leader first. `None` when no such replica exists.
pub(super) fn replica_other_than(
    state: &SharedState,
    partition_id: u32,
    excluded: u64,
) -> Result<Option<u64>, ConsumeError> {
    let Some(members) = partition_members(state, partition_id)? else {
        return Ok(None);
    };
    let eligible = |node: &u64| *node != excluded && *node != state.node_id;
    if members.leader != 0 && eligible(&members.leader) {
        return Ok(Some(members.leader));
    }
    Ok(members.voters.into_iter().find(eligible))
}

struct PartitionMembers {
    leader: u64,
    voters: Vec<u64>,
}

/// The replicas of `partition_id`'s data group. `None` when the routing
/// names no group for it. A node whose routing is not wired yet refuses.
fn partition_members(
    state: &SharedState,
    partition_id: u32,
) -> Result<Option<PartitionMembers>, ConsumeError> {
    let routing_lock = state
        .cluster_routing
        .as_ref()
        .ok_or(ConsumeError::NoClusterRouting)?;
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    let Ok(group_id) = routing.group_for_vshard(partition_id) else {
        return Ok(None);
    };
    Ok(routing.group_info(group_id).map(|group| PartitionMembers {
        leader: group.leader,
        voters: group.members.clone(),
    }))
}

fn build_consume_plan(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<PhysicalPlan, ConsumeError> {
    let limit =
        u64::try_from(params.limit).map_err(|_| ConsumeError::InvalidLimit(params.limit))?;
    let stream_name = canonical_stream_name(
        state,
        params.database_id,
        params.tenant_id,
        params.stream_name,
    );
    let committed_offsets = match params.partition {
        Some(partition_id) => {
            let offset = state.offset_store.get_offset(
                params.database_id,
                params.tenant_id,
                &stream_name,
                params.group_name,
                partition_id,
            );
            vec![(partition_id, offset.epoch, offset.index, offset.sequence)]
        }
        None => state
            .offset_store
            .get_all_offsets(
                params.database_id,
                params.tenant_id,
                &stream_name,
                params.group_name,
            )
            .into_iter()
            .map(|offset| {
                (
                    offset.partition_id,
                    offset.committed_offset.epoch,
                    offset.committed_offset.index,
                    offset.committed_offset.sequence,
                )
            })
            .collect(),
    };
    if committed_offsets.len() > MAX_REMOTE_CDC_COMMITTED_OFFSETS {
        return Err(ConsumeError::InvalidRemoteOffsets(
            "too many committed partition offsets",
        ));
    }
    Ok(PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
        database_id: params.database_id,
        stream_name,
        group_name: params.group_name.to_owned(),
        partition: params.partition,
        limit,
        committed_offsets,
    }))
}

/// Decode and validate caller-owned CDC offsets carried by a cluster plan,
/// as `(partition, epoch, index, sequence)`.
///
/// Duplicate partition identifiers fail closed rather than allowing the map
/// construction to silently select an arbitrary cursor.
pub fn decode_remote_committed_offsets(
    offsets: &[(u32, u64, u64, u64)],
) -> Result<Vec<(u32, CdcOffset)>, ConsumeError> {
    if offsets.len() > MAX_REMOTE_CDC_COMMITTED_OFFSETS {
        return Err(ConsumeError::InvalidRemoteOffsets(
            "too many committed partition offsets",
        ));
    }
    let mut partitions = HashSet::with_capacity(offsets.len());
    let mut decoded = Vec::with_capacity(offsets.len());
    for &(partition_id, epoch, index, sequence) in offsets {
        if !partitions.insert(partition_id) {
            return Err(ConsumeError::InvalidRemoteOffsets(
                "duplicate committed partition offset",
            ));
        }
        decoded.push((partition_id, CdcOffset::at(epoch, index, sequence)));
    }
    Ok(decoded)
}

/// Forward a consume request to a node that holds a replica of the
/// partition. A node that lacks the events below the caller's cursor answers
/// `ConsumeError::OffsetOutOfRange`.
///
/// The authenticated cluster RPC carries a typed Control-Plane operation;
/// reconstructed SQL is deliberately not used for Event-Plane routing.
pub async fn consume_remote(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    leader_node: u64,
) -> Result<ConsumeResult, ConsumeError> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or(ConsumeError::NoClusterTransport)?;
    let plan = build_consume_plan(state, params)?;
    let plan_bytes =
        plan_wire::encode(&plan).map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let request = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: params.tenant_id,
        database_id: params.database_id.as_u64(),
        deadline_remaining_ms: 30_000,
        trace_id: nodedb_types::TraceId::generate().0,
        descriptor_versions: Vec::new(),
        txn_id: None,
        vshard_id: None,
        read_groups: Vec::new(),
    });
    let response = transport
        .send_rpc(leader_node, request)
        .await
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let payload = match response {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            payloads,
            ..
        }) => payloads.into_iter().next().ok_or_else(|| {
            ConsumeError::RemoteError("remote CDC consume returned no payload".into())
        })?,
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(error), ..
        }) => return Err(ConsumeError::RemoteError(format!("{error:?}"))),
        RaftRpc::ExecuteResponse(_) => {
            return Err(ConsumeError::RemoteError(
                "remote CDC consume returned an empty error".into(),
            ));
        }
        _ => {
            return Err(ConsumeError::RemoteError(
                "remote CDC consume returned an unexpected response".into(),
            ));
        }
    };
    crate::util::bounded_msgpack::read_value(&payload)
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let events = match zerompk::from_msgpack::<RemoteConsumeReply>(&payload)
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?
    {
        RemoteConsumeReply::Events(events) => events.into_iter().map(Arc::new).collect::<Vec<_>>(),
        RemoteConsumeReply::OutOfRange {
            partition_id,
            available_from,
        } => {
            return Err(ConsumeError::OffsetOutOfRange {
                partition_id,
                available_from,
            });
        }
    };

    Ok(ConsumeResult {
        partition_offsets: batch_tails(&events),
        events,
        // The remote node computed its own eviction delta and cannot return
        // it here. Zero is the conservative value, never a fabricated one.
        evicted_since_last_poll: 0,
        oldest_available_offset: CdcOffset::ZERO,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::consume::local::consume_local_with_offsets;

    #[test]
    fn build_consume_plan_carries_callers_exact_partition_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let params = ConsumeParams {
            database_id: crate::types::DatabaseId::new(9),
            tenant_id: 1,
            stream_name: "orders_stream",
            group_name: "analytics",
            partition: Some(5),
            limit: 100,
        };
        state
            .offset_store
            .commit_offset(
                params.database_id,
                params.tenant_id,
                params.stream_name,
                params.group_name,
                5,
                CdcOffset::new(42, 3),
            )
            .expect("commit caller offset");

        assert_eq!(
            build_consume_plan(&state, &params).expect("typed consume plan"),
            PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
                database_id: params.database_id,
                stream_name: params.stream_name.to_owned(),
                group_name: params.group_name.to_owned(),
                partition: Some(5),
                limit: 100,
                committed_offsets: vec![(5, 0, 42, 3)],
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn partitionless_remote_plan_carries_all_caller_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let params = ConsumeParams {
            database_id: crate::types::DatabaseId::new(9),
            tenant_id: 1,
            stream_name: "orders_stream",
            group_name: "analytics",
            partition: None,
            limit: 50,
        };
        for (partition, offset) in [(2, CdcOffset::new(8, 1)), (4, CdcOffset::new(9, 2))] {
            state
                .offset_store
                .commit_offset(
                    params.database_id,
                    params.tenant_id,
                    params.stream_name,
                    params.group_name,
                    partition,
                    offset,
                )
                .expect("commit caller offset");
        }

        let PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            partition,
            committed_offsets,
            ..
        }) = build_consume_plan(&state, &params).expect("partitionless plan")
        else {
            panic!("expected cluster CDC consume plan");
        };
        assert_eq!(partition, None);
        assert_eq!(committed_offsets, vec![(2, 0, 8, 1), (4, 0, 9, 2)]);
    }

    #[test]
    fn remote_offset_decoder_rejects_duplicate_or_oversized_partitions() {
        assert!(matches!(
            decode_remote_committed_offsets(&[(1, 0, 1, 1), (1, 0, 2, 1)]),
            Err(ConsumeError::InvalidRemoteOffsets(_))
        ));
        let oversized = vec![(0, 0, 0, 0); MAX_REMOTE_CDC_COMMITTED_OFFSETS + 1];
        assert!(matches!(
            decode_remote_committed_offsets(&oversized),
            Err(ConsumeError::InvalidRemoteOffsets(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remote_consume_uses_callers_committed_cursor_after_local_commit() {
        let caller_dir = tempfile::tempdir().expect("caller tempdir");
        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let (_, _, caller, _, _) = crate::event::test_utils::event_test_deps(&caller_dir);
        let (_, _, remote, _, _) = crate::event::test_utils::event_test_deps(&remote_dir);
        let database_id = crate::types::DatabaseId::DEFAULT;
        let tenant_id = 1;
        let stream = "orders";
        let group = "analytics";
        let retention = crate::event::cdc::stream_def::RetentionConfig {
            max_events: 10,
            max_age_secs: 60,
        };
        let buffer = remote
            .cdc_router
            .ensure_buffer(database_id, tenant_id, stream, &retention);
        let event_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        for sequence in 1..=2 {
            buffer.push(CdcEvent {
                sequence,
                partition: 3,
                collection: stream.into(),
                op: "INSERT".into(),
                row_id: format!("row-{sequence}"),
                event_time,
                lsn: 10,
                index: 10,
                epoch: 0,
                database_id,
                tenant_id,
                new_value: None,
                old_value: None,
                schema_version: 0,
                field_diffs: None,
                system_time_ms: None,
                valid_time_ms: None,
                source: crate::event::EventSource::User,
            });
        }
        let params = ConsumeParams {
            database_id,
            tenant_id,
            stream_name: stream,
            group_name: group,
            partition: Some(3),
            limit: 1,
        };

        let first_plan = build_consume_plan(&caller, &params).expect("first remote plan");
        let PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            committed_offsets, ..
        }) = first_plan
        else {
            panic!("expected cluster CDC consume plan");
        };
        let first_offsets = decode_remote_committed_offsets(&committed_offsets).expect("offsets");
        let first = consume_local_with_offsets(&remote, &params, Some(&first_offsets))
            .expect("first remote consume");
        assert_eq!(first.events[0].offset_token(), "0:10:1");

        caller
            .offset_store
            .commit_offset(
                database_id,
                tenant_id,
                stream,
                group,
                3,
                CdcOffset::new(10, 1),
            )
            .expect("commit on caller only");
        let second_plan = build_consume_plan(&caller, &params).expect("second remote plan");
        let PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            committed_offsets, ..
        }) = second_plan
        else {
            panic!("expected cluster CDC consume plan");
        };
        let second_offsets = decode_remote_committed_offsets(&committed_offsets).expect("offsets");
        let second = consume_local_with_offsets(&remote, &params, Some(&second_offsets))
            .expect("second remote consume");
        assert_eq!(second.events[0].offset_token(), "0:10:2");
        assert_eq!(
            remote
                .offset_store
                .get_offset(database_id, tenant_id, stream, group, 3),
            CdcOffset::ZERO
        );
    }

    /// A node whose routing is not wired refuses to place a partition read.
    #[tokio::test]
    async fn a_node_without_routing_refuses_to_place_a_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(matches!(
            remote_partition_leader(&state, 5),
            Err(ConsumeError::NoClusterRouting)
        ));
    }

    /// A one-node cluster replicates every group, so it serves every
    /// partition from its own buffer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_one_node_cluster_serves_every_partition_itself() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        assert_eq!(
            remote_partition_leader(&cluster.state, 5).expect("placed"),
            None
        );
        assert_eq!(other_replica(&cluster.state, 5).expect("placed"), None);
        cluster.shutdown().await;
    }

    #[test]
    fn an_out_of_range_reply_round_trips() {
        let reply = RemoteConsumeReply::OutOfRange {
            partition_id: 6,
            available_from: CdcOffset::at(1, 88, 0),
        };
        let bytes = zerompk::to_msgpack_vec(&reply).expect("encode reply");
        match zerompk::from_msgpack::<RemoteConsumeReply>(&bytes).expect("decode reply") {
            RemoteConsumeReply::OutOfRange {
                partition_id,
                available_from,
            } => {
                assert_eq!(partition_id, 6);
                assert_eq!(available_from, CdcOffset::at(1, 88, 0));
            }
            RemoteConsumeReply::Events(_) => panic!("expected an out-of-range reply"),
        }
    }
}

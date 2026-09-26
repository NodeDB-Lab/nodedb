// SPDX-License-Identifier: BUSL-1.1

//! Shared stream consumption logic.
//!
//! Used by both HTTP endpoints and pgwire SELECT to read events from a
//! change stream's buffer using consumer group offsets.
//!
//! **Cluster-wide:** When a specific partition is requested and the vShard
//! leader for that partition is on another node, the request is forwarded
//! as a typed operation over the authenticated cluster RPC transport. The
//! remote Control Plane reads its local Event-Plane buffer and returns the
//! serialized events.

use tracing::debug;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::offset::CdcOffset;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{
    ClusterEventOp, MAX_REMOTE_CDC_COMMITTED_OFFSETS, PhysicalPlan, wire as plan_wire,
};

/// Parameters for consuming events from a stream.
pub struct ConsumeParams<'a> {
    pub database_id: crate::types::DatabaseId,
    pub tenant_id: u64,
    pub stream_name: &'a str,
    pub group_name: &'a str,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u32>,
    /// Maximum events to return.
    pub limit: usize,
}

/// Result of consuming events from a stream.
pub struct ConsumeResult {
    /// The events read from the buffer. Events are shared `Arc<CdcEvent>`
    /// so consumer fan-out (webhook, Kafka, SHOW, commit) doesn't deep-clone.
    pub events: Vec<Arc<CdcEvent>>,
    /// Per-partition latest composite position seen in this batch.
    pub partition_offsets: Vec<(u32, CdcOffset)>,
    /// Number of events dropped from this stream's buffer since the consumer
    /// group's previous poll. Zero on the first ever poll for this group, or
    /// when no evictions have occurred.
    pub evicted_since_last_poll: u64,
    /// Oldest composite position still available in the stream buffer. The
    /// initial position means the buffer is empty.
    pub oldest_available_offset: CdcOffset,
}

/// Consume events from a change stream using consumer group offsets.
///
/// Reads events strictly after each partition's committed composite offset.
/// Does NOT auto-commit offsets — the caller must explicitly COMMIT OFFSET.
///
/// **Cluster-aware:** If a specific partition is requested and the vShard
/// leader is remote, returns `ConsumeError::RemotePartition` so the caller
/// can use `consume_remote` over the authenticated cluster transport.
pub fn consume_stream(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    let mut canonical_stream = crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name(
        state,
        params.database_id,
        params.tenant_id,
        params.stream_name,
    );
    if let Some(topic_name) = canonical_stream.strip_prefix("topic:") {
        // Migration mutates catalog, registry, and the separate offset store.
        // It must serialize with DROP TOPIC exactly like DDL does: topic first,
        // then canonical and legacy group identities in that fixed order.
        let topic_lock = state.ep_topic_registry.lifecycle_lock(
            params.database_id,
            params.tenant_id,
            topic_name,
        );
        let _topic_guard = topic_lock
            .try_lock()
            .map_err(|_| ConsumeError::LifecycleBusy)?;
        canonical_stream = crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name(
            state, params.database_id, params.tenant_id, params.stream_name,
        );
        if canonical_stream.starts_with("topic:") {
            let legacy_stream = canonical_stream.trim_start_matches("topic:");
            let canonical_lock = state.group_registry.lifecycle_lock(
                params.database_id,
                params.tenant_id,
                &canonical_stream,
                params.group_name,
            );
            let _canonical_guard = canonical_lock
                .try_lock()
                .map_err(|_| ConsumeError::LifecycleBusy)?;
            let legacy_lock = state.group_registry.lifecycle_lock(
                params.database_id,
                params.tenant_id,
                legacy_stream,
                params.group_name,
            );
            let _legacy_guard = legacy_lock
                .try_lock()
                .map_err(|_| ConsumeError::LifecycleBusy)?;
            crate::control::server::shared::ddl::neutral::consumer_group::identity::migrate_legacy_topic_group(
                state, params.database_id, params.tenant_id, &canonical_stream, params.group_name,
            ).map_err(|error| ConsumeError::RemoteError(error.message))?;
        }
    }
    let params = ConsumeParams {
        database_id: params.database_id,
        tenant_id: params.tenant_id,
        stream_name: &canonical_stream,
        group_name: params.group_name,
        partition: params.partition,
        limit: params.limit,
    };

    validate_consume_identity(state, &params)?;

    // Cluster-aware: check if the requested partition is remote.
    if let Some(partition_id) = params.partition
        && let Some(remote_node) = remote_partition_leader(state, partition_id)
    {
        debug!(
            partition = partition_id,
            remote_node,
            stream = params.stream_name,
            "partition is remote — forwarding consume request"
        );
        return Err(ConsumeError::RemotePartition {
            partition_id,
            leader_node: remote_node,
        });
    }

    // Local consumption path.
    consume_local(state, &params)
}

/// Validate the stream and consumer-group identity before a local buffer read.
///
/// Cluster receivers call this after envelope validation so an authenticated
/// peer cannot consume an arbitrary local buffer with a fabricated group.
pub fn validate_consume_identity(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<(), ConsumeError> {
    // Topics use buffer keys with the "topic:" prefix. When the stream name
    // already carries that prefix, accept it only for a registered topic.
    let stream_exists = state
        .stream_registry
        .get(params.database_id, params.tenant_id, params.stream_name)
        .is_some();
    let topic_exists = params
        .stream_name
        .strip_prefix("topic:")
        .is_some_and(|bare| {
            state
                .ep_topic_registry
                .get(params.database_id, params.tenant_id, bare)
                .is_some()
        });
    if !stream_exists && !topic_exists {
        return Err(ConsumeError::StreamNotFound(params.stream_name.to_string()));
    }
    if state
        .group_registry
        .get(
            params.database_id,
            params.tenant_id,
            params.stream_name,
            params.group_name,
        )
        .is_none()
    {
        return Err(ConsumeError::GroupNotFound(
            params.group_name.to_string(),
            params.stream_name.to_string(),
        ));
    }
    Ok(())
}

/// Consume events from a local stream buffer.
///
/// This is the core logic, always reads from the local `CdcRouter` buffers.
/// Used directly for local partitions and by `consume_remote` on the remote
/// node after the gateway routes and executes the stream SELECT.
pub fn consume_local(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    consume_local_with_offsets(state, params, None)
}

/// Consume from a local buffer using caller-supplied committed offsets when
/// present. Cluster RPC receivers use this so a remote buffer is read from the
/// caller node's consumer-group cursor; ordinary local consumers pass `None`
/// and continue to use their local [`OffsetStore`](crate::event::cdc::OffsetStore).
pub fn consume_local_with_offsets(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    committed_offsets: Option<&[(u32, CdcOffset)]>,
) -> Result<ConsumeResult, ConsumeError> {
    // Get the stream buffer.
    let buffer = state
        .cdc_router
        .get_buffer(params.database_id, params.tenant_id, params.stream_name)
        .ok_or_else(|| ConsumeError::BufferEmpty(params.stream_name.to_string()))?;

    // Read events based on committed offsets.
    let events = if let Some(partition_id) = params.partition {
        // Single partition read. A missing supplied cursor intentionally starts
        // at ZERO, as does a locally uncommitted partition.
        let from = match committed_offsets {
            Some(offsets) => offsets
                .iter()
                .find_map(|(id, offset)| (*id == partition_id).then_some(*offset))
                .unwrap_or(CdcOffset::ZERO),
            None => state.offset_store.get_offset(
                params.database_id,
                params.tenant_id,
                params.stream_name,
                params.group_name,
                partition_id,
            ),
        };
        buffer.read_partition_from(partition_id, from, params.limit)
    } else {
        // Apply each partition cursor independently. A shared minimum LSN can
        // skip an uncommitted same-LSN sibling behind LIMIT, or redeliver an
        // already acknowledged partition.
        let offsets: HashMap<u32, CdcOffset> = match committed_offsets {
            Some(offsets) => offsets.iter().copied().collect(),
            None => state
                .offset_store
                .get_all_offsets(
                    params.database_id,
                    params.tenant_id,
                    params.stream_name,
                    params.group_name,
                )
                .into_iter()
                .map(|offset| (offset.partition_id, offset.committed_offset))
                .collect(),
        };
        buffer.read_after_partition_offsets(&offsets, params.limit)
    };

    // Compute the exact per-partition tail for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u32, CdcOffset> =
        std::collections::BTreeMap::new();
    for event in &events {
        let entry = partition_offsets
            .entry(event.partition)
            .or_insert(CdcOffset::ZERO);
        if event.position() > *entry {
            *entry = event.position();
        }
    }

    // Remote consumers own their eviction baseline too: do not mutate the
    // receiver's OffsetStore when an RPC supplied caller-owned cursors.
    let evicted_since_last_poll = match committed_offsets {
        Some(_) => 0,
        None => state.offset_store.swap_eviction_baseline(
            params.database_id,
            params.tenant_id,
            params.stream_name,
            params.group_name,
            buffer.total_evicted(),
        ),
    };
    let oldest_available_offset = buffer.earliest_offset().unwrap_or(CdcOffset::ZERO);

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
        evicted_since_last_poll,
        oldest_available_offset,
    })
}

/// Check if a partition's vShard leader is on a remote node.
///
/// Returns `Some(remote_node_id)` if the leader is remote, `None` if local
/// or if we're in single-node mode.
fn remote_partition_leader(state: &SharedState, partition_id: u32) -> Option<u64> {
    let routing_lock = state.cluster_routing.as_ref()?;
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    let leader = routing.leader_for_vshard(partition_id).ok()?;
    if leader == state.node_id || leader == 0 {
        None // Local or no leader known.
    } else {
        Some(leader)
    }
}

fn build_consume_plan(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<PhysicalPlan, ConsumeError> {
    let limit =
        u64::try_from(params.limit).map_err(|_| ConsumeError::InvalidLimit(params.limit))?;
    let stream_name = crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name(
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
            vec![(partition_id, offset.lsn, offset.sequence)]
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
                    offset.committed_offset.lsn,
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

/// Decode and validate caller-owned CDC offsets carried by a cluster plan.
///
/// Duplicate partition identifiers fail closed rather than allowing the map
/// construction to silently select an arbitrary cursor.
pub fn decode_remote_committed_offsets(
    offsets: &[(u32, u64, u64)],
) -> Result<Vec<(u32, CdcOffset)>, ConsumeError> {
    if offsets.len() > MAX_REMOTE_CDC_COMMITTED_OFFSETS {
        return Err(ConsumeError::InvalidRemoteOffsets(
            "too many committed partition offsets",
        ));
    }
    let mut partitions = HashSet::with_capacity(offsets.len());
    let mut decoded = Vec::with_capacity(offsets.len());
    for &(partition_id, lsn, sequence) in offsets {
        if !partitions.insert(partition_id) {
            return Err(ConsumeError::InvalidRemoteOffsets(
                "duplicate committed partition offset",
            ));
        }
        decoded.push((partition_id, CdcOffset::new(lsn, sequence)));
    }
    Ok(decoded)
}

/// Forward a consume request directly to the remote partition leader.
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
    let events = zerompk::from_msgpack::<Vec<CdcEvent>>(&payload)
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Compute the exact per-partition tail for the remotely serialized batch.
    let mut partition_offsets: std::collections::BTreeMap<u32, CdcOffset> =
        std::collections::BTreeMap::new();
    for event in &events {
        let entry = partition_offsets
            .entry(event.partition)
            .or_insert(CdcOffset::ZERO);
        if event.position() > *entry {
            *entry = event.position();
        }
    }

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
        // For remote consumes the eviction metadata comes from the remote node.
        // The remote `consume_local` path already computed the delta on that
        // node; we cannot reconstruct it here. Surface 0 so callers always get
        // a valid (conservative) value rather than stale or fabricated data.
        evicted_since_last_poll: 0,
        oldest_available_offset: CdcOffset::ZERO,
    })
}

/// Errors from stream consumption.
#[derive(Debug)]
pub enum ConsumeError {
    StreamNotFound(String),
    GroupNotFound(String, String),
    /// Stream exists but buffer is empty (no events yet).
    BufferEmpty(String),
    /// Partition is on a remote node — caller should use `consume_remote()`.
    RemotePartition {
        partition_id: u32,
        leader_node: u64,
    },
    /// Remote consume failed.
    RemoteError(String),
    /// Gateway not available (cluster transport not ready).
    NoClusterTransport,
    /// Requested LIMIT cannot be represented by the SQL integer type.
    InvalidLimit(usize),
    /// Caller-provided cluster cursor vector is malformed or exceeds its bound.
    InvalidRemoteOffsets(&'static str),
    /// A concurrent topic/group lifecycle transition owns the required locks.
    /// The caller must retry rather than read or migrate a mixed incarnation.
    LifecycleBusy,
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamNotFound(s) => write!(f, "change stream '{s}' does not exist"),
            Self::GroupNotFound(g, s) => {
                write!(f, "consumer group '{g}' does not exist on stream '{s}'")
            }
            Self::BufferEmpty(s) => write!(f, "stream '{s}' has no buffered events"),
            Self::RemotePartition {
                partition_id,
                leader_node,
            } => {
                write!(
                    f,
                    "partition {partition_id} is on remote node {leader_node}"
                )
            }
            Self::RemoteError(e) => write!(f, "remote consume error: {e}"),
            Self::NoClusterTransport => {
                write!(f, "cluster transport not available for remote stream read")
            }
            Self::InvalidLimit(limit) => {
                write!(f, "stream LIMIT {limit} exceeds cluster wire range")
            }
            Self::InvalidRemoteOffsets(reason) => {
                write!(f, "invalid remote CDC committed offsets: {reason}")
            }
            Self::LifecycleBusy => write!(
                f,
                "topic or consumer-group lifecycle transition is in progress"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_error_display() {
        let e = ConsumeError::StreamNotFound("orders".into());
        assert!(e.to_string().contains("orders"));
    }

    #[test]
    fn remote_partition_error_display() {
        let e = ConsumeError::RemotePartition {
            partition_id: 5,
            leader_node: 3,
        };
        assert!(e.to_string().contains("partition 5"));
        assert!(e.to_string().contains("node 3"));
    }

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
                committed_offsets: vec![(5, 42, 3)],
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
        assert_eq!(committed_offsets, vec![(2, 8, 1), (4, 9, 2)]);
    }

    #[test]
    fn remote_offset_decoder_rejects_duplicate_or_oversized_partitions() {
        assert!(matches!(
            decode_remote_committed_offsets(&[(1, 1, 1), (1, 2, 1)]),
            Err(ConsumeError::InvalidRemoteOffsets(_))
        ));
        let oversized = vec![(0, 0, 0); MAX_REMOTE_CDC_COMMITTED_OFFSETS + 1];
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
        assert_eq!(first.events[0].offset_token(), "10:1");

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
        assert_eq!(second.events[0].offset_token(), "10:2");
        assert_eq!(
            remote
                .offset_store
                .get_offset(database_id, tenant_id, stream, group, 3),
            CdcOffset::ZERO
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn topic_consume_commit_then_consume_uses_one_canonical_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let database_id = crate::types::DatabaseId::DEFAULT;
        let tenant_id = 1;
        let topic = "orders";
        let stream = "topic:orders";
        let group = "analytics";
        let retention = crate::event::cdc::stream_def::RetentionConfig {
            max_events: 10,
            max_age_secs: 60,
        };
        state
            .ep_topic_registry
            .register(crate::event::topic::TopicDef {
                database_id,
                tenant_id,
                name: topic.into(),
                retention: retention.clone(),
                owner: "admin".into(),
                created_at: 0,
                last_sequence: 0,
                last_lsn: 0,
            });
        state
            .group_registry
            .register(crate::event::cdc::consumer_group::ConsumerGroupDef {
                database_id,
                tenant_id,
                name: group.into(),
                stream_name: stream.into(),
                owner: "admin".into(),
                created_at: 0,
            });
        let event_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        state
            .cdc_router
            .ensure_buffer(database_id, tenant_id, stream, &retention)
            .push(CdcEvent {
                sequence: 1,
                partition: 0,
                collection: stream.into(),
                op: "PUBLISH".into(),
                row_id: "msg-1".into(),
                event_time,
                lsn: 1,
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
        state
            .cdc_router
            .get_buffer(database_id, tenant_id, stream)
            .expect("topic buffer")
            .push(CdcEvent {
                sequence: 2,
                partition: 0,
                collection: stream.into(),
                op: "PUBLISH".into(),
                row_id: "msg-2".into(),
                event_time,
                lsn: 1,
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

        let params = ConsumeParams {
            database_id,
            tenant_id,
            stream_name: topic,
            group_name: group,
            partition: Some(0),
            limit: 1,
        };
        let first = consume_stream(&state, &params).expect("first consume");
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].offset_token(), "1:1");
        state
            .offset_store
            .commit_offset(
                database_id,
                tenant_id,
                stream,
                group,
                0,
                CdcOffset::new(1, 1),
            )
            .expect("commit canonical offset");
        let second = consume_stream(&state, &params).expect("second consume");
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].offset_token(), "1:2");
    }

    #[tokio::test]
    async fn single_node_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        // No cluster_routing → always local.
        assert!(remote_partition_leader(&state, 5).is_none());
    }
}

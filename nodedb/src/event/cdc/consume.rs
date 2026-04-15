//! Shared stream consumption logic.
//!
//! Used by both HTTP endpoints and pgwire SELECT to read events from a
//! change stream's buffer using consumer group offsets.
//!
//! **Cluster-wide:** When a specific partition is requested and the vShard
//! leader for that partition is on another node, the request is forwarded
//! via `gateway.execute_sql` (C-δ.6). The remote node executes the stream
//! SELECT locally and returns serialised events. This makes change streams
//! cluster-wide — consumers on any node can read any partition.

use tracing::debug;

use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;

/// Parameters for consuming events from a stream.
pub struct ConsumeParams<'a> {
    pub tenant_id: u32,
    pub stream_name: &'a str,
    pub group_name: &'a str,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u16>,
    /// Maximum events to return.
    pub limit: usize,
}

/// Result of consuming events from a stream.
pub struct ConsumeResult {
    /// The events read from the buffer.
    pub events: Vec<CdcEvent>,
    /// Per-partition latest LSN seen in this batch (for offset tracking).
    pub partition_offsets: Vec<(u16, u64)>,
}

/// Consume events from a change stream using consumer group offsets.
///
/// Reads events with LSN > the group's committed offset for each partition.
/// Does NOT auto-commit offsets — the caller must explicitly COMMIT OFFSET.
///
/// **Cluster-aware:** If a specific partition is requested and the vShard
/// leader is remote, returns `ConsumeError::RemotePartition` so the caller
/// can use `consume_remote` which routes through `gateway.execute_sql`.
pub fn consume_stream(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    // Verify stream exists.
    if state
        .stream_registry
        .get(params.tenant_id, params.stream_name)
        .is_none()
    {
        return Err(ConsumeError::StreamNotFound(params.stream_name.to_string()));
    }

    // Verify consumer group exists.
    if state
        .group_registry
        .get(params.tenant_id, params.stream_name, params.group_name)
        .is_none()
    {
        return Err(ConsumeError::GroupNotFound(
            params.group_name.to_string(),
            params.stream_name.to_string(),
        ));
    }

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
    consume_local(state, params)
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
    // Get the stream buffer.
    let buffer = state
        .cdc_router
        .get_buffer(params.tenant_id, params.stream_name)
        .ok_or_else(|| ConsumeError::BufferEmpty(params.stream_name.to_string()))?;

    // Read events based on committed offsets.
    let events = if let Some(partition_id) = params.partition {
        // Single partition read.
        let from_lsn = state.offset_store.get_offset(
            params.tenant_id,
            params.stream_name,
            params.group_name,
            partition_id,
        );
        buffer.read_partition_from_lsn(partition_id, from_lsn, params.limit)
    } else {
        // All partitions: read from the minimum committed offset.
        // Each event's partition field lets consumers track per-partition progress.
        let all_offsets = state.offset_store.get_all_offsets(
            params.tenant_id,
            params.stream_name,
            params.group_name,
        );
        // Use the minimum offset across all committed partitions, or 0 if none committed.
        let min_lsn = all_offsets
            .iter()
            .map(|o| o.committed_lsn)
            .min()
            .unwrap_or(0);
        buffer.read_from_lsn(min_lsn, params.limit)
    };

    // Compute per-partition max LSN for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u16, u64> =
        std::collections::BTreeMap::new();
    for e in &events {
        let entry = partition_offsets.entry(e.partition).or_insert(0);
        if e.lsn > *entry {
            *entry = e.lsn;
        }
    }

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
    })
}

/// Check if a partition's vShard leader is on a remote node.
///
/// Returns `Some(remote_node_id)` if the leader is remote, `None` if local
/// or if we're in single-node mode.
fn remote_partition_leader(state: &SharedState, partition_id: u16) -> Option<u64> {
    let routing_lock = state.cluster_routing.as_ref()?;
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    let leader = routing.leader_for_vshard(partition_id).ok()?;
    if leader == state.node_id || leader == 0 {
        None // Local or no leader known.
    } else {
        Some(leader)
    }
}

/// Build a SQL statement for forwarding a consume request to a remote node.
///
/// The remote node executes this as a normal SQL query, which routes back
/// through the pgwire handler → `consume_stream()` → local buffer read.
pub fn build_consume_sql(params: &ConsumeParams<'_>) -> String {
    // For topic buffers, the stream name already has "topic:" prefix handled
    // by the DDL layer. We forward the raw stream/topic name.
    if let Some(partition_id) = params.partition {
        format!(
            "SELECT * FROM STREAM {} PARTITION {} CONSUMER GROUP {} LIMIT {}",
            params.stream_name, partition_id, params.group_name, params.limit
        )
    } else {
        format!(
            "SELECT * FROM STREAM {} CONSUMER GROUP {} LIMIT {}",
            params.stream_name, params.group_name, params.limit
        )
    }
}

/// Forward a consume request to the remote partition leader via the gateway.
///
/// Routes the stream SELECT SQL through `gateway.execute_sql`, which plans it
/// locally and dispatches it as an `ExecuteRequest` over QUIC to the correct
/// leader node. The `leader_node` parameter is accepted for caller
/// compatibility but is ignored — the gateway handles node selection.
pub async fn consume_remote(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    _leader_node: u64,
) -> Result<ConsumeResult, ConsumeError> {
    let gateway = state
        .gateway
        .as_ref()
        .ok_or(ConsumeError::NoClusterTransport)?;

    let sql = build_consume_sql(params);
    let tenant_id = params.tenant_id;

    let gw_ctx = crate::control::gateway::core::QueryContext {
        tenant_id: crate::types::TenantId::new(tenant_id),
        trace_id: 0,
    };

    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);

    let payloads = gateway
        .execute_sql(&gw_ctx, &sql, &[], || {
            let tasks = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(query_ctx.plan_sql(&sql, crate::types::TenantId::new(tenant_id)))
            })
            .map_err(|e| crate::Error::PlanError {
                detail: e.to_string(),
            })?;
            // Take the first task's plan (stream reads are single-task).
            tasks
                .into_iter()
                .next()
                .map(|t| t.plan)
                .ok_or_else(|| crate::Error::PlanError {
                    detail: "stream SELECT produced no physical tasks".into(),
                })
        })
        .await
        .map_err(|e| ConsumeError::RemoteError(e.to_string()))?;

    // Deserialize events from the response payloads.
    // Payloads contain msgpack-serialised Vec<CdcEvent>.
    let events = if let Some(payload) = payloads.first() {
        zerompk::from_msgpack::<Vec<CdcEvent>>(payload).unwrap_or_default()
    } else {
        Vec::new()
    };

    // Compute per-partition max LSN for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u16, u64> =
        std::collections::BTreeMap::new();
    for e in &events {
        let entry = partition_offsets.entry(e.partition).or_insert(0);
        if e.lsn > *entry {
            *entry = e.lsn;
        }
    }

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
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
        partition_id: u16,
        leader_node: u64,
    },
    /// Remote consume failed.
    RemoteError(String),
    /// Gateway not available (cluster transport not ready).
    NoClusterTransport,
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
            Self::NoClusterTransport => write!(f, "gateway not available for remote stream read"),
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
    fn build_consume_sql_with_partition() {
        let params = ConsumeParams {
            tenant_id: 1,
            stream_name: "orders_stream",
            group_name: "analytics",
            partition: Some(5),
            limit: 100,
        };
        let sql = build_consume_sql(&params);
        assert_eq!(
            sql,
            "SELECT * FROM STREAM orders_stream PARTITION 5 CONSUMER GROUP analytics LIMIT 100"
        );
    }

    #[test]
    fn build_consume_sql_all_partitions() {
        let params = ConsumeParams {
            tenant_id: 1,
            stream_name: "orders_stream",
            group_name: "analytics",
            partition: None,
            limit: 50,
        };
        let sql = build_consume_sql(&params);
        assert_eq!(
            sql,
            "SELECT * FROM STREAM orders_stream CONSUMER GROUP analytics LIMIT 50"
        );
    }

    #[test]
    fn single_node_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        // No cluster_routing → always local.
        assert!(remote_partition_leader(&state, 5).is_none());
    }
}

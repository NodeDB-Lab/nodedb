// SPDX-License-Identifier: BUSL-1.1

//! Read a change stream from this node's buffers.
//!
//! Every replica of a data group routes the group's committed writes into its
//! own buffers, at the same positions (see `CdcRouter`). A consumer cursor is
//! therefore valid on every replica. A replica that lags the leader returns a
//! shorter prefix of the same sequence, never a different one, so a consumer
//! that moves between nodes or across a leader change neither skips nor
//! re-reads an event.

use std::collections::HashMap;

use crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name;
use crate::control::state::SharedState;
use crate::event::cdc::offset::CdcOffset;

use super::error::ConsumeError;
use super::params::{ConsumeParams, ConsumeResult, batch_tails};
use super::remote::{other_replica, remote_partition_leader};

/// Consume events from a change stream using consumer group offsets.
///
/// Reads events strictly after each partition's committed composite offset.
/// Does NOT auto-commit offsets — the caller must explicitly COMMIT OFFSET.
///
/// A partition read that names a partition this node holds no replica of
/// returns `ConsumeError::RemotePartition`, and the caller forwards it with
/// `consume_remote` over the authenticated cluster transport. So does a read
/// whose cursor lies below the events this node holds, when another replica
/// of the partition exists. With no such replica, the read fails with
/// `ConsumeError::OffsetOutOfRange`, never a silent skip.
pub async fn consume_stream(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    let canonical_stream = resolve_stream(state, params);
    let params = ConsumeParams {
        database_id: params.database_id,
        tenant_id: params.tenant_id,
        stream_name: &canonical_stream,
        group_name: params.group_name,
        partition: params.partition,
        limit: params.limit,
    };

    validate_consume_identity(state, &params)?;

    if let Some(partition_id) = params.partition
        && let Some(remote_node) = remote_partition_leader(state, partition_id)?
    {
        tracing::debug!(
            partition = partition_id,
            remote_node,
            stream = params.stream_name,
            "no local replica of the partition — forwarding consume request"
        );
        return Err(ConsumeError::RemotePartition {
            partition_id,
            leader_node: remote_node,
        });
    }

    match consume_local(state, &params) {
        // This node lacks the events a snapshot install covered. Another
        // replica of the partition that holds them serves the read.
        Err(ConsumeError::OffsetOutOfRange {
            partition_id,
            available_from,
        }) => match other_replica(state, partition_id)? {
            Some(node) => Err(ConsumeError::RemotePartition {
                partition_id,
                leader_node: node,
            }),
            None => Err(ConsumeError::OffsetOutOfRange {
                partition_id,
                available_from,
            }),
        },
        result => result,
    }
}

/// The canonical stream name for `params`.
fn resolve_stream(state: &SharedState, params: &ConsumeParams<'_>) -> String {
    canonical_stream_name(
        state,
        params.database_id,
        params.tenant_id,
        params.stream_name,
    )
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

/// Consume events from a local stream buffer, after this node's committed
/// offsets. The offset store is replicated, so these are the group's
/// cluster-wide offsets.
pub fn consume_local(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    consume_local_with_offsets(state, params, None)
}

/// Consume from a local buffer using caller-supplied committed offsets when
/// present. Cluster RPC receivers use this so a remote buffer is read from the
/// caller node's consumer-group cursor; ordinary local consumers pass `None`
/// and use the local [`OffsetStore`](crate::event::cdc::OffsetStore).
pub fn consume_local_with_offsets(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    committed_offsets: Option<&[(u32, CdcOffset)]>,
) -> Result<ConsumeResult, ConsumeError> {
    // Apply each partition cursor independently. A shared minimum position
    // can skip an uncommitted sibling behind LIMIT, or redeliver an already
    // acknowledged partition. A missing cursor starts at ZERO.
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
    let cursor = |partition: u32| offsets.get(&partition).copied().unwrap_or(CdcOffset::ZERO);
    ensure_available(state, params.partition, &cursor)?;

    let buffer = state
        .cdc_router
        .get_buffer(params.database_id, params.tenant_id, params.stream_name)
        .ok_or_else(|| ConsumeError::BufferEmpty(params.stream_name.to_string()))?;

    let events = match params.partition {
        Some(partition_id) => {
            buffer.read_partition_from(partition_id, cursor(partition_id), params.limit)
        }
        None => buffer.read_after_partition_offsets(&offsets, params.limit),
    };

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

    Ok(ConsumeResult {
        partition_offsets: batch_tails(&events),
        events,
        evicted_since_last_poll,
        oldest_available_offset: buffer.earliest_offset().unwrap_or(CdcOffset::ZERO),
    })
}

/// Refuse a read whose cursor lies below the first event this node holds
/// for a partition it reads. Serving it skips the missing events.
fn ensure_available(
    state: &SharedState,
    partition: Option<u32>,
    cursor: &dyn Fn(u32) -> CdcOffset,
) -> Result<(), ConsumeError> {
    let availability = state.cdc_router.availability();
    let floors = match partition {
        Some(partition_id) => availability
            .floor(partition_id)
            .map(|floor| vec![(partition_id, floor)])
            .unwrap_or_default(),
        None => availability.all(),
    };
    for (partition_id, _) in floors {
        if let Some(available_from) = availability.misses(partition_id, cursor(partition_id)) {
            return Err(ConsumeError::OffsetOutOfRange {
                partition_id,
                available_from,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::event::CdcEvent;

    fn publish(index: u64, sequence: u64, row_id: &str) -> CdcEvent {
        CdcEvent {
            sequence,
            partition: 0,
            collection: "topic:orders".into(),
            op: "PUBLISH".into(),
            row_id: row_id.into(),
            event_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            lsn: index,
            index,
            epoch: 0,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: crate::event::EventSource::User,
        }
    }

    /// A consume places its partition read through the routing table, so it
    /// runs on a one-node cluster, which replicates every partition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topic_consume_commit_then_consume_uses_one_canonical_identity() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
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
                last_epoch: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
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
                modification_hlc: nodedb_types::Hlc::ZERO,
            });
        let buffer = state
            .cdc_router
            .ensure_buffer(database_id, tenant_id, stream, &retention);
        buffer.push(publish(1, 1, "msg-1"));
        buffer.push(publish(1, 2, "msg-2"));

        let params = ConsumeParams {
            database_id,
            tenant_id,
            stream_name: topic,
            group_name: group,
            partition: Some(0),
            limit: 1,
        };
        let first = consume_stream(state, &params).await.expect("first consume");
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].offset_token(), "0:1:1");
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
        let second = consume_stream(state, &params)
            .await
            .expect("second consume");
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].offset_token(), "0:1:2");
        cluster.shutdown().await;
    }

    /// A node that joined partition 0 from a snapshot holds its events from
    /// index 101. A cursor below that is refused with the floor, never served
    /// with the gap skipped. A cursor that acknowledged every write below the
    /// floor reads normally.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cursor_below_the_install_floor_is_refused_with_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let database_id = crate::types::DatabaseId::DEFAULT;
        let stream = "orders_stream";
        let retention = crate::event::cdc::stream_def::RetentionConfig {
            max_events: 10,
            max_age_secs: 60,
        };
        let buffer = state
            .cdc_router
            .ensure_buffer(database_id, 1, stream, &retention);
        buffer.push(publish(101, 2, "after-install"));
        let floor = CdcOffset::at(0, 101, 0);
        state.cdc_router.availability().raise(0, floor);
        let params = ConsumeParams {
            database_id,
            tenant_id: 1,
            stream_name: stream,
            group_name: "analytics",
            partition: Some(0),
            limit: 10,
        };

        let refused =
            consume_local_with_offsets(&state, &params, Some(&[(0, CdcOffset::whole_index(40))]));
        assert!(matches!(
            refused,
            Err(ConsumeError::OffsetOutOfRange {
                partition_id: 0,
                available_from,
            }) if available_from == floor
        ));

        let served =
            consume_local_with_offsets(&state, &params, Some(&[(0, CdcOffset::whole_index(100))]))
                .expect("a cursor at the floor misses nothing");
        assert_eq!(served.events.len(), 1);
        assert_eq!(served.events[0].row_id, "after-install");
    }
}

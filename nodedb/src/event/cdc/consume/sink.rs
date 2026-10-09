// SPDX-License-Identifier: BUSL-1.1

//! Read every partition of a change stream for the node that runs its sink.
//!
//! A sink runs on one node (see [`crate::event::cdc::sink_owner`]), but a
//! data group with fewer replicas than the cluster has nodes keeps its
//! partitions only on its members. The sink owner reads the partitions of
//! groups it replicates from its own buffer, and every other group's
//! partitions from one member of that group, over the remote consume path.
//! Each read carries the consumer group's replicated offsets, so each
//! partition resumes where the last delivery committed, on any owner.
//!
//! Each partition comes from exactly one source per read, so no event is
//! delivered twice in one batch.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;

use super::error::ConsumeError;
use super::local::consume_stream;
use super::params::{ConsumeParams, ConsumeResult, batch_tails};
use super::remote::{consume_remote, replica_other_than};

/// Where the sink owner reads each data group's partitions from.
struct ReadPlan {
    /// Groups this node replicates: read from its own buffer.
    local: BTreeSet<u64>,
    /// Every other group, by the member node that serves its partitions.
    remote: BTreeMap<u64, BTreeSet<u64>>,
    /// The data group of every vShard.
    vshard_group: Vec<u64>,
}

impl ReadPlan {
    /// The plan from this node's routing table. A node whose routing is not
    /// wired yet refuses.
    fn from_routing(state: &SharedState) -> Result<Self, ConsumeError> {
        let routing_lock = state
            .cluster_routing
            .as_ref()
            .ok_or(ConsumeError::NoClusterRouting)?;
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        Ok(Self::from_table(state.node_id, &routing))
    }

    /// The plan of node `node_id` under `routing`.
    fn from_table(node_id: u64, routing: &nodedb_cluster::RoutingTable) -> Self {
        let vshard_group = routing.vshard_to_group().to_vec();
        let data_groups: BTreeSet<u64> = vshard_group.iter().copied().collect();
        let mut local = BTreeSet::new();
        let mut remote: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
        for group_id in data_groups {
            let Some(info) = routing.group_info(group_id) else {
                continue;
            };
            if info.members.contains(&node_id) {
                local.insert(group_id);
                continue;
            }
            let server = match info.leader {
                0 => info.members.first().copied(),
                leader => Some(leader),
            };
            if let Some(node) = server {
                remote.entry(node).or_default().insert(group_id);
            }
        }
        Self {
            local,
            remote,
            vshard_group,
        }
    }

    /// The data group of `partition`, a vShard.
    fn group_of(&self, partition: u32) -> Option<u64> {
        let vshard = usize::try_from(partition).ok()?;
        self.vshard_group.get(vshard).copied()
    }

    /// Whether `event` belongs to one of `groups`.
    fn keeps(&self, groups: &BTreeSet<u64>, event: &CdcEvent) -> bool {
        self.group_of(event.partition)
            .is_some_and(|group| groups.contains(&group))
    }
}

/// Read up to `params.limit` events of every partition of the stream after
/// the consumer group's committed offsets, from this node's buffer and from
/// the members of every group it does not replicate. `params.partition` is
/// ignored: a sink reads every partition.
///
/// Within each partition the events stay in position order, so committing
/// the tails of any delivered prefix is exact.
pub async fn consume_for_sink(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    let every_partition = ConsumeParams {
        database_id: params.database_id,
        tenant_id: params.tenant_id,
        stream_name: params.stream_name,
        group_name: params.group_name,
        partition: None,
        limit: params.limit,
    };
    let plan = ReadPlan::from_routing(state)?;
    let local = read_local(state, &every_partition).await?;

    let mut events: Vec<Arc<CdcEvent>> = local
        .events
        .into_iter()
        .filter(|event| plan.keeps(&plan.local, event))
        .collect();
    for (node, groups) in &plan.remote {
        match consume_remote(state, &every_partition, *node).await {
            Ok(result) => events.extend(
                result
                    .events
                    .into_iter()
                    .filter(|event| plan.keeps(groups, event)),
            ),
            Err(ConsumeError::OffsetOutOfRange {
                partition_id,
                available_from,
            }) => {
                // The node lacks events below the group's cursor on one
                // partition. Another member serves that partition alone,
                // and the node's read resumes once its cursor passes them.
                if plan
                    .group_of(partition_id)
                    .is_some_and(|group| groups.contains(&group))
                {
                    let missing = MissingPartition {
                        partition_id,
                        available_from,
                        excluded: *node,
                    };
                    events.extend(read_one_elsewhere(state, &every_partition, missing).await?);
                }
            }
            Err(error) => {
                tracing::warn!(
                    stream = params.stream_name,
                    node,
                    error = %error,
                    "sink read: a group member did not serve its partitions; \
                     the next read retries it"
                );
            }
        }
    }
    Ok(ConsumeResult {
        partition_offsets: batch_tails(&events),
        events,
        evicted_since_last_poll: local.evicted_since_last_poll,
        oldest_available_offset: local.oldest_available_offset,
    })
}

/// This node's own buffer. A partition whose events this node lacks below
/// the cursor is read from another member alone.
async fn read_local(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    match consume_stream(state, params).await {
        Ok(result) => Ok(result),
        Err(ConsumeError::BufferEmpty(_)) => Ok(empty()),
        Err(ConsumeError::RemotePartition {
            partition_id,
            leader_node,
        }) => {
            let single = ConsumeParams {
                partition: Some(partition_id),
                ..*params
            };
            let events = consume_remote(state, &single, leader_node).await?.events;
            Ok(ConsumeResult {
                partition_offsets: batch_tails(&events),
                events,
                ..empty()
            })
        }
        Err(error) => Err(error),
    }
}

/// A partition a member cannot serve after the consumer's cursor.
struct MissingPartition {
    partition_id: u32,
    /// The first position the member holds.
    available_from: crate::event::cdc::offset::CdcOffset,
    /// The member that refused the read.
    excluded: u64,
}

/// The events of the missing partition alone, from another member. With no
/// other member, the read fails with the refusing member's out-of-range
/// error: the consumer resets to `available_from`, never skips silently.
async fn read_one_elsewhere(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    missing: MissingPartition,
) -> Result<Vec<Arc<CdcEvent>>, ConsumeError> {
    let MissingPartition {
        partition_id,
        available_from,
        excluded,
    } = missing;
    let Some(node) = replica_other_than(state, partition_id, excluded)? else {
        return Err(ConsumeError::OffsetOutOfRange {
            partition_id,
            available_from,
        });
    };
    let single = ConsumeParams {
        partition: Some(partition_id),
        ..*params
    };
    Ok(consume_remote(state, &single, node).await?.events)
}

/// A read that found nothing.
fn empty() -> ConsumeResult {
    ConsumeResult {
        events: Vec::new(),
        partition_offsets: Vec::new(),
        evicted_since_last_poll: 0,
        oldest_available_offset: crate::event::cdc::offset::CdcOffset::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(partition: u32) -> CdcEvent {
        CdcEvent {
            sequence: 1,
            partition,
            collection: "orders".into(),
            op: "INSERT".into(),
            row_id: "o1".into(),
            event_time: 0,
            lsn: 0,
            index: 1,
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

    #[test]
    fn each_group_is_read_from_one_source() {
        // Replication factor 1: group g has node g as its only member, and
        // vShard v belongs to group 1 + v % 3.
        let routing = nodedb_cluster::RoutingTable::uniform(3, &[1, 2, 3], 1);
        let plan = ReadPlan::from_table(1, &routing);
        assert_eq!(plan.local, BTreeSet::from([1]));
        assert_eq!(
            plan.remote,
            BTreeMap::from([(2, BTreeSet::from([2])), (3, BTreeSet::from([3]))])
        );

        assert!(plan.keeps(&plan.local, &event(0)));
        assert!(!plan.keeps(&plan.local, &event(1)));
        assert!(plan.keeps(&plan.remote[&2], &event(1)));
        assert!(plan.keeps(&plan.remote[&3], &event(2)));
        assert!(!plan.keeps(&plan.remote[&2], &event(2)));
    }

    #[test]
    fn a_node_replicating_every_group_reads_only_its_own_buffer() {
        let routing = nodedb_cluster::RoutingTable::uniform(3, &[1, 2, 3], 3);
        let plan = ReadPlan::from_table(2, &routing);
        assert_eq!(plan.local, BTreeSet::from([1, 2, 3]));
        assert!(plan.remote.is_empty());
    }
}

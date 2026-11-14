// SPDX-License-Identifier: BUSL-1.1

//! Forwarding change runs to the nodes that do not replicate a partition.
//!
//! Only the partition's leader forwards, per the routing table's leader hint.
//! Each target node has one feed task (see [`super::feed`]). A settle wakes
//! the task of every target of the settled partition, and the task reads
//! the stretch the target lacks from this node's stream. A stretch with no
//! event costs no message: the next run with an event covers it. An idle
//! feed whose position advanced goes out on a heartbeat, at most once per
//! `change_feed_heartbeat_ms`.
//!
//! A leadership starts its feeds at once. Every new leader commits a no-op
//! in its term, at boot too, and the apply loop settles it like any entry:
//! the settle wakes the feed (see `apply_loop::settle`), and the Raft loop
//! names this node the group's leader in the routing table before it hands
//! the no-op to the apply loop. The feed of a partition whose leadership
//! term changed probes the follower before it sends.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use nodedb_cluster::RaftRpc;
use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};

use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::event::cross_shard::types::{
    NOTIFY_PARTITION_GROUP, NotifyBroadcastMsg, NotifyBroadcastReply, NotifyChange,
};

use super::ChangePartition;
use super::feed::{ExchangeError, FeedPeer, PeerWake, run_peer};
use super::forward_read::FORWARD_RUN_EVENTS;
use super::ring::AppendedRun;

#[derive(Default)]
pub(super) struct ChangeFanout {
    /// The wake of each target node's feed task.
    peers: Mutex<HashMap<u64, Arc<PeerWake>>>,
    heartbeat_started: AtomicBool,
}

impl ChangeFanout {
    /// The nodes this node forwards `partition` to: every active node that
    /// does not replicate the partition's data group, when this node leads
    /// the group. Empty otherwise.
    pub fn targets(&self, shared: &SharedState, partition: ChangePartition) -> Vec<u64> {
        let (Some(topology), Some(routing)) = (&shared.cluster_topology, &shared.cluster_routing)
        else {
            return Vec::new();
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let ChangePartition(group) = partition;
        let Some(info) = routing.group_info(group) else {
            return Vec::new();
        };
        if info.leader != shared.node_id {
            return Vec::new();
        }
        let topology = topology.read().unwrap_or_else(|p| p.into_inner());
        topology
            .active_nodes()
            .iter()
            .map(|node| node.node_id)
            .filter(|id| {
                *id != shared.node_id && !info.members.contains(id) && !info.learners.contains(id)
            })
            .collect()
    }

    /// Wake the feed task of every target of `partition`, and start the
    /// task of a target that has none. With `heartbeat`, the pass also
    /// sends a stretch with no event. Returns whether `partition` has a
    /// target.
    pub fn wake(
        &self,
        shared: &Arc<SharedState>,
        partition: ChangePartition,
        heartbeat: bool,
    ) -> bool {
        let targets = self.targets(shared, partition);
        if targets.is_empty() {
            return false;
        }
        let Some(transport) = &shared.cluster_transport else {
            return true;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return true;
        };
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        for peer in targets {
            let wake = peers.entry(peer).or_insert_with(|| {
                let wake = Arc::new(PeerWake::default());
                let link = PeerLink {
                    shared: Arc::downgrade(shared),
                    transport: Arc::downgrade(transport),
                    node_id: shared.node_id,
                    peer,
                };
                runtime.spawn(run_peer(
                    link,
                    Arc::clone(&wake),
                    shared.shutdown.subscribe(),
                ));
                wake
            });
            wake.wake(heartbeat);
        }
        true
    }

    /// Start the heartbeat task once. It runs while `shared` lives, until
    /// the node's shutdown signal. It holds `shared` only for one tick.
    pub fn ensure_heartbeat(&self, shared: &Arc<SharedState>) {
        if self.heartbeat_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.heartbeat_started.store(false, Ordering::Release);
            return;
        };
        let interval = Duration::from_millis(
            shared
                .tuning
                .cluster_transport
                .change_feed_heartbeat_ms
                .max(1),
        );
        let weak: Weak<SharedState> = Arc::downgrade(shared);
        let mut shutdown = shared.shutdown.subscribe();
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = shutdown.wait_cancelled() => return,
                    _ = ticker.tick() => {}
                }
                let Some(shared) = weak.upgrade() else {
                    return;
                };
                shared.change_stream.heartbeat(&shared);
            }
        });
    }
}

/// One follower node over the cluster transport. It holds the node's state
/// and transport weakly, and takes a strong handle for one read or one
/// exchange only, so a stopped node's transport closes once its owners let
/// it go.
struct PeerLink {
    shared: Weak<SharedState>,
    transport: Weak<nodedb_cluster::NexarTransport>,
    node_id: u64,
    peer: u64,
}

impl FeedPeer for PeerLink {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn partitions(&self) -> Option<Vec<(ChangePartition, u64)>> {
        let shared = self.shared.upgrade()?;
        let stream = &shared.change_stream;
        Some(
            stream
                .partitions()
                .into_iter()
                .filter(|partition| {
                    stream
                        .fanout
                        .targets(&shared, *partition)
                        .contains(&self.peer)
                })
                .map(|partition| (partition, lead_term(&shared, partition)))
                .collect(),
        )
    }

    fn feeds(&self, partition: ChangePartition) -> bool {
        self.shared.upgrade().is_some_and(|shared| {
            shared
                .change_stream
                .fanout
                .targets(&shared, partition)
                .contains(&self.peer)
        })
    }

    async fn read(
        &self,
        partition: ChangePartition,
        after: CdcOffset,
    ) -> crate::Result<Option<AppendedRun>> {
        let Some(shared) = self.shared.upgrade() else {
            return Ok(None);
        };
        // A read below the ring reads the journal from disk.
        let task = tokio::task::spawn_blocking(move || {
            shared
                .change_stream
                .forward_run(partition, after, FORWARD_RUN_EVENTS)
        });
        match task.await {
            Ok(read) => read,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => Err(crate::Error::Internal {
                detail: format!("the change run read did not finish: {error}"),
            }),
        }
    }

    async fn exchange(
        &self,
        msg: &NotifyBroadcastMsg,
    ) -> Result<NotifyBroadcastReply, ExchangeError> {
        let payload = zerompk::to_msgpack_vec(msg).map_err(|error| ExchangeError::Encode {
            detail: error.to_string(),
        })?;
        let envelope = VShardEnvelope::new(
            VShardMessageType::NotifyBroadcast,
            self.node_id,
            self.peer,
            0,
            payload,
        );
        let transport = self
            .transport
            .upgrade()
            .ok_or(ExchangeError::TransportClosed)?;
        let sent = transport
            .send_rpc(self.peer, RaftRpc::VShardEnvelope(envelope.to_bytes()))
            .await;
        decode_reply(sent)
    }

    fn rejected(&self, partition: ChangePartition, detail: &str) {
        let ChangePartition(group) = partition;
        crate::diag::change_run_rejected(self.peer, group, detail);
    }
}

/// The Raft term at which the routing table names this node's leadership
/// of `partition`'s group. A new term is a new leadership, whose feed
/// probes the follower again. `0` without a cluster view.
fn lead_term(shared: &SharedState, partition: ChangePartition) -> u64 {
    let ChangePartition(group) = partition;
    shared.cluster_routing.as_ref().map_or(0, |routing| {
        routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_info(group)
            .map_or(0, |info| info.leader_term)
    })
}

/// The follower's reply in the answer to one send, or why there is none.
fn decode_reply(
    sent: nodedb_cluster::error::Result<RaftRpc>,
) -> Result<NotifyBroadcastReply, ExchangeError> {
    let bytes = match sent {
        Ok(RaftRpc::VShardEnvelope(bytes)) => bytes,
        Ok(_) => return Err(ExchangeError::NotEnvelope),
        Err(error) => {
            return Err(ExchangeError::Transport {
                detail: error.to_string(),
            });
        }
    };
    let envelope = VShardEnvelope::from_bytes(&bytes).ok_or(ExchangeError::EnvelopeDecode)?;
    if envelope.msg_type != VShardMessageType::NotifyBroadcastAck {
        return Err(ExchangeError::UnexpectedReply {
            msg_type: format!("{:?}", envelope.msg_type),
        });
    }
    zerompk::from_msgpack(&envelope.payload).map_err(|error| ExchangeError::ReplyDecode {
        detail: error.to_string(),
    })
}

impl NotifyBroadcastMsg {
    /// The wire form of `run`.
    pub(crate) fn from_run(source_node: u64, run: &AppendedRun) -> Self {
        let ChangePartition(group) = run.partition;
        let (partition_kind, partition_id) = (NOTIFY_PARTITION_GROUP, group);
        Self {
            source_node,
            partition_kind,
            partition_id,
            after: run.after,
            through: run.through,
            changes: run
                .events
                .iter()
                .map(|event| NotifyChange {
                    position: event.position(),
                    tenant_id: event.tenant_id.as_u64(),
                    database_id: event.database_id().as_u64(),
                    collection: event.collection.clone(),
                    document_id: event.document_id.to_string(),
                    operation: event.operation.as_str().to_string(),
                    timestamp_ms: event.timestamp_ms,
                    lsn: event.lsn.as_u64(),
                })
                .collect(),
            probe: false,
        }
    }

    /// A probe of the receiver's position in `partition`.
    pub(crate) fn probe(source_node: u64, partition: ChangePartition) -> Self {
        let ChangePartition(group) = partition;
        Self {
            source_node,
            partition_kind: NOTIFY_PARTITION_GROUP,
            partition_id: group,
            after: CdcOffset::ZERO,
            through: CdcOffset::ZERO,
            changes: Vec::new(),
            probe: true,
        }
    }

    /// The partition the run belongs to, `None` for an unknown kind.
    pub fn partition(&self) -> Option<ChangePartition> {
        match self.partition_kind {
            NOTIFY_PARTITION_GROUP => Some(ChangePartition(self.partition_id)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cross_shard::types::NotifyAnswer;

    fn answer(
        msg_type: VShardMessageType,
        payload: Vec<u8>,
    ) -> nodedb_cluster::error::Result<RaftRpc> {
        Ok(RaftRpc::VShardEnvelope(
            VShardEnvelope::new(msg_type, 2, 1, 0, payload).to_bytes(),
        ))
    }

    /// A reply envelope decodes to the follower's answer and position. A
    /// reply that does not decode, a reply of another type, and a transport
    /// error are no reply: the feed sends the stretch again.
    #[test]
    fn only_a_decoded_reply_reaches_the_feed() {
        let reply = NotifyBroadcastReply {
            answer: NotifyAnswer::Taken,
            through: Some(CdcOffset::whole_index(4)),
        };
        let payload = zerompk::to_msgpack_vec(&reply).expect("encode the reply");
        let decoded = decode_reply(answer(VShardMessageType::NotifyBroadcastAck, payload))
            .expect("the reply decodes");
        assert_eq!(decoded, reply);
        let failures = [
            answer(VShardMessageType::NotifyBroadcastAck, Vec::new()),
            answer(VShardMessageType::CrossShardEventAck, Vec::new()),
            Ok(RaftRpc::VShardEnvelope(vec![0xc1])),
            Err(nodedb_cluster::error::ClusterError::Transport {
                detail: "peer unreachable".into(),
            }),
        ];
        for sent in failures {
            assert!(decode_reply(sent).is_err());
        }
    }
}

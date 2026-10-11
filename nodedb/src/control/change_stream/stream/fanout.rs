// SPDX-License-Identifier: BUSL-1.1

//! Forwarding change runs to the nodes that do not replicate a partition.
//!
//! Only the partition's leader forwards, per the routing table's leader hint.
//! A run goes out when the feed gained events. It covers the feed from the
//! position the leader last forwarded, so a feed stretch with no events costs
//! no message. An idle feed whose position advanced goes out on a heartbeat,
//! at most once per `change_feed_heartbeat_ms`. A receiver therefore sees a
//! continuous feed, and records a hole only for a run that never arrived.
//!
//! Each target node has one bounded queue drained by one task that awaits
//! every run's ack before it sends the next, so a node appends one leader's
//! runs in order. A run that cannot be queued is dropped. The target then
//! records a hole when the next run arrives, and every cursor below the hole
//! resets.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{trace, warn};

use crate::control::state::SharedState;
use crate::event::cross_shard::types::{NOTIFY_PARTITION_GROUP, NotifyBroadcastMsg, NotifyChange};

use super::ChangePartition;
use super::ring::AppendedRun;

/// Runs queued for one target node before new runs are dropped.
const PEER_QUEUE: usize = 1024;

#[derive(Default)]
pub(super) struct ChangeFanout {
    peers: Mutex<HashMap<u64, mpsc::Sender<Vec<u8>>>>,
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

    /// Queue `run` for every node in `targets`.
    pub fn send(&self, shared: &SharedState, run: &AppendedRun, targets: &[u64]) {
        let Some(transport) = &shared.cluster_transport else {
            return;
        };
        let msg = NotifyBroadcastMsg::from_run(shared.node_id, run);
        let payload = match zerompk::to_msgpack_vec(&msg) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(%error, "encoding a change-stream run failed; peers record a hole");
                return;
            }
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        for &peer in targets {
            let sender = peers.entry(peer).or_insert_with(|| {
                spawn_peer(&runtime, Arc::clone(transport), shared.node_id, peer)
            });
            match sender.try_send(payload.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        peer,
                        "change-stream forward queue is full; the peer records a hole"
                    );
                }
                Err(TrySendError::Closed(_)) => {
                    peers.remove(&peer);
                }
            }
        }
    }

    /// Start the heartbeat task once. It runs while `shared` lives.
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
        runtime.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(shared) = weak.upgrade() else {
                    return;
                };
                shared.change_stream.heartbeat(&shared);
            }
        });
    }
}

fn spawn_peer(
    runtime: &tokio::runtime::Handle,
    transport: Arc<nodedb_cluster::NexarTransport>,
    node_id: u64,
    peer: u64,
) -> mpsc::Sender<Vec<u8>> {
    use nodedb_cluster::RaftRpc;
    use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};

    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(PEER_QUEUE);
    runtime.spawn(async move {
        while let Some(payload) = receiver.recv().await {
            let envelope = VShardEnvelope::new(
                VShardMessageType::NotifyBroadcast,
                node_id,
                peer,
                0,
                payload,
            );
            // The ack returns once the peer appended the run.
            if let Err(error) = transport
                .send_rpc(peer, RaftRpc::VShardEnvelope(envelope.to_bytes()))
                .await
            {
                trace!(peer, %error, "forwarding a change-stream run failed; the peer records a hole");
            }
        }
    });
    sender
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

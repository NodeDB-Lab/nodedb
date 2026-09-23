// SPDX-License-Identifier: BUSL-1.1

//! [`RaftSequencerProposer`]: the production [`SequencerProposer`].
//!
//! On the sequencer leader it appends the entry to the local sequencer
//! group. On any other node it forwards the entry to the leader over the
//! cluster transport as a `DataProposeRequest` with the `Sequencer` target,
//! the same RPC that forwards data-group proposals. The leader's handler
//! proposes it to its sequencer group.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;
use tracing::debug;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::rpc_codec::{DataProposeRequest, ProposeTarget, RaftRpc};

use super::seam::{ProposeDispatch, SequencerProposeError, SequencerProposer};
use crate::control::server::exchange::resolve::register_peers_from_topology;
use crate::control::state::SharedState;

/// Most forward RPCs one proposer keeps in flight. A proposal past this
/// limit fails with [`SequencerProposeError::ForwardBusy`], and the
/// scheduler proposes it again on a later stall tick.
pub const MAX_INFLIGHT_SEQUENCER_FORWARDS: usize = 64;

/// How a proposal reaches the sequencer group from this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SequencerRoute {
    /// This node leads the sequencer group.
    Local,
    /// Another node leads the sequencer group.
    Forward { leader: u64 },
}

/// Pick the route from this node's view of the sequencer group.
///
/// `leader` is the leader this node observes, `0` while unknown. A node
/// that is not leader but observes itself as leader has a stale view, so it
/// has no leader to send to.
pub(super) fn sequencer_route(
    is_leader: bool,
    leader: u64,
    local_node: u64,
) -> Result<SequencerRoute, SequencerProposeError> {
    if is_leader {
        return Ok(SequencerRoute::Local);
    }
    if leader == 0 || leader == local_node {
        return Err(SequencerProposeError::NoLeader);
    }
    Ok(SequencerRoute::Forward { leader })
}

/// Proposes sequencer entries locally on the sequencer leader and forwards
/// them to the leader from every other node.
pub struct RaftSequencerProposer {
    node_id: u64,
    multi_raft: Arc<Mutex<MultiRaft>>,
    /// Source of the cluster transport and topology for forwards.
    shared: Arc<SharedState>,
    /// One permit per forward RPC in flight.
    forwards: Arc<Semaphore>,
}

impl RaftSequencerProposer {
    pub fn new(node_id: u64, multi_raft: Arc<Mutex<MultiRaft>>, shared: Arc<SharedState>) -> Self {
        Self {
            node_id,
            multi_raft,
            shared,
            forwards: Arc::new(Semaphore::new(MAX_INFLIGHT_SEQUENCER_FORWARDS)),
        }
    }

    /// Send `bytes` to `leader` on a spawned task.
    ///
    /// The task logs a refused or failed forward at `debug`. The scheduler
    /// does not need the RPC result: it proposes the entry again until the
    /// completion registry shows it applied.
    fn forward(
        &self,
        leader: u64,
        bytes: Vec<u8>,
    ) -> Result<ProposeDispatch, SequencerProposeError> {
        let Some(transport) = self.shared.cluster_transport.as_ref() else {
            return Err(SequencerProposeError::NoTransport { leader });
        };
        let permit = Arc::clone(&self.forwards)
            .try_acquire_owned()
            .map_err(|_| SequencerProposeError::ForwardBusy {
                leader,
                limit: MAX_INFLIGHT_SEQUENCER_FORWARDS,
            })?;
        register_peers_from_topology(&self.shared, transport, &BTreeSet::from([leader]));
        let transport = Arc::clone(transport);
        tokio::spawn(async move {
            let _permit = permit;
            let rpc = RaftRpc::DataProposeRequest(DataProposeRequest {
                target: ProposeTarget::Sequencer,
                bytes,
            });
            match transport.send_rpc(leader, rpc).await {
                Ok(RaftRpc::DataProposeResponse(resp)) if resp.success => {}
                Ok(RaftRpc::DataProposeResponse(resp)) => debug!(
                    leader,
                    leader_hint = ?resp.leader_hint,
                    error = %resp.error_message,
                    "calvin: sequencer leader refused a forwarded entry",
                ),
                Ok(other) => debug!(
                    leader,
                    response = ?other,
                    "calvin: unexpected reply to a forwarded sequencer entry",
                ),
                Err(e) => debug!(
                    leader,
                    error = %e,
                    "calvin: forward of a sequencer entry failed",
                ),
            }
        });
        Ok(ProposeDispatch::Forwarded { leader })
    }
}

impl SequencerProposer for RaftSequencerProposer {
    fn propose(&self, bytes: Vec<u8>) -> Result<ProposeDispatch, SequencerProposeError> {
        let leader = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let route = sequencer_route(
                mr.is_group_leader(SEQUENCER_GROUP_ID),
                mr.group_leader(SEQUENCER_GROUP_ID),
                self.node_id,
            )?;
            match route {
                SequencerRoute::Local => {
                    mr.propose_to_group(SEQUENCER_GROUP_ID, bytes)?;
                    return Ok(ProposeDispatch::Local);
                }
                SequencerRoute::Forward { leader } => leader,
            }
        };
        self.forward(leader, bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_cluster::RoutingTable;
    use nodedb_raft::message::AppendEntriesRequest;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    const LOCAL_NODE: u64 = 1;
    const REMOTE_LEADER: u64 = 2;

    fn shared_state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("test.wal")).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    /// A `MultiRaft` on `LOCAL_NODE` with a sequencer group of `peers`.
    fn multi_raft(dir: &std::path::Path, peers: Vec<u64>) -> Arc<Mutex<MultiRaft>> {
        let rt = RoutingTable::uniform(1, &[LOCAL_NODE], 1);
        let mut mr = MultiRaft::new(LOCAL_NODE, rt, dir.to_path_buf());
        mr.add_group(SEQUENCER_GROUP_ID, peers)
            .expect("add sequencer group");
        Arc::new(Mutex::new(mr))
    }

    #[test]
    fn route_is_local_on_the_leader() {
        assert_eq!(
            sequencer_route(true, LOCAL_NODE, LOCAL_NODE).expect("route"),
            SequencerRoute::Local
        );
    }

    #[test]
    fn route_forwards_to_a_remote_leader() {
        assert_eq!(
            sequencer_route(false, REMOTE_LEADER, LOCAL_NODE).expect("route"),
            SequencerRoute::Forward {
                leader: REMOTE_LEADER
            }
        );
    }

    #[test]
    fn route_has_no_target_without_a_known_leader() {
        assert!(matches!(
            sequencer_route(false, 0, LOCAL_NODE),
            Err(SequencerProposeError::NoLeader)
        ));
        assert!(matches!(
            sequencer_route(false, LOCAL_NODE, LOCAL_NODE),
            Err(SequencerProposeError::NoLeader)
        ));
    }

    #[tokio::test]
    async fn sequencer_leader_appends_the_entry_locally() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mr = multi_raft(dir.path(), vec![]);
        {
            let mut guard = mr.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(node) = guard.groups_mut().get_mut(&SEQUENCER_GROUP_ID) {
                // no-determinism: test-only forced election deadline so the single voter campaigns immediately.
                node.election_deadline_override(Instant::now() - Duration::from_millis(1));
            }
            for _ in 0..20 {
                guard.tick().expect("tick");
                if guard.is_group_leader(SEQUENCER_GROUP_ID) {
                    break;
                }
            }
            assert!(guard.is_group_leader(SEQUENCER_GROUP_ID));
        }
        let before = mr
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .unwrap_or(0);
        let proposer =
            RaftSequencerProposer::new(LOCAL_NODE, Arc::clone(&mr), shared_state(dir.path()));

        let dispatch = proposer.propose(vec![9, 9]).expect("local propose");

        assert_eq!(dispatch, ProposeDispatch::Local);
        let after = mr
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .unwrap_or(0);
        assert_eq!(after, before + 1);
    }

    /// A follower that knows the remote leader takes the forward path. The
    /// fixture has no cluster transport, so the forward stops there with
    /// the leader named, and nothing is appended to the local log.
    #[tokio::test]
    async fn follower_forwards_to_the_sequencer_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mr = multi_raft(dir.path(), vec![REMOTE_LEADER]);
        let before = {
            let mut guard = mr.lock().unwrap_or_else(|p| p.into_inner());
            guard
                .handle_append_entries(&AppendEntriesRequest {
                    term: 1,
                    leader_id: REMOTE_LEADER,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                    group_id: SEQUENCER_GROUP_ID,
                })
                .expect("heartbeat");
            assert_eq!(guard.group_leader(SEQUENCER_GROUP_ID), REMOTE_LEADER);
            guard.last_log_index(SEQUENCER_GROUP_ID).unwrap_or(0)
        };
        let proposer =
            RaftSequencerProposer::new(LOCAL_NODE, Arc::clone(&mr), shared_state(dir.path()));

        let result = proposer.propose(vec![9, 9]);

        assert!(
            matches!(
                result,
                Err(SequencerProposeError::NoTransport {
                    leader: REMOTE_LEADER
                })
            ),
            "{result:?}"
        );
        let after = mr
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .unwrap_or(0);
        assert_eq!(after, before, "a follower appends nothing locally");
    }
}

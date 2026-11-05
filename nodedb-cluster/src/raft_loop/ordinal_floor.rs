// SPDX-License-Identifier: BUSL-1.1

//! The ordinal floor exchange: a group's leader raises the group's
//! system-time floor on a quorum of voters, or gathers it from one.
//!
//! A leader that closes a read cut `S` runs the exchange with `S` before it
//! answers the read index, so a quorum holds a floor of at least `S`,
//! durably. A newly elected leader runs it once per term before it admits
//! its first write. The two quorums share a voter, whose term only grows: it
//! answered the old leader's raise before the new leader's gather, so the
//! new leader learns a floor of at least `S` and stamps above it. No clock
//! skew bound enters the argument.

use std::time::{Duration, Instant};

use futures::StreamExt;

use crate::error::{ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::multi_raft::FloorAnswer;
use crate::rpc_codec::{OrdinalFloorRequest, OrdinalFloorResponse, RaftRpc};

use super::loop_core::{CommitApplier, RaftLoop};

/// The pause before a voter that refused the round, or did not answer, is
/// asked again: a voter behind the leader's term catches up within a
/// heartbeat.
const RETRY_PAUSE: Duration = Duration::from_millis(20);

/// The longest one ask of a voter runs before the voter is asked again, so a
/// voter that returns during the round still counts. The exchange is
/// idempotent: a floor only rises.
const ASK_BOUND: Duration = Duration::from_secs(1);

/// How the asks of one voter ended.
enum VoterEnd {
    /// The voter holds this floor at the round's term.
    Held(i64),
    /// The voter is at a later term, so this node no longer leads the round's.
    LaterTerm,
    /// The round's time ended with no answer at its term.
    OutOfTime,
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Raise `group_id`'s floor to `floor` on a quorum of voters, this
    /// leader included, each durably. Returns the highest floor the quorum
    /// holds, which this leader then holds too.
    ///
    /// Fails with [`ClusterError::ReadIndexNotLeader`] when this node does
    /// not lead the group or stops leading it at the round's term, and with
    /// [`ClusterError::ReadIndexTimeout`] when no quorum answered within
    /// `timeout`.
    pub async fn spread_ordinal_floor(
        &self,
        group_id: u64,
        floor: i64,
        timeout: Duration,
    ) -> Result<i64> {
        let started = Instant::now();
        let round = self.raise_own_floor(group_id, floor).await?;
        let mut highest = round.floor;
        let mut held: usize = 1;
        let request = OrdinalFloorRequest {
            group_id,
            term: round.term,
            floor: round.floor,
        };
        // Each voter runs its own asks, so a voter that refused asks again
        // while a voter that is down holds nothing back. A quorum ends the
        // round at once.
        let mut asks: futures::stream::FuturesUnordered<_> = round
            .voters
            .iter()
            .map(|&peer| self.hold_floor_on_voter(peer, request.clone(), started, timeout))
            .collect();
        while held < round.quorum {
            match asks.next().await {
                Some(VoterEnd::Held(peer_floor)) => {
                    held += 1;
                    highest = highest.max(peer_floor);
                }
                Some(VoterEnd::LaterTerm) => {
                    return Err(ClusterError::ReadIndexNotLeader { group_id });
                }
                Some(VoterEnd::OutOfTime) => {}
                None => {
                    return Err(ClusterError::ReadIndexTimeout {
                        group_id,
                        waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    });
                }
            }
        }
        drop(asks);
        let still_leads = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leads_at_term(group_id, round.term);
        if !still_leads {
            return Err(ClusterError::ReadIndexNotLeader { group_id });
        }
        if highest > round.floor {
            self.raise_own_floor(group_id, highest).await?;
        }
        Ok(highest)
    }

    /// The floor a write this leader proposes to `group_id` stamps above.
    /// The first write of a leadership term gathers it from a quorum of
    /// voters; every later one reads the gathered value. Fails like
    /// [`Self::spread_ordinal_floor`].
    pub async fn gathered_ordinal_floor(&self, group_id: u64, timeout: Duration) -> Result<i64> {
        let (own, term) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(floor) = mr.gathered_ordinal_floor(group_id) {
                return Ok(floor);
            }
            if !mr.is_group_leader(group_id) {
                return Err(ClusterError::ReadIndexNotLeader { group_id });
            }
            let (_, term) = mr.group_leader_at_term(group_id);
            (mr.group_ordinal_floor(group_id).unwrap_or(0), term)
        };
        let floor = self.spread_ordinal_floor(group_id, own, timeout).await?;
        self.multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .mark_floor_gathered(group_id, term);
        Ok(floor)
    }

    /// A voter's side of the exchange: take the leader's floor when this
    /// node is at the leader's term, make the hard state durable, and answer
    /// the floor this node holds.
    pub(super) async fn handle_ordinal_floor_rpc(
        &self,
        req: OrdinalFloorRequest,
    ) -> Result<RaftRpc> {
        let (answer, ticket) = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let mark = mr.staged_through(req.group_id);
            let answer = mr.answer_floor_round(req.group_id, req.term, req.floor);
            mr.persist_group_hard_state(req.group_id)?;
            (answer, mr.reply_ticket(req.group_id, mark))
        };
        if let Some(ticket) = ticket {
            ticket.durable().await?;
        }
        let response = match answer {
            FloorAnswer::Held(floor) => OrdinalFloorResponse {
                term: req.term,
                floor: Some(floor),
            },
            FloorAnswer::Refused { term } => OrdinalFloorResponse { term, floor: None },
        };
        Ok(RaftRpc::OrdinalFloorResponse(response))
    }

    /// Raise this leader's own floor of `group_id` to `floor`, durably, and
    /// describe the round that spreads it.
    async fn raise_own_floor(
        &self,
        group_id: u64,
        floor: i64,
    ) -> Result<crate::multi_raft::FloorRound> {
        let (round, ticket) = {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let mark = mr.staged_through(group_id);
            let round = mr
                .begin_floor_round(group_id, floor)
                .ok_or(ClusterError::ReadIndexNotLeader { group_id })?;
            mr.persist_group_hard_state(group_id)?;
            (round, mr.reply_ticket(group_id, mark))
        };
        if let Some(ticket) = ticket {
            ticket.durable().await?;
        }
        Ok(round)
    }

    /// Ask `peer` for the round in `request` until it answers at the round's
    /// term, reports a later term, or `timeout` past `started` ends.
    async fn hold_floor_on_voter(
        &self,
        peer: u64,
        request: OrdinalFloorRequest,
        started: Instant,
        timeout: Duration,
    ) -> VoterEnd {
        loop {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return VoterEnd::OutOfTime;
            }
            let answer = self
                .ask_floor(peer, request.clone(), remaining.min(ASK_BOUND))
                .await;
            match answer {
                Ok(OrdinalFloorResponse {
                    floor: Some(peer_floor),
                    ..
                }) => return VoterEnd::Held(peer_floor),
                Ok(OrdinalFloorResponse { term, floor: None }) if term > request.term => {
                    return VoterEnd::LaterTerm;
                }
                Ok(OrdinalFloorResponse { floor: None, .. }) | Err(_) => {
                    let left = timeout.saturating_sub(started.elapsed());
                    tokio::time::sleep(RETRY_PAUSE.min(left)).await;
                }
            }
        }
    }

    /// Send one floor round to `peer` and await its answer within `timeout`.
    async fn ask_floor(
        &self,
        peer: u64,
        request: OrdinalFloorRequest,
        timeout: Duration,
    ) -> Result<OrdinalFloorResponse> {
        self.register_peer_addr(peer)?;
        let group_id = request.group_id;
        let sent = self
            .transport
            .send_rpc(peer, RaftRpc::OrdinalFloorRequest(request));
        match tokio::time::timeout(timeout, sent).await {
            Ok(Ok(RaftRpc::OrdinalFloorResponse(response))) => Ok(response),
            Ok(Ok(other)) => Err(ClusterError::Transport {
                detail: format!(
                    "ordinal floor of group {group_id}: unexpected response variant {other:?}"
                ),
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ClusterError::ReadIndexTimeout {
                group_id,
                waited_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, RwLock};

    use nodedb_raft::message::{LogEntry, RequestVoteRequest};

    use super::super::propose_gate::{Admitted, DataProposeGate};
    use super::*;
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};
    use crate::transport::NexarTransport;
    use crate::transport::credentials::TransportCredentials;

    /// The data group of `RoutingTable::uniform(1, ...)`, which owns vShard 0.
    const GROUP: u64 = 1;

    /// How long a live quorum gets to answer or elect.
    const WAIT: Duration = Duration::from_secs(20);

    /// The election timeout of a node that campaigns once its leader is gone.
    const PROMPT: (Duration, Duration) = (Duration::from_secs(1), Duration::from_millis(1_500));

    /// The election timeout of a node that votes and does not campaign.
    const PATIENT: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(3_600));

    /// The default election timeout, which leaves node 1 the first candidate.
    const DEFAULT: (Duration, Duration) = (Duration::from_secs(2), Duration::from_secs(5));

    struct NoCommits;

    impl CommitApplier for NoCommits {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            entries.last().map_or(0, |entry| entry.index)
        }
    }

    /// A write gate that records the floor each admission stamps above.
    #[derive(Default)]
    struct RecordingGate {
        floors: Mutex<Vec<i64>>,
    }

    #[async_trait::async_trait]
    impl DataProposeGate for RecordingGate {
        async fn admit(
            &self,
            _vshard_id: u32,
            _entry: &[u8],
            _deadline: tokio::time::Instant,
            ordinal_floor: i64,
        ) -> Result<Admitted> {
            self.floors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(ordinal_floor);
            Ok(Admitted::default())
        }
    }

    struct Node {
        raft_loop: Arc<RaftLoop<NoCommits>>,
        gate: Arc<RecordingGate>,
        shutdown: tokio::sync::watch::Sender<bool>,
        tasks: Vec<tokio::task::JoinHandle<()>>,
        _dir: tempfile::TempDir,
    }

    impl Node {
        fn floor(&self) -> i64 {
            self.raft_loop
                .multi_raft_handle()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .group_ordinal_floor(GROUP)
                .unwrap_or(0)
        }

        fn leads(&self) -> bool {
            self.raft_loop
                .multi_raft_handle()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_group_leader(GROUP)
        }

        /// Raise this node's floor as a voter's answer to the current leader
        /// does.
        fn hold_floor(&self, floor: i64) {
            let handle = self.raft_loop.multi_raft_handle();
            let mut mr = handle.lock().unwrap_or_else(|p| p.into_inner());
            let (_, term) = mr.group_leader_at_term(GROUP);
            assert_eq!(
                mr.answer_floor_round(GROUP, term, floor),
                FloorAnswer::Held(floor)
            );
            mr.persist_group_hard_state(GROUP).expect("stage");
        }

        fn stop(&mut self) {
            let _ = self.shutdown.send(true);
            for task in &self.tasks {
                task.abort();
            }
        }

        async fn wait_leads(&self) {
            let deadline = Instant::now() + WAIT;
            while !self.leads() {
                assert!(Instant::now() < deadline, "the node did not win the group");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.stop();
        }
    }

    fn transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(
            NexarTransport::new(
                node_id,
                "127.0.0.1:0".parse().expect("loopback"),
                TransportCredentials::Insecure,
            )
            .expect("transport"),
        )
    }

    /// Three running voters of metadata group 0 and data group 1. Node 1
    /// campaigns at once. Each node's election timeout comes from `timeouts`.
    fn spawn_cluster(timeouts: [(Duration, Duration); 3]) -> [Node; 3] {
        let ids = [1u64, 2, 3];
        let transports = ids.map(transport);
        for (from, from_transport) in ids.iter().zip(&transports) {
            for (to, to_transport) in ids.iter().zip(&transports) {
                if from != to {
                    from_transport.register_peer(*to, to_transport.local_addr());
                }
            }
        }
        let routing = RoutingTable::uniform(1, &ids, 3);
        let mut nodes = Vec::with_capacity(ids.len());
        for (position, &node_id) in ids.iter().enumerate() {
            let dir = tempfile::tempdir().expect("tempdir");
            let (min, max) = timeouts[position];
            let mut mr = MultiRaft::new(node_id, routing.clone(), dir.path().to_path_buf())
                .with_election_timeout(min, max);
            let peers: Vec<u64> = ids.iter().copied().filter(|id| *id != node_id).collect();
            mr.add_group(0, peers.clone()).expect("add group 0");
            mr.add_group(GROUP, peers).expect("add data group");
            for node in mr.groups_mut().values_mut() {
                node.expire_boot_vote_fence();
                if node_id == 1 {
                    node.election_deadline_override(Instant::now() - Duration::from_millis(1));
                }
            }
            let mut topology = ClusterTopology::new();
            for (&id, peer_transport) in ids.iter().zip(&transports) {
                topology.add_node(NodeInfo::new(
                    id,
                    peer_transport.local_addr(),
                    NodeState::Active,
                ));
            }
            let gate = Arc::new(RecordingGate::default());
            let node_transport = Arc::clone(&transports[position]);
            let raft_loop = Arc::new(
                RaftLoop::new(
                    mr,
                    Arc::clone(&node_transport),
                    Arc::new(RwLock::new(topology)),
                    NoCommits,
                )
                .with_data_propose_gate(gate.clone()),
            );
            let (shutdown, _) = tokio::sync::watch::channel(false);
            let serving = {
                let raft_loop = Arc::clone(&raft_loop);
                let stop = shutdown.subscribe();
                tokio::spawn(async move {
                    let _ = node_transport.serve(raft_loop, stop).await;
                })
            };
            let running = {
                let raft_loop = Arc::clone(&raft_loop);
                let stop = shutdown.subscribe();
                tokio::spawn(async move { raft_loop.run(stop).await })
            };
            nodes.push(Node {
                raft_loop,
                gate,
                shutdown,
                tasks: vec![serving, running],
                _dir: dir,
            });
        }
        match <[Node; 3]>::try_from(nodes) {
            Ok(nodes) => nodes,
            Err(_) => panic!("three ids build three nodes"),
        }
    }

    /// A leader raises the floor on a quorum, itself included, and a voter's
    /// higher floor comes back and raises the leader's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_spread_raises_a_quorum_and_learns_a_higher_floor() {
        let cluster = spawn_cluster([DEFAULT, DEFAULT, DEFAULT]);
        cluster[0].wait_leads().await;

        let held = cluster[0]
            .raft_loop
            .spread_ordinal_floor(GROUP, 5_000, WAIT)
            .await
            .expect("a quorum answers");
        assert_eq!(held, 5_000);
        let holders = cluster.iter().filter(|node| node.floor() >= 5_000).count();
        assert!(holders >= 2, "{holders} voters hold the floor");

        // Every voter that can complete the quorum holds a higher floor, so
        // the answer that ends the round carries it.
        cluster[1].hold_floor(9_000);
        cluster[2].hold_floor(9_000);
        let held = cluster[0]
            .raft_loop
            .spread_ordinal_floor(GROUP, 6_000, WAIT)
            .await
            .expect("a quorum answers");
        assert_eq!(held, 9_000);
        assert_eq!(cluster[0].floor(), 9_000);
    }

    /// A voter answers only at its own term. An earlier term's raise and a
    /// later term's raise both change nothing and count toward no quorum.
    #[tokio::test]
    async fn a_voter_answers_only_at_the_exact_term() {
        let dir = tempfile::tempdir().expect("tempdir");
        let routing = RoutingTable::uniform(1, &[1, 2, 3], 3);
        let mut mr = MultiRaft::new(1, routing, dir.path().to_path_buf());
        mr.add_group(GROUP, vec![2, 3]).expect("add data group");
        // A vote request takes this voter to term 5.
        mr.handle_request_vote(&RequestVoteRequest {
            term: 5,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: GROUP,
            transfer: true,
        })
        .expect("the group is mounted");
        let raft_loop = RaftLoop::new(
            mr,
            transport(1),
            Arc::new(RwLock::new(ClusterTopology::new())),
            NoCommits,
        );
        let ask = |group_id: u64, term: u64, floor: i64| {
            raft_loop.handle_ordinal_floor_rpc(OrdinalFloorRequest {
                group_id,
                term,
                floor,
            })
        };
        let answer = |rpc: Result<RaftRpc>| match rpc.expect("the voter answers") {
            RaftRpc::OrdinalFloorResponse(response) => response,
            other => panic!("expected a floor response, got {other:?}"),
        };
        let handle = raft_loop.multi_raft_handle();
        let floor_held = || {
            handle
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .group_ordinal_floor(GROUP)
        };

        for wrong_term in [4, 6] {
            let refused = answer(ask(GROUP, wrong_term, 900).await);
            assert_eq!(
                refused,
                OrdinalFloorResponse {
                    term: 5,
                    floor: None,
                },
                "term {wrong_term}"
            );
            assert_eq!(floor_held(), Some(0), "term {wrong_term} raises nothing");
        }

        let held = answer(ask(GROUP, 5, 900).await);
        assert_eq!(held.floor, Some(900));
        assert_eq!(held.term, 5);
        assert!(
            handle
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .durability_ticket(GROUP)
                .is_none(),
            "the floor is durable before the answer leaves"
        );

        let lower = answer(ask(GROUP, 5, 100).await);
        assert_eq!(lower.floor, Some(900), "a lower floor never lowers it");

        let unhosted = answer(ask(9, 5, 900).await);
        assert_eq!(
            unhosted,
            OrdinalFloorResponse {
                term: 0,
                floor: None,
            }
        );
    }

    /// An old leader's raise reached a voter and not the node that wins next.
    /// The new leader gathers the floor from that voter before its gate admits
    /// its first write, so the write stamps above the closed cut.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_new_leader_gathers_the_floor_before_it_admits() {
        const CUT: i64 = 7_000;
        let mut cluster = spawn_cluster([DEFAULT, PATIENT, PROMPT]);
        cluster[0].wait_leads().await;

        // The old leader and voter 2 form the quorum that holds the cut.
        cluster[0]
            .raft_loop
            .multi_raft_handle()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .begin_floor_round(GROUP, CUT)
            .expect("the leader runs a round");
        cluster[1].hold_floor(CUT);
        assert_eq!(
            cluster[2].floor(),
            0,
            "the next leader does not hold the cut"
        );

        cluster[0].stop();
        cluster[2].wait_leads().await;
        let next = &cluster[2];
        let handle = next.raft_loop.multi_raft_handle();
        assert_eq!(
            handle
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .gathered_ordinal_floor(GROUP),
            None,
            "a new leader starts with no gathered floor"
        );
        assert_eq!(next.floor(), 0);

        let deadline = tokio::time::Instant::now() + WAIT;
        let (group_id, _) = next
            .raft_loop
            .propose_admitted(0, b"first write", deadline)
            .await
            .expect("the new leader admits and proposes");
        assert_eq!(group_id, GROUP);

        let floors = next.gate.floors.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(*floors, vec![CUT], "the gate stamps above the closed cut");
        drop(floors);
        assert_eq!(next.floor(), CUT);
        assert_eq!(
            handle
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .gathered_ordinal_floor(GROUP),
            Some(CUT)
        );
    }

    /// With no quorum the spread fails, so the cut does not close: no read
    /// index answers it, and a new leader's floor stays unmarked as gathered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_spread_does_not_close_the_cut() {
        const CUT: i64 = 8_000;
        const ROUND: Duration = Duration::from_millis(300);
        let mut cluster = spawn_cluster([DEFAULT, DEFAULT, DEFAULT]);
        cluster[0].wait_leads().await;
        cluster[1].stop();
        cluster[2].stop();

        let refused = cluster[0]
            .raft_loop
            .read_index_via_leader(GROUP, ROUND, Some(CUT))
            .await;
        assert!(
            matches!(
                refused,
                Err(ClusterError::ReadIndexTimeout { .. })
                    | Err(ClusterError::ReadIndexNotLeader { .. })
            ),
            "a cut with no quorum answers no read index: {refused:?}"
        );
        assert!(cluster[1].floor() < CUT);
        assert!(cluster[2].floor() < CUT);

        let gathered = cluster[0]
            .raft_loop
            .gathered_ordinal_floor(GROUP, ROUND)
            .await;
        assert!(gathered.is_err(), "no quorum gathers: {gathered:?}");
        assert_eq!(
            cluster[0]
                .raft_loop
                .multi_raft_handle()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .gathered_ordinal_floor(GROUP),
            None
        );
    }

    /// A voter that refuses because it is behind the term asks again while the
    /// other voter is down. The down voter's ask holds nothing back, so a
    /// quorum forms well inside the round's time.
    ///
    /// No node runs its Raft loop here, so no heartbeat moves voter 2. It
    /// reaches the term only through the vote request this test sends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_behind_voter_completes_the_quorum_while_another_is_down() {
        const FLOOR: i64 = 4_000;
        const ROUND: Duration = Duration::from_secs(5);
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let routing = RoutingTable::uniform(1, &[1, 2, 3], 3);

        // Node 1 leads: both of its election rounds answer by hand.
        let mut leader = MultiRaft::new(1, routing.clone(), dirs[0].path().to_path_buf());
        leader.add_group(GROUP, vec![2, 3]).expect("add data group");
        leader
            .groups_mut()
            .get_mut(&GROUP)
            .expect("group mounted")
            .election_deadline_override(Instant::now() - Duration::from_millis(1));
        leader.tick().expect("tick");
        leader
            .handle_pre_vote_response(
                GROUP,
                2,
                &nodedb_raft::message::PreVoteResponse {
                    term: 0,
                    vote_granted: true,
                },
            )
            .expect("the group is mounted");
        let (_, term) = leader.group_leader_at_term(GROUP);
        leader
            .handle_request_vote_response(
                GROUP,
                2,
                &nodedb_raft::message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
            )
            .expect("the group is mounted");
        assert!(leader.is_group_leader(GROUP), "one grant makes a quorum");
        assert!(term > 0, "the leader's term is ahead of voter 2's");

        // Voter 2 serves at term 0. Voter 3 binds an address and never serves.
        let mut behind = MultiRaft::new(2, routing, dirs[1].path().to_path_buf());
        behind.add_group(GROUP, vec![1, 3]).expect("add data group");
        let behind_transport = transport(2);
        let down_transport = transport(3);
        let mut topology = ClusterTopology::new();
        for (id, addr) in [
            (2, behind_transport.local_addr()),
            (3, down_transport.local_addr()),
        ] {
            topology.add_node(NodeInfo::new(id, addr, NodeState::Active));
        }
        let voter = Arc::new(RaftLoop::new(
            behind,
            Arc::clone(&behind_transport),
            Arc::new(RwLock::new(ClusterTopology::new())),
            NoCommits,
        ));
        let (shutdown, stop) = tokio::sync::watch::channel(false);
        let serving = {
            let voter = Arc::clone(&voter);
            tokio::spawn(async move {
                let _ = behind_transport.serve(voter, stop).await;
            })
        };
        let raft_loop = RaftLoop::new(
            leader,
            transport(1),
            Arc::new(RwLock::new(topology)),
            NoCommits,
        );

        let voter_handle = voter.multi_raft_handle();
        let catch_up = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut mr = voter_handle.lock().unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                mr.group_ordinal_floor(GROUP),
                Some(0),
                "voter 2 refused every ask while it was behind the term"
            );
            mr.handle_request_vote(&RequestVoteRequest {
                term,
                candidate_id: 1,
                last_log_index: u64::MAX,
                last_log_term: u64::MAX,
                group_id: GROUP,
                transfer: true,
            })
            .expect("the group is mounted");
        };
        let (spread, ()) = tokio::join!(
            raft_loop.spread_ordinal_floor(GROUP, FLOOR, ROUND),
            catch_up
        );

        assert_eq!(spread.expect("voter 2 completes the quorum"), FLOOR);
        assert_eq!(
            voter_handle
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .group_ordinal_floor(GROUP),
            Some(FLOOR)
        );
        let _ = shutdown.send(true);
        serving.abort();
        drop(down_transport);
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The data-group leader's write gate.
//!
//! The leader of a vShard's data group admits every proposal to that group
//! through the host's write gate before the entry enters its log. The gate
//! takes the write's lock keys on the vShard's Calvin lock table. A Calvin
//! transaction that needs one of those keys then waits until this leader
//! started the entry's apply, so its own effects order after the write.
//!
//! A local proposal and a forwarded one pass the same gate: both reach the
//! leader through [`RaftLoop::propose_admitted`]. `nodedb-cluster` cannot
//! depend on `nodedb`, so the gate lives in the host crate behind
//! [`DataProposeGate`].
//!
//! The gate also stamps the entry with what only the leader decides: the
//! host returns the stamped bytes, and the leader proposes those in place of
//! the bytes the proposer sent.
//!
//! Leadership can move between the gate and the log. Each admission names the
//! leadership it ran for, and the propose rechecks that leadership under the
//! lock that appends the entry. On a mismatch the entry passes the gate again.
//! So no leader appends an entry its gate did not admit at its current term.

use crate::error::{CalvinError, ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::multi_raft::{AdmissionTerm, GatedPropose};

use super::loop_core::{CommitApplier, RaftLoop};

/// The lock keys one admitted proposal holds. Dropping it releases them.
pub trait ProposeHold: Send {
    /// The proposal landed at `log_index` of `group_id`. The host keeps the
    /// keys until this node starts the apply of that entry.
    fn landed(self: Box<Self>, group_id: u64, log_index: u64);
}

/// What the write gate admitted for one entry.
#[derive(Default)]
pub struct Admitted {
    /// The lock keys the entry holds. `None` when it takes none.
    pub hold: Option<Box<dyn ProposeHold>>,
    /// The bytes the leader proposes in place of the bytes the proposer
    /// sent: the entry with the leader's stamps. `None` proposes the bytes
    /// as sent.
    pub entry: Option<Vec<u8>>,
}

/// The host's write gate for data-group proposals on their leader.
#[async_trait::async_trait]
pub trait DataProposeGate: Send + Sync + 'static {
    /// Admit the encoded entry `entry`, proposed to the group of
    /// `vshard_id`, on this leader. A contended entry waits for its keys
    /// until `deadline`, the proposer's own deadline.
    ///
    /// - `Ok(admitted)` with no `hold`: the entry takes no lock key.
    /// - `Ok(admitted)` with a `hold`: the entry holds its keys until the
    ///   hold releases them.
    /// - `Err(ClusterError::Calvin(CalvinError::RouteToSequencer))`: a held
    ///   key blocks the write, and the proposer submits it to the Calvin
    ///   sequencer.
    /// - `Err(ClusterError::Calvin(CalvinError::AdmissionTimedOut))`: the
    ///   keys stayed held until `deadline`. Nothing is proposed.
    ///
    /// `ordinal_floor` is the group's floor this leader gathered: every
    /// system-time ordinal the entry stamps lies above it, so no write lands
    /// at or below a read cut an earlier leader closed.
    async fn admit(
        &self,
        vshard_id: u32,
        entry: &[u8],
        deadline: tokio::time::Instant,
        ordinal_floor: i64,
    ) -> Result<Admitted>;
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Propose `data` to the group of `vshard_id` on this node. When this
    /// node leads the group, the write gate admits the entry first, waiting
    /// no later than `deadline`, the proposer's deadline.
    ///
    /// A node that does not lead the group proposes with no gate. The
    /// propose refuses with `NotLeader`, and the caller forwards the entry to
    /// the leader, which gates it there. A propose that fails after the gate
    /// admitted the entry drops its hold, which releases the keys.
    ///
    /// When leadership moves before the entry lands, the hold drops and the
    /// entry passes the gate again. Past `deadline` it refuses with
    /// `CalvinError::AdmissionTimedOut`, and nothing is proposed.
    pub(super) async fn propose_admitted(
        &self,
        vshard_id: u32,
        data: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<(u64, u64)> {
        let Some(gate) = &self.data_propose_gate else {
            return self.propose(vshard_id, data.to_vec());
        };
        loop {
            let led = self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .admission_term(vshard_id);
            let Admitted { hold, entry } = match led {
                Some(AdmissionTerm { group_id, .. }) => {
                    let floor = self.admission_floor(group_id, deadline).await?;
                    gate.admit(vshard_id, data, deadline, floor).await?
                }
                None => Admitted::default(),
            };
            let entry = entry.unwrap_or_else(|| data.to_vec());
            let proposed = self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .propose_gated(vshard_id, entry, led)?;
            match proposed {
                GatedPropose::Landed {
                    group_id,
                    log_index,
                } => {
                    if let Some(hold) = hold {
                        hold.landed(group_id, log_index);
                    }
                    return Ok((group_id, log_index));
                }
                GatedPropose::Readmit => {
                    // The keys free before the next admission asks for them.
                    drop(hold);
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ClusterError::Calvin(CalvinError::AdmissionTimedOut));
                    }
                }
            }
        }
    }

    /// The ordinal floor a write this leader admits to `group_id` stamps
    /// above, gathered once per leadership term (see `super::ordinal_floor`).
    /// A leader that lost the group meanwhile refuses with `NotLeader`, the
    /// refusal a propose to a non-leader answers, so the proposer forwards.
    async fn admission_floor(&self, group_id: u64, deadline: tokio::time::Instant) -> Result<i64> {
        let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        match self.gathered_ordinal_floor(group_id, timeout).await {
            Err(ClusterError::ReadIndexNotLeader { .. }) => {
                let (leader, term) = self
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .group_leader_at_term(group_id);
                Err(ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                    leader_hint: (leader != 0).then_some(leader),
                    term,
                }))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock, RwLock};
    use std::time::{Duration, Instant};

    use nodedb_raft::message::{LogEntry, RequestVoteRequest};

    use super::*;
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;
    use crate::topology::ClusterTopology;
    use crate::transport::NexarTransport;

    const GROUP: u64 = 1;

    struct NoCommits;

    impl CommitApplier for NoCommits {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            entries.last().map_or(0, |entry| entry.index)
        }
    }

    /// A single voter elected in `group`.
    fn elect(mr: &mut MultiRaft, group: u64) {
        let node = mr.groups_mut().get_mut(&group).expect("group mounted");
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        for _ in 0..5 {
            if mr.is_group_leader(group) {
                return;
            }
            mr.tick().expect("tick");
        }
        panic!("a single voter elects itself in group {group}");
    }

    /// A gate that moves this leader to a later term during its first
    /// admission, as a node deposed and re-elected between the two lock holds.
    /// It stamps each admission with its ordinal, and a hold reports whether
    /// it dropped or landed.
    struct TermMovingGate {
        multi_raft: OnceLock<Arc<Mutex<MultiRaft>>>,
        admissions: AtomicUsize,
        floors: Mutex<Vec<i64>>,
        landed: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    struct CountedHold {
        landed: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    impl ProposeHold for CountedHold {
        fn landed(self: Box<Self>, _group_id: u64, _log_index: u64) {
            self.landed.fetch_add(1, Ordering::SeqCst);
            std::mem::forget(self);
        }
    }

    impl Drop for CountedHold {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl DataProposeGate for TermMovingGate {
        async fn admit(
            &self,
            _vshard_id: u32,
            _entry: &[u8],
            _deadline: tokio::time::Instant,
            ordinal_floor: i64,
        ) -> Result<Admitted> {
            let admission = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
            self.floors
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(ordinal_floor);
            if admission == 1
                && let Some(multi_raft) = self.multi_raft.get()
            {
                let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let (_, term) = mr.group_leader_at_term(GROUP);
                // A vote request of a later term deposes this leader.
                mr.handle_request_vote(&RequestVoteRequest {
                    term: term + 1,
                    candidate_id: 9,
                    last_log_index: 0,
                    last_log_term: 0,
                    group_id: GROUP,
                    transfer: true,
                })
                .expect("the group is mounted");
                assert!(!mr.is_group_leader(GROUP), "the vote deposed the leader");
                elect(&mut mr, GROUP);
            }
            Ok(Admitted {
                hold: Some(Box::new(CountedHold {
                    landed: Arc::clone(&self.landed),
                    dropped: Arc::clone(&self.dropped),
                })),
                entry: Some(format!("admission {admission}").into_bytes()),
            })
        }
    }

    /// Leadership moves while the gate admits. The first admission's entry
    /// never reaches the log and its hold drops. The entry lands with the
    /// stamp of the second admission, at the new term.
    #[tokio::test]
    async fn an_entry_admitted_at_an_earlier_term_passes_the_gate_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = MultiRaft::new(
            1,
            RoutingTable::uniform(1, &[1], 1),
            dir.path().to_path_buf(),
        );
        mr.add_group(GROUP, vec![]).expect("add group");
        elect(&mut mr, GROUP);
        let transport = Arc::new(
            NexarTransport::new(
                1,
                "127.0.0.1:0".parse().expect("loopback"),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .expect("transport"),
        );
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let landed = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(TermMovingGate {
            multi_raft: OnceLock::new(),
            admissions: AtomicUsize::new(0),
            floors: Mutex::new(Vec::new()),
            landed: Arc::clone(&landed),
            dropped: Arc::clone(&dropped),
        });
        let raft_loop =
            RaftLoop::new(mr, transport, topology, NoCommits).with_data_propose_gate(gate.clone());
        assert!(gate.multi_raft.set(raft_loop.multi_raft_handle()).is_ok());
        let term_before = raft_loop
            .multi_raft_handle()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .group_leader_at_term(GROUP)
            .1;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let (group_id, log_index) = raft_loop
            .propose_admitted(0, b"sent", deadline)
            .await
            .expect("the second admission lands");

        assert_eq!(group_id, GROUP);
        assert_eq!(gate.admissions.load(Ordering::SeqCst), 2);
        assert_eq!(landed.load(Ordering::SeqCst), 1, "the second hold landed");
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "the first hold dropped");
        let handle = raft_loop.multi_raft_handle();
        let mut mr = handle.lock().unwrap_or_else(|p| p.into_inner());
        assert!(mr.group_leader_at_term(GROUP).1 > term_before);
        let node = mr.groups_mut().get(&GROUP).expect("group mounted");
        let stored = node
            .log_entry_at(log_index)
            .expect("the entry is in the log");
        assert_eq!(stored.data, b"admission 2");
    }

    /// A deadline that passed when leadership moved refuses the entry
    /// unproposed.
    #[tokio::test]
    async fn a_deadline_past_a_leadership_move_refuses_unproposed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = MultiRaft::new(
            1,
            RoutingTable::uniform(1, &[1], 1),
            dir.path().to_path_buf(),
        );
        mr.add_group(GROUP, vec![]).expect("add group");
        elect(&mut mr, GROUP);
        let transport = Arc::new(
            NexarTransport::new(
                1,
                "127.0.0.1:0".parse().expect("loopback"),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .expect("transport"),
        );
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let gate = Arc::new(TermMovingGate {
            multi_raft: OnceLock::new(),
            admissions: AtomicUsize::new(0),
            floors: Mutex::new(Vec::new()),
            landed: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        });
        let raft_loop =
            RaftLoop::new(mr, transport, topology, NoCommits).with_data_propose_gate(gate.clone());
        assert!(gate.multi_raft.set(raft_loop.multi_raft_handle()).is_ok());

        let refused = raft_loop
            .propose_admitted(0, b"sent", tokio::time::Instant::now())
            .await;
        assert!(
            matches!(
                refused,
                Err(ClusterError::Calvin(CalvinError::AdmissionTimedOut))
            ),
            "{refused:?}"
        );
        assert_eq!(gate.admissions.load(Ordering::SeqCst), 1);
    }
}

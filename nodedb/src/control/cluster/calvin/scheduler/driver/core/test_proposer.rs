// SPDX-License-Identifier: BUSL-1.1

//! Test fixtures for the scheduler's sequencer proposals: a capturing
//! [`SequencerProposer`] and a helper that makes the fixture node lead its
//! vShard's data group.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_cluster::calvin::SequencerEntry;

use super::scheduler::Scheduler;
use super::sequencer_proposer::{ProposeDispatch, SequencerProposeError, SequencerProposer};

/// Records every proposal it receives. The first `fail_first` proposals fail
/// the way a node that does not lead the sequencer group fails. Every later
/// one succeeds.
#[derive(Default)]
pub(super) struct CapturingProposer {
    fail_first: usize,
    /// Every proposal, decoded, with whether it succeeded.
    attempts: Mutex<Vec<(SequencerEntry, bool)>>,
}

impl CapturingProposer {
    /// A proposer that accepts every proposal.
    pub(super) fn accepting() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A proposer that refuses the first `n` proposals.
    pub(super) fn failing_first(n: usize) -> Arc<Self> {
        Arc::new(Self {
            fail_first: n,
            attempts: Mutex::new(Vec::new()),
        })
    }

    /// Number of proposals received, failed ones included.
    pub(super) fn attempt_count(&self) -> usize {
        self.attempts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// The proposals that succeeded, in order.
    pub(super) fn accepted(&self) -> Vec<SequencerEntry> {
        self.attempts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(_, ok)| *ok)
            .map(|(entry, _)| entry.clone())
            .collect()
    }
}

impl SequencerProposer for CapturingProposer {
    fn propose(&self, bytes: Vec<u8>) -> Result<ProposeDispatch, SequencerProposeError> {
        let entry: SequencerEntry =
            zerompk::from_msgpack(&bytes).expect("scheduler proposes a valid SequencerEntry");
        let mut attempts = self.attempts.lock().unwrap_or_else(|p| p.into_inner());
        let ok = attempts.len() >= self.fail_first;
        attempts.push((entry, ok));
        if ok {
            Ok(ProposeDispatch::Local)
        } else {
            Err(SequencerProposeError::NoLeader)
        }
    }
}

/// Make the fixture node the elected leader of the data group that owns
/// `scheduler`'s vShard, apply the term's no-op, and open the scheduler's
/// stage gate, so it stages its grants and proposes its entries.
pub(super) fn lead_data_group(scheduler: &mut Scheduler) {
    let group_id = elect_data_group_leader(scheduler);
    let term_start = scheduler
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .term_start_index(group_id)
        .expect("the elected node names its term start");
    scheduler
        .shared
        .applied_index_watcher(group_id)
        .bump(term_start);
    scheduler.refresh_role();
    assert!(scheduler.role.is_leader(), "the stage gate opens");
}

/// Make the fixture node the elected leader of the data group that owns
/// `scheduler`'s vShard, with its term's no-op not yet applied. Returns
/// the group.
pub(super) fn elect_data_group_leader(scheduler: &Scheduler) -> u64 {
    let mut mr = scheduler
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let group_id = mr
        .routing()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(scheduler.vshard_id)
        .expect("the fixture routing maps every vShard");
    if !mr.contains_group(group_id) {
        mr.add_group(group_id, vec![]).expect("add data group");
    }
    if let Some(node) = mr.groups_mut().get_mut(&group_id) {
        // no-determinism: test-only forced election deadline so the single voter campaigns immediately.
        node.election_deadline_override(Instant::now() - Duration::from_millis(1));
    }
    for _ in 0..20 {
        mr.tick().expect("tick");
        if mr.vshard_role_is_leader(scheduler.vshard_id) {
            return group_id;
        }
    }
    panic!("data group did not elect the single fixture node");
}

/// Step the fixture node down from its data group: a new term starts on
/// another node.
pub(super) fn step_down_from_data_group(scheduler: &Scheduler) {
    let mut mr = scheduler
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let group_id = mr
        .routing()
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(scheduler.vshard_id)
        .expect("the fixture routing maps every vShard");
    let term = mr.leader_term(group_id).expect("the fixture node leads");
    // A heartbeat of a later term from another node deposes this one.
    mr.handle_append_entries(&nodedb_raft::message::AppendEntriesRequest {
        term: term + 1,
        leader_id: scheduler.shared.node_id + 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
        group_id,
        round: 0,
        replicated_floor: 0,
    })
    .expect("the group takes the heartbeat");
    assert!(!mr.vshard_role_is_leader(scheduler.vshard_id));
}

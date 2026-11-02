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
        let mut pending = round.voters.clone();
        while held < round.quorum {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(ClusterError::ReadIndexTimeout {
                    group_id,
                    waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                });
            }
            let request = OrdinalFloorRequest {
                group_id,
                term: round.term,
                floor: round.floor,
            };
            let mut asks: futures::stream::FuturesUnordered<_> = pending
                .iter()
                .map(|&peer| {
                    let request = request.clone();
                    async move { (peer, self.ask_floor(peer, request, remaining).await) }
                })
                .collect();
            let mut retry = Vec::new();
            // A quorum ends the round at once: a voter that is down holds no
            // answer back past it.
            while held < round.quorum
                && let Some((peer, answer)) = asks.next().await
            {
                match answer {
                    Ok(OrdinalFloorResponse {
                        floor: Some(peer_floor),
                        ..
                    }) => {
                        held += 1;
                        highest = highest.max(peer_floor);
                    }
                    Ok(OrdinalFloorResponse { term, floor: None }) if term > round.term => {
                        return Err(ClusterError::ReadIndexNotLeader { group_id });
                    }
                    Ok(OrdinalFloorResponse { floor: None, .. }) | Err(_) => retry.push(peer),
                }
            }
            pending = retry;
            if held < round.quorum {
                tokio::time::sleep(RETRY_PAUSE.min(remaining)).await;
            }
        }
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

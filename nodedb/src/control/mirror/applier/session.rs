// SPDX-License-Identifier: BUSL-1.1

//! One leadership's session: its fence, its tail, and its rounds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::mirror::{MAX_TAIL_BYTES, MirrorError, MirrorSourceClient, MirrorTailRequest};
use nodedb_types::{
    DatabaseId, MirrorCutAdvance, MirrorCutState, MirrorFailure, MirrorGroupPosition,
};

use crate::control::metadata_proposer::propose_mirror_cut;
use crate::control::mirror::source::cut_loop::metadata_term;
use crate::control::state::SharedState;

use super::batch::{GroupCursor, MetadataCursor, next_cut, take_cut, top_seq, wants_fetch};
use super::driver::{IDLE_POLL, MirrorStop, SessionEnd, follows};
use super::progress::{AppliedTracker, EpochMint};
use super::propose::MirrorRaft;

/// How long a fetch waits at the source for a new entry.
const FETCH_WAIT_MS: u64 = 500;
/// The longest wait between two attempts of one step.
pub(super) const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(5);
/// The first wait between two attempts of one step.
pub(super) const FIRST_RETRY_BACKOFF: Duration = Duration::from_millis(50);
/// How often a session asks the source for its groups again.
const HELLO_INTERVAL: Duration = Duration::from_secs(30);
/// How long a session goes with no new cut before it reports applied progress
/// in a `MirrorCut` of its own.
const APPLIED_REPORT_IDLE: Duration = Duration::from_secs(2);

/// Why a fence did not land.
pub(super) enum FenceError {
    /// A later leader owns the mirror.
    Lost,
    Retry(crate::Error),
}

pub(super) struct Session<'a> {
    pub(super) state: &'a SharedState,
    pub(super) database: DatabaseId,
    pub(super) client: &'a MirrorSourceClient,
    pub(super) link_id: u64,
    term: u64,
    pub(super) epoch: u64,
    mint: EpochMint,
    pub(super) ready_cut: u64,
    pub(super) cursors: Vec<GroupCursor>,
    pub(super) metadata: MetadataCursor,
    pub(super) applied: AppliedTracker,
    last_hello: Instant,
    pub(super) last_raise: Instant,
}

impl<'a> Session<'a> {
    pub(super) fn open(
        state: &'a SharedState,
        database: DatabaseId,
        client: &'a MirrorSourceClient,
        link_id: u64,
        term: u64,
    ) -> Self {
        Self {
            state,
            database,
            client,
            link_id,
            term,
            epoch: 0,
            mint: EpochMint::default(),
            ready_cut: 0,
            cursors: Vec::new(),
            metadata: MetadataCursor::new(0),
            applied: AppliedTracker::default(),
            last_hello: Instant::now(),
            last_raise: Instant::now(),
        }
    }

    pub(super) fn raft(&self) -> crate::Result<&Arc<dyn MirrorRaft>> {
        self.state
            .mirror
            .raft
            .get()
            .ok_or_else(|| crate::Error::Internal {
                detail: "the mirror applier runs before Raft started".to_string(),
            })
    }

    pub(super) fn still_leads(&self) -> bool {
        metadata_term(self.state) == Some(self.term)
    }

    pub(super) fn cut_state(&self) -> crate::Result<MirrorCutState> {
        Ok(self
            .state
            .credentials
            .catalog()
            .get_mirror_cut_state(self.database)?
            .unwrap_or_default())
    }

    pub(super) async fn run(mut self) -> SessionEnd {
        let state = match self.start().await {
            Ok(state) => state,
            Err(end) => return end,
        };
        self.resume(&state);
        loop {
            if !self.still_leads() {
                return SessionEnd::LostLeadership;
            }
            match follows(self.state, self.database) {
                Ok(Some(true)) => {}
                Ok(Some(false)) | Err(_) => return SessionEnd::LostLeadership,
                Ok(None) => return SessionEnd::Stop(MirrorStop::Ended),
            }
            if let Err(end) = self.step().await {
                return end;
            }
        }
    }

    /// Learn the source's groups, then fence. Both repeat until they land, while
    /// this node leads.
    async fn start(&mut self) -> Result<MirrorCutState, SessionEnd> {
        let mut backoff = FIRST_RETRY_BACKOFF;
        loop {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            self.hello_groups().await?;
            if self.cursors.is_empty() {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
                continue;
            }
            match self.fence().await {
                Ok(state) => return Ok(state),
                Err(FenceError::Lost) => return Err(SessionEnd::LostLeadership),
                Err(FenceError::Retry(error)) => {
                    tracing::warn!(
                        database = self.database.as_u64(),
                        %error,
                        "mirror applier: the fence did not land"
                    );
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
    }

    /// Ask the source which groups to tail. A group the session does not tail
    /// joins at the position the mirror's state holds for it.
    async fn hello_groups(&mut self) -> Result<(), SessionEnd> {
        let ack = match self.client.hello().await {
            Ok(ack) => ack,
            Err(MirrorError::Refused(refusal)) if !refusal.is_transient() => {
                return Err(SessionEnd::Stop(MirrorStop::Refused(refusal)));
            }
            Err(error) => {
                tracing::warn!(
                    database = self.database.as_u64(),
                    %error,
                    "mirror applier: the source did not answer the hello"
                );
                return Ok(());
            }
        };
        self.last_hello = Instant::now();
        if ack.source_database_id != self.database.as_u64() {
            return Err(MirrorFailure::UnsupportedSourceOp {
                detail: format!(
                    "the source database id is {}, and this mirror holds {}",
                    ack.source_database_id,
                    self.database.as_u64()
                ),
            }
            .into());
        }
        let held = self
            .cut_state()
            .map(|state| state.positions)
            .unwrap_or_default();
        for group in ack
            .groups
            .into_iter()
            .filter(|group| *group != METADATA_GROUP_ID && *group != SEQUENCER_GROUP_ID)
        {
            if self.cursors.iter().all(|cursor| cursor.group != group) {
                self.cursors
                    .push(GroupCursor::new(group, position_of(&held, group)));
            }
        }
        Ok(())
    }

    /// Mint a new epoch and raise the fence to it, with no cut. Every copy of a
    /// lower epoch above `ready_cut` then skips on every replica.
    pub(super) async fn fence(&mut self) -> Result<MirrorCutState, FenceError> {
        let current = self.cut_state().map_err(FenceError::Retry)?;
        let epoch = self
            .mint
            .next(self.term, current.fence_epoch)
            .ok_or(FenceError::Lost)?;
        let advance = MirrorCutAdvance {
            cut: current.ready_cut,
            epoch,
            ..MirrorCutAdvance::default()
        };
        propose_mirror_cut(self.state, self.database.as_u64(), advance, Vec::new())
            .await
            .map_err(FenceError::Retry)?;
        let state = self.cut_state().map_err(FenceError::Retry)?;
        if state.fence_epoch > epoch {
            return Err(FenceError::Lost);
        }
        if state.fence_epoch != epoch {
            return Err(FenceError::Retry(crate::Error::Internal {
                detail: format!(
                    "mirror applier: the fence holds {} after proposing {epoch}",
                    state.fence_epoch
                ),
            }));
        }
        self.epoch = epoch;
        Ok(state)
    }

    /// Place every cursor at the replicated positions.
    fn resume(&mut self, state: &MirrorCutState) {
        self.ready_cut = state.ready_cut;
        self.cursors = self
            .cursors
            .iter()
            .map(|cursor| {
                GroupCursor::new(cursor.group, position_of(&state.positions, cursor.group))
            })
            .collect();
        self.metadata = MetadataCursor::new(position_of(&state.positions, METADATA_GROUP_ID));
    }

    /// Do one round's work: fetch, then apply the next cut if every group holds
    /// its barrier. With no cut, report what the groups applied.
    async fn step(&mut self) -> Result<(), SessionEnd> {
        if self.last_hello.elapsed() >= HELLO_INTERVAL {
            self.hello_groups().await?;
            self.last_hello = Instant::now();
        }
        self.fetch_groups().await?;
        let Some(cut) = next_cut(&self.cursors) else {
            if self.last_raise.elapsed() >= APPLIED_REPORT_IDLE {
                self.report_applied().await;
            }
            return Ok(());
        };
        let Some(batch) = take_cut(&mut self.cursors, cut) else {
            return Ok(());
        };
        self.apply_cut(batch).await
    }

    async fn fetch_groups(&mut self) -> Result<(), SessionEnd> {
        let top = top_seq(&self.cursors);
        let wanted: Vec<usize> = self
            .cursors
            .iter()
            .enumerate()
            .filter(|(_, cursor)| wants_fetch(cursor, top))
            .map(|(at, _)| at)
            .collect();
        if wanted.is_empty() {
            return Ok(());
        }
        let requests: Vec<MirrorTailRequest> = wanted
            .iter()
            .map(|at| self.request(self.cursors[*at].group, self.cursors[*at].next_index))
            .collect();
        let client = self.client;
        let answers = join_all(requests.iter().map(|request| client.tail(request))).await;
        for (at, answer) in wanted.into_iter().zip(answers) {
            match answer {
                Ok(batch) => {
                    self.state.mirror.links.record_received(self.database);
                    self.cursors[at].ingest(batch, self.database, self.ready_cut)?;
                }
                Err(error) => self.note_source_error(error)?,
            }
        }
        Ok(())
    }

    fn request(&self, group: u64, from_index: u64) -> MirrorTailRequest {
        MirrorTailRequest {
            link_id: self.link_id,
            group,
            from_index,
            max_bytes: MAX_TAIL_BYTES,
            wait_ms: FETCH_WAIT_MS,
        }
    }

    /// A refusal the next request cannot clear stops the link. Any other error
    /// leaves every cursor where it was, and the next round asks again.
    pub(super) fn note_source_error(&self, error: MirrorError) -> Result<(), SessionEnd> {
        match error {
            MirrorError::Refused(refusal) if !refusal.is_transient() => {
                Err(SessionEnd::Stop(MirrorStop::Refused(refusal)))
            }
            other => {
                tracing::warn!(
                    database = self.database.as_u64(),
                    error = %other,
                    "mirror applier: a source request failed; the next round retries it"
                );
                Ok(())
            }
        }
    }

    /// Fetch the source's metadata log through `target`.
    pub(super) async fn fetch_metadata(&mut self, target: u64) -> Result<(), SessionEnd> {
        while !self.metadata.reached(target) {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            let request = self.request(METADATA_GROUP_ID, self.metadata.next_index);
            match self.client.tail(&request).await {
                Ok(batch) => self.metadata.ingest(batch),
                Err(error) => {
                    self.note_source_error(error)?;
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
        }
        Ok(())
    }
}

/// `group`'s place in `held`. A group with no place starts from its first entry.
fn position_of(held: &[MirrorGroupPosition], group: u64) -> u64 {
    held.iter()
        .find(|position| position.group_id == group)
        .map_or(0, |position| position.log_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_with_no_place_starts_from_its_first_entry() {
        let held = [MirrorGroupPosition {
            group_id: 3,
            log_index: 9,
        }];
        assert_eq!(position_of(&held, 3), 9);
        assert_eq!(position_of(&held, 4), 0);
    }
}

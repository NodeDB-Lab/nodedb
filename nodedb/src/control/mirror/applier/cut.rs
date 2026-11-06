// SPDX-License-Identifier: BUSL-1.1

//! Apply one source cut: commit its entries, raise it, ack it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::mirror::MirrorAck;
use nodedb_types::{MirrorCutAdvance, MirrorEntryStamp, MirrorGroupPosition};

use crate::control::metadata_proposer::propose_mirror_cut;
use crate::control::security::auth_fence::cluster::{group_of_vshard, routed_groups};
use crate::control::wal_replication::types::MirrorCalvinFloor;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

use super::batch::{CutBatch, positions};
use super::driver::SessionEnd;
use super::progress::PendingCut;
use super::propose::{GroupProposal, SealAt, propose_group};
use super::rewrite::{CatalogCheck, rewrite_entry};
use super::session::{FIRST_RETRY_BACKOFF, FenceError, MAX_RETRY_BACKOFF, Session};

/// How long one attempt of one cut can take in one group.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

impl Session<'_> {
    pub(super) async fn apply_cut(&mut self, batch: CutBatch) -> Result<(), SessionEnd> {
        let target = batch.metadata_target.max(self.metadata.position);
        self.fetch_metadata(target).await?;
        let catalog = {
            let mut check = CatalogCheck::new(self.state.credentials.catalog(), self.database);
            self.metadata.take_through(target, &mut check)?
        };
        let resume = positions(&self.cursors, &self.metadata);
        let seals = self.commit_batch(&batch).await?;
        self.raise_cut(&batch, catalog, resume.clone()).await?;
        self.ready_cut = batch.cut;
        self.last_raise = Instant::now();
        self.applied.push(PendingCut {
            cut: batch.cut,
            hlc_ns: batch.hlc_ns,
            cap_ordinal: batch.mark.cap_ordinal,
            seals,
        });
        self.ack(batch.cut, resume).await
    }

    /// Commit the cut's entries and seals in every mirror group. A failed
    /// attempt fences a new epoch, and the next attempt proposes the whole cut.
    async fn commit_batch(&mut self, batch: &CutBatch) -> Result<Vec<SealAt>, SessionEnd> {
        let mut backoff = FIRST_RETRY_BACKOFF;
        loop {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            match self.attempt(batch).await {
                Ok(seals) => return Ok(seals),
                Err(error) => {
                    tracing::warn!(
                        database = self.database.as_u64(),
                        cut = batch.cut,
                        epoch = self.epoch,
                        %error,
                        "mirror applier: a cut attempt failed; a new epoch proposes it again"
                    );
                }
            }
            loop {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
                match self.fence().await {
                    Ok(_) => break,
                    Err(FenceError::Lost) => return Err(SessionEnd::LostLeadership),
                    Err(FenceError::Retry(error)) => {
                        tracing::warn!(%error, "mirror applier: the fence did not land");
                        if !self.still_leads() {
                            return Err(SessionEnd::LostLeadership);
                        }
                    }
                }
            }
        }
    }

    async fn attempt(&self, batch: &CutBatch) -> crate::Result<Vec<SealAt>> {
        let raft = Arc::clone(self.raft()?);
        let proposals = self.proposals(batch)?;
        let deadline = tokio::time::Instant::now() + ATTEMPT_TIMEOUT;
        let answers = join_all(
            proposals
                .into_iter()
                .map(|proposal| propose_group(raft.as_ref(), proposal, deadline)),
        )
        .await;
        answers.into_iter().collect()
    }

    /// Encode the cut for this epoch: each entry in its mirror group, then each
    /// group's seal. The source's order within a group's vShards holds.
    fn proposals(&self, batch: &CutBatch) -> crate::Result<Vec<GroupProposal>> {
        // This node's own catalog, which holds the mirror. Every replica waits
        // for it, so none reads "unknown database" as "dropped".
        let metadata_floor = self.state.applied_index_watcher(METADATA_GROUP_ID).floor();
        let mut proposals: Vec<GroupProposal> = routed_groups(self.state)
            .into_iter()
            .filter(|group| *group != SEQUENCER_GROUP_ID && *group != METADATA_GROUP_ID)
            .map(|group| GroupProposal {
                group,
                entries: Vec::new(),
            })
            .collect();
        for source in &batch.entries {
            let mut entry = source.entry.clone();
            rewrite_entry(
                &mut entry,
                MirrorEntryStamp {
                    source_group: source.group,
                    source_index: source.index,
                    cut: batch.cut,
                    epoch: self.epoch,
                },
                metadata_floor,
            );
            let group = group_of_vshard(self.state, entry.vshard_id)?;
            let proposal = proposals
                .iter_mut()
                .find(|proposal| proposal.group == group)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("mirror applier: group {group} is not in the routing table"),
                })?;
            proposal.entries.push((entry.vshard_id, entry.encode()?));
        }
        for proposal in &mut proposals {
            let vshard = self.seal_vshard(proposal.group)?;
            let mut seal = ReplicatedEntry::new(
                0,
                self.database.as_u64(),
                vshard,
                ReplicatedWrite::MirrorSeal {
                    last: false,
                    calvin_floors: self.floors_of(batch, proposal.group)?,
                },
            );
            rewrite_entry(
                &mut seal,
                MirrorEntryStamp {
                    source_group: 0,
                    source_index: 0,
                    cut: batch.cut,
                    epoch: self.epoch,
                },
                metadata_floor,
            );
            proposal.entries.push((vshard, seal.encode()?));
        }
        Ok(proposals)
    }

    fn seal_vshard(&self, group: u64) -> crate::Result<u32> {
        self.state
            .cluster_routing
            .as_ref()
            .and_then(|routing| {
                routing
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .vshards_for_group(group)
                    .first()
                    .copied()
            })
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("mirror applier: group {group} homes no vShard"),
            })
    }

    fn floors_of(&self, batch: &CutBatch, group: u64) -> crate::Result<Vec<MirrorCalvinFloor>> {
        let mut floors = Vec::new();
        for floor in &batch.floors {
            if group_of_vshard(self.state, floor.vshard)? == group {
                floors.push(*floor);
            }
        }
        Ok(floors)
    }

    /// Propose the `MirrorCut` of a committed batch. It can repeat: an advance
    /// the row already holds changes nothing.
    async fn raise_cut(
        &mut self,
        batch: &CutBatch,
        catalog: Vec<Vec<u8>>,
        resume: Vec<MirrorGroupPosition>,
    ) -> Result<(), SessionEnd> {
        let mut advance = MirrorCutAdvance {
            cut: batch.cut,
            epoch: self.epoch,
            positions: resume,
            ..MirrorCutAdvance::default()
        };
        self.fill_applied(&mut advance).await;
        let mut backoff = FIRST_RETRY_BACKOFF;
        loop {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            let proposed = propose_mirror_cut(
                self.state,
                self.database.as_u64(),
                advance.clone(),
                catalog.clone(),
            )
            .await;
            if let Err(error) = proposed {
                tracing::warn!(cut = batch.cut, %error, "mirror applier: a cut did not land");
            } else {
                match self.cut_state() {
                    Ok(state)
                        if state.ready_cut >= batch.cut && state.fence_epoch == self.epoch =>
                    {
                        return Ok(());
                    }
                    // A later fence took the mirror, or PROMOTE sealed it.
                    Ok(_) => return Err(SessionEnd::LostLeadership),
                    Err(error) => {
                        tracing::warn!(%error, "mirror applier: a cut state read failed");
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
    }

    /// Put what the groups applied on `advance`.
    async fn fill_applied(&mut self, advance: &mut MirrorCutAdvance) {
        let Ok(raft) = self.raft().map(Arc::clone) else {
            return;
        };
        let newest = self
            .applied
            .take_newest_applied(self.state, raft.as_ref(), self.database)
            .await;
        if let Some(cut) = newest {
            advance.applied_cut = cut.cut;
            advance.applied_cut_hlc_ns = cut.hlc_ns;
            advance.cap_ordinal = cut.cap_ordinal;
        }
    }

    /// Tell the metadata log what the groups applied, with no new cut.
    pub(super) async fn report_applied(&mut self) {
        let mut advance = MirrorCutAdvance {
            cut: self.ready_cut,
            epoch: self.epoch,
            ..MirrorCutAdvance::default()
        };
        self.fill_applied(&mut advance).await;
        if advance.applied_cut == 0 {
            return;
        }
        if let Err(error) =
            propose_mirror_cut(self.state, self.database.as_u64(), advance, Vec::new()).await
        {
            tracing::warn!(%error, "mirror applier: the applied cut did not land");
        }
    }

    /// Tell the source how far the mirror holds its log. A later cut acks again,
    /// so an error here costs only retention.
    async fn ack(&self, cut: u64, resume: Vec<MirrorGroupPosition>) -> Result<(), SessionEnd> {
        let ack = MirrorAck {
            link_id: self.link_id,
            cut,
            positions: resume,
        };
        match self.client.ack(&ack).await {
            Ok(()) => Ok(()),
            Err(error) => self.note_source_error(error),
        }
    }
}

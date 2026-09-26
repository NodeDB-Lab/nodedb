// SPDX-License-Identifier: BUSL-1.1

//! A backup's consistent cut.
//!
//! The cut picks the envelope watermark `W`, then waits until every user
//! write committed below `W` has a final outcome on this node, and only then
//! lets the backup snapshot. Afterwards every write is one of two kinds:
//!
//! - committed below `W`: applied before the snapshot, so the backup holds it;
//! - committed at or above `W`: its mark is above `W`, so a restore of this
//!   backup refuses it.
//!
//! Three waits make the cut:
//!
//! - **Raft data groups.** A replicated write carries its proposer's commit
//!   stamp and applies in log order. The cut proposes a
//!   [`ReplicatedWrite::CutBarrier`] carrying `W` into every data group this
//!   node hosts and waits for this node's apply of it. Every entry before the
//!   barrier applied first. Every entry after it records a commit HLC above
//!   `W`, however early its proposer stamped it.
//! - **Calvin transactions.** A Calvin transaction commits at its place in
//!   the sequencer log. The cut proposes a `CutMarker` carrying `W` into the
//!   sequencer log and waits until every Calvin scheduler on this node passed
//!   it: every transaction delivered before the marker installed or dropped.
//!   Every transaction delivered after it records a commit HLC above `W`.
//! - **Local write windows.** A write the funnel appends here stamps itself
//!   after its record is minted inside an outcome-floor window. The cut reads
//!   the highest LSN any window minted, after it picks `W`, and waits for the
//!   outcome floor to reach it. A record minted later stamps itself above `W`.
//!   On a server with no Raft groups a write stamps itself before it mints, so
//!   its mark is durable first. The cut first waits for every such stamp at or
//!   below `W` to mint, then reads the highest LSN.
//!
//! Two stamps can share a wall time. Once it picks `W`, the cut moves this
//! node's clock past `W`, so every later stamp here reads above it.
//!
//! The waits bind this node's replicas. A remote source node takes the same
//! cut at `W` itself before it snapshots: the snapshot plan sent to it carries
//! `W`, and its Control Plane runs [`cut_at`] first.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::SequencerEntry;
use nodedb_types::Hlc;

use crate::Error;
use crate::control::security::auth_fence::cluster::{group_of_vshard, hosts_group, routed_groups};
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, propose_replicated_entry};
use crate::types::{DatabaseId, VShardId};

/// Pick the envelope watermark and wait until every write committed below it
/// has a final outcome on this node. Returns the watermark.
pub(super) async fn consistent_cut(state: &Arc<SharedState>, tenant_id: u64) -> Result<u64, Error> {
    let watermark = state.hlc_clock.now().wall_ns;
    cut_at(state, tenant_id, watermark).await?;
    Ok(watermark)
}

/// Take the consistent cut at `watermark` on this node: wait until every
/// write committed below it has a final outcome here. A remote source node
/// runs it for the watermark the backup's coordinator picked.
pub(crate) async fn cut_at(
    state: &Arc<SharedState>,
    tenant_id: u64,
    watermark: u64,
) -> Result<(), Error> {
    state
        .hlc_clock
        .update(Hlc::new(watermark.saturating_add(1), 0));
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let deadline = tokio::time::Instant::now() + timeout;
    // A local write stamped at or below the watermark has not always minted
    // its record yet. Every stamp taken from here on reads above it.
    if !state
        .tenant_marks
        .await_local_stamps_minted(watermark, deadline)
        .await
    {
        return Err(Error::Internal {
            detail: format!(
                "backup: local writes stamped at or below watermark {watermark} did not mint \
                 their WAL records within {}s, so the backup cannot take a consistent cut. \
                 Retry the backup",
                timeout.as_secs(),
            ),
        });
    }
    // Read after the watermark: a record minted after this read stamps its
    // write above the watermark.
    let target = state.outcome_floor.max_noted();

    let (groups, calvin) = tokio::join!(
        cut_data_groups(state, tenant_id, watermark),
        cut_calvin(state, watermark, deadline),
    );
    groups?;
    calvin?;

    if !state.outcome_floor.await_floor(target, deadline).await {
        return Err(Error::Internal {
            detail: format!(
                "backup: writes minted at or below WAL LSN {} had no final outcome within \
                 {}s, so the backup cannot take a consistent cut (outcome floor at {}). \
                 Retry the backup. A write window held until restart keeps the floor \
                 below it: restart the node if the floor does not move",
                target.as_u64(),
                timeout.as_secs(),
                state.outcome_floor.floor().as_u64(),
            ),
        });
    }
    Ok(())
}

/// Propose a cut barrier carrying `watermark` into every data group this node
/// hosts, and wait for this node's apply of each.
async fn cut_data_groups(
    state: &Arc<SharedState>,
    tenant_id: u64,
    watermark: u64,
) -> Result<(), Error> {
    let Some(proposer) = state.async_raft_proposer() else {
        return Ok(());
    };
    let barriers = futures::future::join_all(barrier_vshards(state).into_iter().map(
        |(group_id, vshard_id)| {
            let entry = ReplicatedEntry::new(
                tenant_id,
                DatabaseId::DEFAULT.as_u64(),
                vshard_id,
                ReplicatedWrite::CutBarrier { hlc: watermark },
            );
            async move {
                propose_replicated_entry(state, proposer, entry)
                    .await
                    .map_err(|error| (group_id, error))
            }
        },
    ))
    .await;
    for barrier in barriers {
        if let Err((group_id, error)) = barrier {
            // A group this node left while the barrier waited holds no
            // replica here to cut: the source node that snapshots it takes
            // its own cut.
            if !hosts_group(state, group_id) {
                tracing::info!(
                    group_id,
                    %error,
                    "backup: this node left the group before its cut barrier applied here; \
                     the group needs no cut on this node"
                );
                continue;
            }
            return Err(Error::Internal {
                detail: format!(
                    "backup: the consistent-cut barrier of raft group {group_id} did not \
                     apply on this node: {error}. Retry the backup"
                ),
            });
        }
    }
    Ok(())
}

/// How long the cut waits for its Calvin marker before it proposes the
/// marker again. A leader change can drop a proposed marker; a second copy is
/// harmless, since a scheduler passes each marker once its earlier
/// transactions finished.
const CUT_MARKER_RETRY: Duration = Duration::from_secs(1);

/// Propose a Calvin cut marker carrying `watermark`, and wait until every
/// Calvin scheduler on this node passed it, or `deadline`.
pub(crate) async fn cut_calvin(
    state: &Arc<SharedState>,
    watermark: u64,
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let cuts = &state.calvin.cuts;
    if cuts.is_empty() {
        // No Calvin scheduler runs here: this node applies no Calvin write.
        return Ok(());
    }
    let proposer = state
        .calvin
        .sequencer_proposer
        .get()
        .ok_or_else(|| Error::Internal {
            detail: "backup: Calvin schedulers run on this node, but no sequencer proposer \
                     is set, so the consistent cut cannot place its marker. Retry the backup \
                     once the cluster finished starting"
                .into(),
        })?;
    let marker = zerompk::to_msgpack_vec(&SequencerEntry::CutMarker { hlc: watermark }).map_err(
        |error| Error::Internal {
            detail: format!("backup: encode the Calvin cut marker: {error}"),
        },
    )?;
    let mut last_refusal = None;
    loop {
        if let Err(error) = proposer.propose(marker.clone()) {
            last_refusal = Some(error.to_string());
        }
        let attempt_deadline = deadline.min(tokio::time::Instant::now() + CUT_MARKER_RETRY);
        let lagging = cuts.await_passed(watermark, attempt_deadline).await;
        if lagging.is_empty() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Internal {
                detail: format!(
                    "backup: the Calvin schedulers of vShards {lagging:?} did not pass the \
                     consistent-cut marker in time, so Calvin transactions sequenced before \
                     the backup may not be installed (last marker refusal: {}). Retry the \
                     backup",
                    last_refusal.as_deref().unwrap_or("none")
                ),
            });
        }
    }
}

/// One vShard per data group this node hosts: the barrier of a group is
/// proposed through any vShard the group homes.
fn barrier_vshards(state: &SharedState) -> BTreeMap<u64, u32> {
    let hosted: Vec<u64> = routed_groups(state)
        .into_iter()
        .filter(|group_id| {
            *group_id != METADATA_GROUP_ID
                && *group_id != SEQUENCER_GROUP_ID
                && hosts_group(state, *group_id)
        })
        .collect();
    let mut vshards = BTreeMap::new();
    for vshard_id in 0..VShardId::COUNT {
        if vshards.len() == hosted.len() {
            break;
        }
        if let Ok(group_id) = group_of_vshard(state, vshard_id)
            && hosted.contains(&group_id)
        {
            vshards.entry(group_id).or_insert(vshard_id);
        }
    }
    vshards
}

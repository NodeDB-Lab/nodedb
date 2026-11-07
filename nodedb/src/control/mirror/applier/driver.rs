// SPDX-License-Identifier: BUSL-1.1

//! One mirror database's applier.
//!
//! It runs on the mirror's metadata leader, and every other node's task waits.
//! Each leadership runs one session:
//!
//! 1. Fence first: a new epoch releases whatever a former applier left held.
//! 2. A mirror still bootstrapping installs its first capture.
//! 3. Tail every source group from the replicated positions.
//! 4. For each cut, commit its entries and each group's seal, stamped with the
//!    epoch, while they hold their lanes.
//! 5. Propose `MirrorCut`, which installs the cut's catalog and raises
//!    `ready_cut` in one index. The held entries then apply.
//! 6. Ack the source, so its pin moves.
//!
//! A session keeps no state a new leader needs: `MirrorCutState` holds it.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::mirror::{CrossClusterLink, MirrorRefusal, MirrorSourceClient};
use nodedb_types::{DatabaseId, MirrorFailure, MirrorStatus, MirrorStatusStep};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::mirror::source::cut_loop::metadata_term;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::session::{FIRST_RETRY_BACKOFF, FenceError, MAX_RETRY_BACKOFF, Session};

/// How long a node that does not lead waits before it looks again.
pub(super) const IDLE_POLL: Duration = Duration::from_millis(250);

/// Why an applier ended.
#[derive(Debug)]
pub enum MirrorStop {
    /// The mirror follows no source: PROMOTE, a drop, or a failure already
    /// recorded in its status.
    Ended,
    /// A source entry or operation stopped the link.
    Failed(MirrorFailure),
    /// The source refused the link for good.
    Refused(MirrorRefusal),
}

/// Why one session ended.
pub(super) enum SessionEnd {
    /// This node no longer leads the metadata group.
    LostLeadership,
    Stop(MirrorStop),
}

impl From<MirrorFailure> for SessionEnd {
    fn from(failure: MirrorFailure) -> Self {
        Self::Stop(MirrorStop::Failed(failure))
    }
}

/// Whether the database tails its source, or takes its first capture when
/// `false`, or `None` when the applier has nothing to do.
pub(super) fn follows(state: &SharedState, database: DatabaseId) -> crate::Result<Option<bool>> {
    let descriptor = state.credentials.catalog().get_database(database)?;
    let Some(origin) = descriptor.and_then(|descriptor| descriptor.mirror_origin) else {
        return Ok(None);
    };
    Ok(match origin.status {
        MirrorStatus::Following | MirrorStatus::Degraded | MirrorStatus::Disconnected => Some(true),
        MirrorStatus::Bootstrapping { .. } => Some(false),
        MirrorStatus::Promoting { .. } | MirrorStatus::Failed { .. } | MirrorStatus::Promoted => {
            None
        }
    })
}

/// Run one database's applier until it ends.
pub async fn run_mirror_applier(
    state: Arc<SharedState>,
    database: DatabaseId,
    client: &MirrorSourceClient,
    link_id: u64,
) -> MirrorStop {
    loop {
        match follows(&state, database) {
            Ok(None) => return MirrorStop::Ended,
            Ok(Some(_)) => {}
            Err(error) => {
                tracing::warn!(database = database.as_u64(), %error, "mirror applier: a catalog read failed");
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            }
        }
        let Some(term) = metadata_term(&state) else {
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        };
        let session = Session::open(&state, database, client, link_id, term);
        match session.run().await {
            SessionEnd::LostLeadership => {}
            SessionEnd::Stop(MirrorStop::Ended) => return MirrorStop::Ended,
            SessionEnd::Stop(stop) => {
                if record_failure(&state, database, client, link_id, term, &stop).await {
                    return stop;
                }
            }
        }
    }
}

/// What an operator sees for a link that stopped.
fn failure_of(stop: &MirrorStop) -> Option<MirrorFailure> {
    match stop {
        MirrorStop::Ended => None,
        MirrorStop::Failed(failure) => Some(failure.clone()),
        MirrorStop::Refused(refusal) => Some(match refusal {
            MirrorRefusal::LogTruncated {
                group,
                first_available,
            } => MirrorFailure::LogTruncated {
                group: *group,
                first_available: *first_available,
            },
            // The source expires a pin when the log it holds grows past a bound.
            MirrorRefusal::PinExpired { .. } => MirrorFailure::LogTruncated {
                group: 0,
                first_available: 0,
            },
            MirrorRefusal::SourceDropped { .. } | MirrorRefusal::DatabaseUnknown { .. } => {
                MirrorFailure::SourceDropped
            }
            MirrorRefusal::BadVersion { .. }
            | MirrorRefusal::WrongCluster { .. }
            | MirrorRefusal::SameCluster
            | MirrorRefusal::DatabaseNotAllowed { .. }
            | MirrorRefusal::DefaultDatabase
            | MirrorRefusal::SourceIsMirror { .. }
            | MirrorRefusal::LinkNotOwned { .. }
            | MirrorRefusal::LinkUnknown { .. }
            | MirrorRefusal::GroupNotTailed { .. }
            | MirrorRefusal::NotHosted { .. }
            | MirrorRefusal::FrameTooLarge { .. }
            | MirrorRefusal::Malformed { .. }
            | MirrorRefusal::Unavailable { .. }
            | MirrorRefusal::UnmaterializedClone { .. }
            | MirrorRefusal::BootstrapFailed { .. }
            | MirrorRefusal::BootstrapRestart { .. } => MirrorFailure::UnsupportedSourceOp {
                detail: refusal.to_string(),
            },
        }),
    }
}

/// Fence, then propose the failure, while this node leads. Answers true once
/// the row shows the failure. With no leadership, a later leader's session
/// meets the failure itself.
///
/// The fence comes first: an entry this applier left above `ready_cut` holds its
/// lane until a verdict skips it, and no later cut comes to raise it. A failure
/// is reported once per occurrence through the capture's grouping key.
async fn record_failure(
    state: &SharedState,
    database: DatabaseId,
    client: &MirrorSourceClient,
    link_id: u64,
    term: u64,
    stop: &MirrorStop,
) -> bool {
    let Some(failure) = failure_of(stop) else {
        return true;
    };
    crate::diag::mirror_link_stopped(database.as_u64(), &failure);
    let mut fence = Session::open(state, database, client, link_id, term);
    let mut backoff = FIRST_RETRY_BACKOFF;
    loop {
        if !fence.still_leads() {
            return false;
        }
        let fenced = fence.fence().await;
        if let Err(FenceError::Lost) = fenced {
            return false;
        }
        let step = CatalogEntry::StepMirrorStatus {
            database_id: database.as_u64(),
            step: MirrorStatusStep::Fail {
                failure: failure.clone(),
            },
        };
        match propose_catalog_entry_async(state, &step).await {
            Ok(_) if fenced.is_ok() => {
                if matches!(follows(state, database), Ok(None)) {
                    return true;
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    database = database.as_u64(),
                    %error,
                    "mirror applier: the failure did not land; it is proposed again"
                );
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
    }
}

/// Give back the source's pin when this mirror follows nothing: it was promoted
/// or dropped, and no later fetch needs the log. Only the metadata leader asks.
/// The source's pin expiry covers an ask that does not arrive.
async fn release_ended_link(
    state: &SharedState,
    database: DatabaseId,
    client: &MirrorSourceClient,
    link_id: u64,
) {
    let follows_nothing = match state.credentials.catalog().get_database(database) {
        Ok(Some(descriptor)) => descriptor
            .mirror_origin
            .is_none_or(|origin| matches!(origin.status, MirrorStatus::Promoted)),
        Ok(None) => true,
        Err(_) => false,
    };
    if !follows_nothing || metadata_term(state).is_none() {
        return;
    }
    if let Err(error) = client.release(link_id).await {
        tracing::info!(
            database = database.as_u64(),
            %error,
            "mirror applier: the source's pin stays until it expires"
        );
    }
}

/// Start one database's applier on this node. Each node runs one, and only the
/// metadata leader's acts.
pub fn spawn_mirror_applier(
    shared: &Arc<SharedState>,
    database: DatabaseId,
    link: Arc<CrossClusterLink>,
) {
    shared.mirror.links.insert(database, Arc::clone(&link));
    let state = Arc::clone(shared);
    let link_id = link.link_id();
    let client = MirrorSourceClient::new(link);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_applier",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            tokio::select! {
                _ = shutdown.wait_cancelled() => {}
                stop = run_mirror_applier(Arc::clone(&state), database, &client, link_id) => {
                    match stop {
                        MirrorStop::Ended => tracing::info!(
                            database = database.as_u64(),
                            "mirror applier ended"
                        ),
                        MirrorStop::Failed(_) | MirrorStop::Refused(_) => tracing::warn!(
                            database = database.as_u64(),
                            ?stop,
                            "mirror applier stopped on a failure"
                        ),
                    }
                    release_ended_link(&state, database, &client, link_id).await;
                    // The supervisor starts this database again from the catalog,
                    // once it shows an applier has work.
                    state.mirror.links.teardown_link(database);
                }
            }
        },
    );
}

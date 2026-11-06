// SPDX-License-Identifier: BUSL-1.1

//! One mirror database's applier.
//!
//! It runs on the mirror's metadata leader, and every other node's task waits.
//! Each leadership runs one session:
//!
//! 1. Fence first: a new epoch releases whatever a former applier left held.
//! 2. Tail every source group from the replicated positions.
//! 3. For each cut, commit its entries and each group's seal, stamped with the
//!    epoch, while they hold their lanes.
//! 4. Propose `MirrorCut`, which installs the cut's catalog and raises
//!    `ready_cut` in one index. The held entries then apply.
//! 5. Ack the source, so its pin moves.
//!
//! A session keeps no state a new leader needs: `MirrorCutState` holds it.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::mirror::{CrossClusterLink, MirrorRefusal, MirrorSourceClient};
use nodedb_types::{DatabaseId, MirrorFailure, MirrorStatus};

use crate::control::mirror::source::cut_loop::metadata_term;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::session::Session;

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

/// Whether the database follows its source now, or `None` when the applier has
/// nothing to wait for.
pub(super) fn follows(state: &SharedState, database: DatabaseId) -> crate::Result<Option<bool>> {
    let descriptor = state.credentials.catalog().get_database(database)?;
    let Some(origin) = descriptor.and_then(|descriptor| descriptor.mirror_origin) else {
        return Ok(None);
    };
    Ok(match origin.status {
        MirrorStatus::Following | MirrorStatus::Degraded | MirrorStatus::Disconnected => Some(true),
        // A bootstrap's end moves the status, and the tail starts at its positions.
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
    client: MirrorSourceClient,
    link_id: u64,
) -> MirrorStop {
    loop {
        match follows(&state, database) {
            Ok(None) => return MirrorStop::Ended,
            Ok(Some(true)) => {}
            Ok(Some(false)) => {
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            }
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
        let session = Session::open(&state, database, &client, link_id, term);
        match session.run().await {
            SessionEnd::LostLeadership => {}
            SessionEnd::Stop(stop) => return stop,
        }
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
                stop = run_mirror_applier(state, database, client, link_id) => {
                    tracing::warn!(
                        database = database.as_u64(),
                        ?stop,
                        "mirror applier stopped"
                    );
                }
            }
        },
    );
}

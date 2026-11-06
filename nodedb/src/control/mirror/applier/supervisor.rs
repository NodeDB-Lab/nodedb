// SPDX-License-Identifier: BUSL-1.1

//! Start one applier per mirror database on this node, from the catalog.
//!
//! The catalog decides: a restart and a new `MIRROR DATABASE` take one path, on
//! every node. An applier that stopped on a failure stays stopped until an
//! operator acts, so the supervisor starts each database once per drop. A
//! database that leaves the catalog clears its place.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::DatabaseId;

use crate::control::mirror::restart::{MirrorRestartDecision, enumerate_resumable_mirrors};
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::driver::spawn_mirror_applier;
use super::link::open_source_link;

/// How often each node looks for a mirror with no applier.
const SUPERVISOR_INTERVAL: Duration = Duration::from_secs(1);

/// What this node already did about each mirror.
#[derive(Debug, Default)]
struct Supervised {
    /// The databases with an applier on this node.
    started: HashSet<DatabaseId>,
    /// The databases whose link did not open, already logged.
    reported: HashSet<DatabaseId>,
}

impl Supervised {
    /// Forget every database `decisions` no longer holds, and answer the
    /// decisions with no applier yet.
    fn pending<'a>(
        &mut self,
        decisions: &'a [MirrorRestartDecision],
    ) -> Vec<&'a MirrorRestartDecision> {
        let held: HashSet<DatabaseId> = decisions.iter().map(|d| d.database).collect();
        self.started.retain(|database| held.contains(database));
        self.reported.retain(|database| held.contains(database));
        decisions
            .iter()
            .filter(|d| d.wants_applier() && !self.started.contains(&d.database))
            .collect()
    }
}

/// One pass: start an applier for each mirror that has none. A link that does
/// not open retries on the next pass, and logs once.
fn supervise(shared: &Arc<SharedState>, supervised: &mut Supervised) {
    if shared.mirror.cluster_id.get().is_none() || shared.mirror.raft.get().is_none() {
        return;
    }
    let decisions = match enumerate_resumable_mirrors(shared.credentials.catalog()) {
        Ok(decisions) => decisions,
        Err(error) => {
            tracing::warn!(%error, "mirror supervisor: the catalog did not read");
            return;
        }
    };
    for decision in supervised.pending(&decisions) {
        match open_source_link(
            shared,
            &decision.source_cluster,
            &decision.source_database_name,
        ) {
            Ok(link) => {
                tracing::info!(
                    database = %decision.database_name,
                    ready_cut = decision.ready_cut,
                    needs_bootstrap = decision.needs_bootstrap,
                    status = ?decision.status,
                    "mirror supervisor: starting the applier"
                );
                spawn_mirror_applier(shared, decision.database, link);
                supervised.started.insert(decision.database);
                supervised.reported.remove(&decision.database);
            }
            Err(error) => {
                if supervised.reported.insert(decision.database) {
                    tracing::warn!(
                        database = %decision.database_name,
                        source = %decision.source_cluster,
                        %error,
                        "mirror supervisor: the source link did not open"
                    );
                }
            }
        }
    }
}

/// Run the supervisor on this node until shutdown.
pub fn spawn_mirror_supervisor(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_supervisor",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut supervised = Supervised::default();
            let mut tick = tokio::time::interval(SUPERVISOR_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                supervise(&state, &mut supervised);
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use nodedb_types::{MirrorFailure, MirrorStatus};

    use super::*;

    fn decision(id: u64, status: MirrorStatus) -> MirrorRestartDecision {
        MirrorRestartDecision {
            database: DatabaseId::new(id),
            database_name: format!("db{id}"),
            source_cluster: "source".to_string(),
            source_database_name: format!("db{id}"),
            status,
            ready_cut: 0,
            positions: Vec::new(),
            needs_bootstrap: true,
        }
    }

    fn pending_ids(supervised: &mut Supervised, decisions: &[MirrorRestartDecision]) -> Vec<u64> {
        supervised
            .pending(decisions)
            .iter()
            .map(|d| d.database.as_u64())
            .collect()
    }

    #[test]
    fn a_started_mirror_is_not_started_again() {
        let mut supervised = Supervised::default();
        let decisions = [decision(1, MirrorStatus::Following)];
        assert_eq!(pending_ids(&mut supervised, &decisions), vec![1]);
        supervised.started.insert(DatabaseId::new(1));
        assert!(pending_ids(&mut supervised, &decisions).is_empty());
    }

    #[test]
    fn a_failed_mirror_gets_no_applier() {
        let mut supervised = Supervised::default();
        let decisions = [decision(
            2,
            MirrorStatus::Failed {
                failure: MirrorFailure::SourceDropped,
            },
        )];
        assert!(pending_ids(&mut supervised, &decisions).is_empty());
    }

    /// A drop clears the place, so a mirror created under the id again starts.
    #[test]
    fn a_dropped_mirror_clears_its_place() {
        let mut supervised = Supervised::default();
        let decisions = [decision(3, MirrorStatus::Following)];
        supervised.started.insert(DatabaseId::new(3));
        assert!(pending_ids(&mut supervised, &[]).is_empty());
        assert_eq!(pending_ids(&mut supervised, &decisions), vec![3]);
    }
}

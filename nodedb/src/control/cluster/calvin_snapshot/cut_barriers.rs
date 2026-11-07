// SPDX-License-Identifier: BUSL-1.1

//! The backup cut barriers a data-group snapshot carries.
//!
//! A cut barrier applied in a group is replicated state, yet a replica learns
//! of it only from the entry. A receiver never applies the entries a snapshot
//! covers, so the snapshot carries two things for them:
//!
//! - every barrier of a cut whose window is open, with its log index. A cut
//!   waiting on the receiver and a scheduler holding redo for the cut read it
//!   as applied.
//! - the group's durable cut floors. Every later entry records a commit HLC
//!   above them, on the receiver as on the builder.

use crate::Error;
use crate::control::backup::cut_order::{CutWindow, OrderedCut};
use crate::control::pitr::restore_point::{group_cut_floors, install_cut_floors};
use crate::control::state::SharedState;
use crate::types::{CoveredCutBarrier, CutFloor};

/// The cut barriers of `group_id` a snapshot through `cut_index` carries.
/// The caller holds the group's apply gate exclusive, so no barrier of the
/// group applies meanwhile, and every applied one sits at or below the cut.
pub fn capture_cut_barriers(
    shared: &SharedState,
    group_id: u64,
    cut_index: u64,
) -> Result<(Vec<CoveredCutBarrier>, Vec<CutFloor>), Error> {
    // no-determinism: the window bounds only which cuts still wait anywhere.
    let now = shared.hlc_clock.now().wall_ns;
    let barriers = shared
        .calvin
        .cut_barriers
        .applied_in_group(group_id)
        .into_iter()
        .filter(|(cut, log_index)| {
            *log_index <= cut_index && now <= CutWindow::on(shared, cut.hlc).hold_until
        })
        .map(|(cut, log_index)| cut.covered_at(log_index))
        .collect();
    let floors = group_cut_floors(shared, group_id, cut_index)?;
    Ok((barriers, floors))
}

/// Install the cut barriers a snapshot of `group_id` carried: the floors
/// first, durably, then each barrier as applied here, which wakes every
/// cut wait and every scheduler holding redo for it.
pub fn install_cut_barriers(
    shared: &SharedState,
    group_id: u64,
    barriers: &[CoveredCutBarrier],
    floors: &[CutFloor],
) -> crate::Result<()> {
    install_cut_floors(shared, group_id, floors)?;
    for barrier in barriers {
        shared.calvin.cut_barriers.note_applied(
            &OrderedCut::from_covered(barrier),
            group_id,
            barrier.log_index,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use nodedb_cluster::RoutingTable;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::backup::cut_order::wait::await_group_barriers;
    use crate::control::cluster::calvin_snapshot::{capture_calvin_cut, install_calvin_cut};
    use crate::wal::WalManager;

    const NODE: u64 = 1;

    /// A fresh node that hosts every group of its routing table, and one
    /// of those groups.
    fn node() -> (Arc<SharedState>, u64, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 8);
        let mut state = SharedState::new(dispatcher, wal).unwrap();
        let routing = RoutingTable::uniform(1, &[NODE], 1);
        let group_id = routing.group_ids()[0];
        let shared = Arc::get_mut(&mut state).unwrap();
        shared.node_id = NODE;
        shared.cluster_routing = Some(Arc::new(RwLock::new(routing)));
        (state, group_id, dir)
    }

    fn open_cut(state: &SharedState) -> OrderedCut {
        OrderedCut {
            hlc: state.hlc_clock.now().wall_ns,
            restore_point: 0,
            capture: None,
            mirror: None,
        }
    }

    #[test]
    fn a_snapshot_carries_the_barriers_at_or_below_its_cut() {
        let (leader, group_id, _dir) = node();
        let cut = open_cut(&leader);
        let closed = OrderedCut {
            hlc: 1,
            ..cut.clone()
        };
        let later = OrderedCut {
            hlc: cut.hlc + 1,
            ..cut.clone()
        };
        leader.calvin.cut_barriers.note_applied(&cut, group_id, 40);
        leader
            .calvin
            .cut_barriers
            .note_applied(&closed, group_id, 30);
        leader
            .calvin
            .cut_barriers
            .note_applied(&later, group_id, 60);
        crate::control::pitr::restore_point::persist_cut_floor(&leader, group_id, 40, cut.hlc)
            .unwrap();
        crate::control::pitr::restore_point::persist_cut_floor(&leader, group_id, 60, later.hlc)
            .unwrap();

        let captured = capture_calvin_cut(&leader, group_id, &HashSet::new(), 7, 50).unwrap();
        assert_eq!(
            captured.cut_barriers,
            [cut.covered_at(40)],
            "a closed window and a barrier above the cut stay behind"
        );
        assert_eq!(
            captured.cut_floors,
            [CutFloor {
                index: 40,
                watermark: cut.hlc
            }]
        );
    }

    #[tokio::test]
    async fn an_installed_barrier_ends_a_pending_cut_wait() {
        let (leader, group_id, _leader_dir) = node();
        let cut = open_cut(&leader);
        leader.calvin.cut_barriers.note_applied(&cut, group_id, 40);
        crate::control::pitr::restore_point::persist_cut_floor(&leader, group_id, 40, cut.hlc)
            .unwrap();
        let captured = capture_calvin_cut(&leader, group_id, &HashSet::new(), 7, 50).unwrap();

        let (follower, follower_group, _follower_dir) = node();
        assert_eq!(follower_group, group_id);
        let waiting = {
            let follower = Arc::clone(&follower);
            let cut = cut.clone();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                await_group_barriers(&follower, &cut, &[group_id], deadline).await
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "no barrier applied on the follower");

        // The install raises the follower's applied index past the snapshot.
        follower.group_watchers.bump(group_id, 50);
        install_calvin_cut(&follower, group_id, &HashSet::new(), captured).unwrap();
        assert_eq!(
            follower.calvin.cut_barriers.applied(cut.key(), group_id),
            Some(40)
        );
        assert_eq!(
            follower.pitr.recorded_cuts().watermark_below(group_id, 51),
            Some(cut.hlc),
            "the apply loop raises later entries above the cut"
        );
        assert_eq!(
            follower
                .credentials
                .catalog()
                .group_cut_floors(group_id)
                .unwrap()
                .len(),
            1,
            "the floor outlives a restart"
        );
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the install wakes the wait")
            .expect("the wait runs")
            .expect("the cut wait succeeds");
    }
}

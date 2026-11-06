// SPDX-License-Identifier: BUSL-1.1

//! The two background passes of a source cluster.
//!
//! The cut pass runs on the metadata group's leader while any link holds a live
//! pin. Each cut places a Calvin marker and one barrier in every data group,
//! ordered as a backup's cut is, so no Calvin transaction straddles it. A mirror
//! batches the log between two cuts. A former leader's cut can still land after
//! a new leader's: its number sits below, and a mirror ignores it. Each barrier
//! alone is a consistent place in its group.
//!
//! The pin pass runs on every node. Retained bytes differ per replica, so each
//! node judges its own. Any node can propose an expiry: the row merges, so two
//! proposals of one expiry land as one.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::{MirrorCutMark, MirrorPin, MirrorPinState};

use crate::control::backup::cut::cut_for_mirror;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::read_cut::statement_cap;
use crate::control::security::auth_fence::cluster::routed_groups;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::pins::{PIN_CEILING, pins_to_expire};

/// The gate between a cut's mark and its barriers.
pub const CUT_BARRIER_FAIL_POINT: &str = "mirror_cut::before_barrier";

/// How often each node judges the pins against its replicas.
const PIN_PASS_INTERVAL: Duration = Duration::from_secs(10);

/// The bits of a cut number that hold the leader's own count.
const SEQ_COUNT_BITS: u32 = 32;

/// Numbers one leader's cuts: `(term << 32) | count`. A later term numbers
/// above every earlier one, with no metadata entry per cut.
#[derive(Debug, Default)]
struct CutNumbers {
    term: u64,
    count: u64,
}

impl CutNumbers {
    fn next(&mut self, term: u64) -> u64 {
        if term != self.term {
            self.term = term;
            self.count = 0;
        }
        self.count += 1;
        (term << SEQ_COUNT_BITS) | (self.count & u64::from(u32::MAX))
    }
}

/// This node's term as the metadata group's leader, while it leads.
pub(crate) fn metadata_term(state: &SharedState) -> Option<u64> {
    state
        .multi_raft
        .get()?
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .leader_term(METADATA_GROUP_ID)
}

/// Take one mirror cut, numbered `seq`.
///
/// The cap comes first: every version at or below it sits before the barrier in
/// every group. The metadata index comes first too: the cut's schema covers
/// what this node applied before any of its entries.
pub async fn take_mirror_cut(state: &SharedState, seq: u64) -> crate::Result<MirrorCutMark> {
    let metadata_index = state.applied_index_watcher(METADATA_GROUP_ID).current();
    let now_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let cap = statement_cap(state, now_ms).await?;
    let mark = MirrorCutMark {
        seq,
        cap_ordinal: cap.cap,
        metadata_index,
        // The mirror's nodes raise their floor to it. A later mint on a promoted
        // mirror then carves above every value this source issued.
        surrogate_hwm: state.surrogate_assigner.current_hwm(),
    };
    // Holds a cut between its mark and its barriers, so a test can write what
    // the mark's metadata index does not cover.
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::before_mirror_cut_barrier(
        state.node_id,
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return Err(crate::Error::Internal {
            detail: "mirror cut: this node shut down before its barriers".to_string(),
        });
    }
    let watermark = state.hlc_clock.now().wall_ns;
    let deadline = StatementDeadline::of_statement(state).tokio_instant();
    cut_for_mirror(state, watermark, mark, deadline).await?;
    Ok(mark)
}

/// Bound every group's compaction by the pins, for each group this node has not
/// bound yet. A group mounted later keeps the bound its id already holds.
pub(crate) fn install_pin_ceilings(state: &SharedState) {
    let Some(multi_raft) = state.multi_raft.get() else {
        return;
    };
    let unbound: Vec<u64> = routed_groups(state)
        .into_iter()
        .chain(std::iter::once(METADATA_GROUP_ID))
        .filter(|group| *group != SEQUENCER_GROUP_ID && !state.mirror.pins.has_ceiling(*group))
        .collect();
    if unbound.is_empty() {
        return;
    }
    let mut multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    for group in unbound {
        multi_raft.set_compaction_ceiling(group, PIN_CEILING, state.mirror.pins.ceiling_for(group));
    }
}

/// Bound each of `groups` by the pins before Raft starts, so no compaction runs
/// past a pin this node just loaded.
pub(crate) fn bind_boot_ceilings(
    state: &SharedState,
    multi_raft: &mut nodedb_cluster::MultiRaft,
    groups: Vec<u64>,
) {
    let groups = groups
        .into_iter()
        .chain(std::iter::once(METADATA_GROUP_ID))
        .filter(|group| *group != SEQUENCER_GROUP_ID);
    for group in groups {
        multi_raft.set_compaction_ceiling(group, PIN_CEILING, state.mirror.pins.ceiling_for(group));
    }
}

/// Run compaction on every group this node hosts, at its durable applied index.
///
/// A group compacts when its floor moves, and that check finds nothing to do
/// while a ceiling holds the log. A ceiling that rises later moves no floor. So
/// a released log stays held until the group's next write, and an idle group has
/// none: the cut pass stops once no pin is live. A log's memory is what a pin's
/// byte bound protects, so an expiry has to free it with no write behind it.
///
/// The compactor applies each group's own holds, and it finds nothing to do
/// under a group's threshold. The sequencer's compaction runs on its own path.
fn compact_released_logs(state: &SharedState) {
    let (Some(multi_raft), Some(compactor)) = (state.multi_raft.get(), state.raft_compactor.get())
    else {
        return;
    };
    let floors: Vec<(u64, u64)> = {
        let multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        multi_raft
            .group_ids()
            .into_iter()
            .filter(|group| *group != SEQUENCER_GROUP_ID)
            .filter_map(|group| {
                multi_raft
                    .log_probe_view(group, None)
                    .map(|view| (group, view.durable_applied))
            })
            .collect()
    };
    for (group, floor) in floors {
        if let Err(error) = compactor(group, floor) {
            tracing::warn!(group, floor, %error, "mirror source: a released log did not compact");
        }
    }
}

/// One pin pass: bind new groups, expire what passed a bound, free what a pin
/// released, and on the metadata leader remove what stopped holding long ago.
pub async fn pin_pass(state: &SharedState) {
    install_pin_ceilings(state);
    compact_released_logs(state);
    let Some(multi_raft) = state.multi_raft.get() else {
        return;
    };
    let statuses = multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .group_statuses();
    let pins = state.mirror.pins.all();
    let now_ms = nodedb_types::wall_now_ns() / 1_000_000;
    let tuning = &state.tuning.mirror;
    for link_id in pins_to_expire(&pins, &statuses, now_ms, tuning) {
        let Some(pin) = pins.iter().find(|pin| pin.link_id == link_id) else {
            continue;
        };
        tracing::warn!(
            link_id,
            database = pin.database,
            client = %pin.client,
            "mirror source: a pin passed its bound; the link must bootstrap again"
        );
        let expired = MirrorPin {
            state: MirrorPinState::Expired,
            ..pin.clone()
        };
        propose(state, CatalogEntry::PutMirrorPin(Box::new(expired))).await;
    }
    if metadata_term(state).is_none() {
        return;
    }
    let keep_ms = tuning.max_pin_age_secs.saturating_mul(2_000);
    for pin in pins.iter().filter(|pin| {
        !matches!(pin.state, MirrorPinState::Live) && now_ms.saturating_sub(pin.acked_ms) > keep_ms
    }) {
        propose(
            state,
            CatalogEntry::DeleteMirrorPin {
                link_id: pin.link_id,
            },
        )
        .await;
    }
}

/// Propose a pin change. The next pass proposes it again after an error.
async fn propose(state: &SharedState, entry: CatalogEntry) {
    if let Err(error) = propose_catalog_entry_async(state, &entry).await {
        tracing::warn!(
            kind = entry.kind(),
            %error,
            "mirror source: a pin change did not commit; the next pass retries it"
        );
    }
}

/// Start the cut pass and the pin pass.
pub fn spawn_mirror_source_loops(shared: &Arc<SharedState>) {
    let cuts = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_cut_loop",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(cuts.tuning.mirror.cut_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut numbers = CutNumbers::default();
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if !cuts.mirror.pins.any_live() {
                    continue;
                }
                let Some(term) = metadata_term(&cuts) else {
                    continue;
                };
                let seq = numbers.next(term);
                if let Err(error) = take_mirror_cut(&cuts, seq).await {
                    tracing::warn!(seq, %error, "mirror source: a cut failed; the next tick cuts");
                }
            }
        },
    );
    let pins = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_pin_loop",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(PIN_PASS_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                pin_pass(&pins).await;
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_term_numbers_above_every_earlier_cut() {
        let mut numbers = CutNumbers::default();
        let first = numbers.next(3);
        let second = numbers.next(3);
        assert!(second > first);
        let new_leader = numbers.next(4);
        assert!(new_leader > second);
        assert_eq!(new_leader, (4 << SEQ_COUNT_BITS) | 1);
    }
}

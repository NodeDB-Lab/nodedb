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
//! - **Calvin transactions.** A Calvin transaction commits at its place in
//!   the sequencer log. The cut proposes a `CutMarker` carrying `W` into the
//!   sequencer log and waits until every Calvin scheduler on this node passed
//!   it: every transaction delivered before the marker installed or dropped.
//!   Every transaction delivered after it records a commit HLC above `W`.
//! - **Raft data groups.** A replicated write carries its proposer's commit
//!   stamp and applies in log order. The cut places a
//!   [`ReplicatedWrite::CutBarrier`] carrying `W` in every data group and
//!   waits for this node's apply of the barrier of each group it hosts.
//!   Every entry before the barrier applied first. Every entry after it
//!   records a commit HLC above `W`, however early its proposer stamped it. A
//!   database backup's barrier also captures its tenants at that log
//!   position (see [`super::cut_capture`]).
//! - **Local write windows.** A write the funnel appends here stamps itself
//!   after its record is minted inside an outcome-floor window. The cut reads
//!   the highest LSN any window minted, after it picks `W`, and waits for the
//!   outcome floor to reach it. A record minted later stamps itself above `W`.
//!
//! A Calvin slice installs from a stamped redo entry its group's leader
//! proposes into the data-group log. The barrier is ordered with the marker
//! (see [`super::cut_order`]): the marker carries the barrier, and each
//! group's leader proposes it once its schedulers passed the marker, while
//! it holds the redo of every later transaction until it applied the
//! barrier. In every group, each slice of a transaction sequenced before the
//! marker sits before the barrier, and each slice of a later one after it,
//! so no transaction is in a capture or a restore point on one group only.
//!
//! Two stamps can share a wall time. Once it picks `W`, the cut moves this
//! node's clock past `W`, so every later stamp here reads above it.
//!
//! The waits bind this node's replicas. A remote source node takes the same
//! cut at `W` itself before it snapshots: the snapshot plan sent to it carries
//! `W`, and its Control Plane runs [`cut_at`] first.
//!
//! [`ReplicatedWrite::CutBarrier`]: crate::control::wal_replication::ReplicatedWrite::CutBarrier

use std::time::Duration;

use nodedb_physical::physical_plan::CutCaptureRequest;
use nodedb_types::Hlc;

use crate::Error;
use crate::control::state::SharedState;

use super::cut_order::OrderedCut;
use super::cut_order::driver::{ensure_driver, hosted_data_groups};
use super::cut_order::marker::{Marker, place_marker};
use super::cut_order::wait::await_group_barriers;

/// Pick the envelope watermark and wait until every write committed below it
/// has a final outcome on this node. Returns the watermark.
pub(super) async fn consistent_cut(state: &SharedState) -> Result<u64, Error> {
    let watermark = state.hlc_clock.now().wall_ns;
    cut_at(state, watermark).await?;
    Ok(watermark)
}

/// Take the consistent cut at `watermark` on this node: wait until every
/// write committed below it has a final outcome here. A remote source node
/// runs it for the watermark the backup's coordinator picked.
pub(crate) async fn cut_at(state: &SharedState, watermark: u64) -> Result<(), Error> {
    cut_at_point(state, watermark, 0).await
}

/// [`cut_at`] for the cluster restore point `restore_point`, `0` for a
/// backup's cut. The barriers and the Calvin marker carry the point's id, so
/// every replica records its group's place at the point.
pub(crate) async fn cut_at_point(
    state: &SharedState,
    watermark: u64,
    restore_point: u64,
) -> Result<(), Error> {
    cut_barriers(
        state,
        OrderedCut {
            hlc: watermark,
            restore_point,
            capture: None,
        },
    )
    .await
}

/// [`cut_at`] whose barriers carry a database backup's capture `request`.
/// The leader of each group this node hosts captures the request's tenants
/// when it applies the request's first barrier in that group.
pub(crate) async fn cut_with_capture(
    state: &SharedState,
    watermark: u64,
    request: &CutCaptureRequest,
) -> Result<(), Error> {
    cut_barriers(
        state,
        OrderedCut {
            hlc: watermark,
            restore_point: 0,
            capture: Some(request.clone()),
        },
    )
    .await
}

async fn cut_barriers(state: &SharedState, cut: OrderedCut) -> Result<(), Error> {
    state
        .hlc_clock
        .update(Hlc::new(cut.hlc.saturating_add(1), 0));
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let deadline = tokio::time::Instant::now() + timeout;
    // Read after the watermark: a record minted after this read stamps its
    // write above the watermark.
    let target = state.outcome_floor.max_noted();

    let groups = hosted_data_groups(state);
    if !groups.is_empty() {
        // This node places the barrier of every group it leads.
        ensure_driver(&state.self_arc()?, &cut);
    }
    let marker = Marker {
        hlc: cut.hlc,
        restore_point: cut.restore_point,
        barrier: Some(&cut),
    };
    let (barriers, calvin) = tokio::join!(
        await_group_barriers(state, &cut, &groups, deadline),
        place_marker(state, marker, &groups, deadline),
    );
    calvin?;
    barriers?;

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

/// Propose a Calvin cut marker carrying `watermark` and no barrier, and wait
/// until every Calvin scheduler on this node passed it, or `deadline`.
pub(crate) async fn cut_calvin(
    state: &SharedState,
    watermark: u64,
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let marker = Marker {
        hlc: watermark,
        restore_point: 0,
        barrier: None,
    };
    place_marker(state, marker, &[], deadline).await
}

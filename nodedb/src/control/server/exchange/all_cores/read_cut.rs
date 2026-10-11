// SPDX-License-Identifier: BUSL-1.1

//! The read cut of a distributed graph run, resolved on this node.
//!
//! Every superstep of a distributed PageRank run, and every node of a WCC
//! round, reads the same graph. The run's first dispatch carries the
//! watermark of a Calvin cut marker. Each node proposes the marker (a second
//! copy is harmless), then waits until:
//!
//! - its sequencer replica applied the marker, recording the highest epoch
//!   instant applied before it (`CalvinCuts::note_instant`), and
//! - every Calvin scheduler it runs passed the marker: every transaction
//!   sequenced before it is installed here.
//!
//! The cut is that instant as a system-time ordinal. Epoch instants rise
//! strictly in log order, so every edge version sequenced before the marker
//! is at or below the cut, and every version sequenced after it is above.
//! Every node applies the same log, so every node resolves the same cut.

use std::time::Duration;

use nodedb_cluster::calvin::SequencerEntry;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

use crate::control::state::SharedState;

/// How long a node waits for the marker before it proposes it again. A
/// leader change can drop a proposed marker.
const MARKER_RETRY: Duration = Duration::from_secs(1);

/// Resolve the read cut of a BSP or WCC `plan` on this node. A plan that
/// carries a cut marker gets its cut in `system_as_of`. Returns the cut the
/// plan's cores read at. A plan with neither a marker nor a cut is an error.
pub(super) async fn resolve_plan_cut(
    state: &SharedState,
    plan: &mut PhysicalPlan,
) -> crate::Result<i64> {
    let (marker, system_as_of) = match plan {
        PhysicalPlan::Graph(GraphOp::BspSuperstep(bsp)) => {
            (&mut bsp.read_cut_marker, &mut bsp.system_as_of)
        }
        PhysicalPlan::Graph(GraphOp::WccSuperstep(wcc)) => {
            (&mut wcc.read_cut_marker, &mut wcc.system_as_of)
        }
        _ => {
            return Err(crate::Error::Internal {
                detail: "graph read cut: only a BSP or WCC superstep carries a read cut".into(),
            });
        }
    };
    if *marker != 0 {
        *system_as_of = Some(resolve_read_cut(state, *marker).await?);
        *marker = 0;
    }
    system_as_of.ok_or_else(|| crate::Error::Internal {
        detail: "graph read cut: a distributed graph superstep arrived with no read cut and no \
                 cut marker"
            .into(),
    })
}

/// Propose the cut marker `marker` and wait until it applied here and every
/// local Calvin scheduler passed it. Returns the cut as a system-time
/// ordinal: `0` when no epoch applied before the marker, so no Calvin edge
/// version is visible.
async fn resolve_read_cut(state: &SharedState, marker: u64) -> crate::Result<i64> {
    let proposer = state
        .calvin
        .sequencer_proposer
        .get()
        .ok_or_else(|| crate::Error::Internal {
            detail: "graph read cut: no sequencer proposer is set on this node, so the run \
                     cannot place its cut marker. Retry once the cluster finished starting"
                .into(),
        })?;
    let entry = zerompk::to_msgpack_vec(&SequencerEntry::CutMarker {
        hlc: marker,
        restore_point: 0,
        barrier: None,
    })
    .map_err(|error| crate::Error::Internal {
        detail: format!("graph read cut: encode the cut marker: {error}"),
    })?;
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_refusal = None;
    loop {
        if let Err(error) = proposer.propose(entry.clone()) {
            last_refusal = Some(error.to_string());
        }
        let attempt = deadline.min(tokio::time::Instant::now() + MARKER_RETRY);
        if let Some(instant) = state.calvin.cuts.await_instant(marker, attempt).await {
            return Ok(instant.map_or(0, nodedb_types::ms_to_ordinal_upper));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(crate::Error::Internal {
                detail: format!(
                    "graph read cut: cut marker {marker} did not apply and pass every Calvin \
                     scheduler of this node within {}s (vShards still before it: {:?}, last \
                     marker refusal: {}). Retry the query",
                    timeout.as_secs(),
                    state.calvin.cuts.lagging(marker),
                    last_refusal.as_deref().unwrap_or("none"),
                ),
            });
        }
    }
}

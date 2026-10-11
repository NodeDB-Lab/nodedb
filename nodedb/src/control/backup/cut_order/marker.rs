// SPDX-License-Identifier: BUSL-1.1

//! A cut's Calvin marker: proposed into the sequencer log until it took.

use std::time::Duration;

use nodedb_cluster::calvin::SequencerEntry;

use crate::Error;
use crate::control::state::SharedState;

use super::ordered_cut::OrderedCut;
use super::wait::await_placed;

/// How long the cut waits for its Calvin marker before it proposes the
/// marker again. A leader change can drop a proposed marker. A second copy
/// is harmless: it names the same cut, and every scheduler keeps the place
/// of the first copy it received.
const CUT_MARKER_RETRY: Duration = Duration::from_secs(1);

/// What one cut marker carries.
#[derive(Clone, Copy)]
pub(crate) struct Marker<'a> {
    pub hlc: u64,
    pub restore_point: u64,
    /// The cut whose barriers the marker orders, `None` for a marker with no
    /// barrier.
    pub barrier: Option<&'a OrderedCut>,
}

/// Propose `marker` into the sequencer log, and wait until every Calvin
/// scheduler on this node passed it, or `deadline`.
///
/// With no scheduler here, nothing here shows that the marker applied. An
/// ordered cut then proposes the marker again until the barrier of every
/// group of `groups` applied here: the groups' leaders place the barriers
/// only once their schedulers passed the marker.
pub(crate) async fn place_marker(
    state: &SharedState,
    marker: Marker<'_>,
    groups: &[u64],
    deadline: tokio::time::Instant,
) -> Result<(), Error> {
    let cuts = &state.calvin.cuts;
    let calvin_here = !cuts.is_empty();
    // With no scheduler here, the barriers of this node's groups show that
    // the marker applied.
    let watched = marker
        .barrier
        .filter(|_| !calvin_here && !groups.is_empty());
    if !calvin_here && watched.is_none() {
        // No Calvin scheduler runs here and no group here waits on the
        // marker: this node applies no Calvin write.
        return Ok(());
    }
    let proposer = state
        .calvin
        .sequencer_proposer
        .get()
        .ok_or_else(|| Error::Internal {
            detail: "backup: this node takes a Calvin cut, but no sequencer proposer is set, so \
                     the consistent cut cannot place its marker. Retry the backup once the \
                     cluster finished starting"
                .into(),
        })?;
    let entry = zerompk::to_msgpack_vec(&SequencerEntry::CutMarker {
        hlc: marker.hlc,
        restore_point: marker.restore_point,
        barrier: marker.barrier.map(OrderedCut::to_wire),
    })
    .map_err(|error| Error::Internal {
        detail: format!("backup: encode the Calvin cut marker: {error}"),
    })?;
    let mut last_refusal = None;
    loop {
        if let Err(error) = proposer.propose(entry.clone()) {
            last_refusal = Some(error.to_string());
        }
        let attempt_deadline = deadline.min(tokio::time::Instant::now() + CUT_MARKER_RETRY);
        let lagging = cuts.await_passed(marker.hlc, attempt_deadline).await;
        if lagging.is_empty() {
            let Some(cut) = watched else {
                return Ok(());
            };
            if await_placed(state, cut.key(), groups, attempt_deadline).await
                || tokio::time::Instant::now() >= deadline
            {
                // A group with no barrier by the deadline fails the cut's
                // barrier wait, which names it.
                return Ok(());
            }
            continue;
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

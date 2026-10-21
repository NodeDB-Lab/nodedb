// SPDX-License-Identifier: BUSL-1.1

//! The cut marker one distributed run (BSP PageRank, WCC) reads at.
//!
//! The coordinator names a Calvin cut marker by a fresh watermark and sends
//! it in the run's first dispatch. Every owner node proposes and waits for
//! the marker, then reads at the epoch instant the marker's sequencer place
//! fixes (`exchange::all_cores::read_cut`). Every node applies the same
//! sequencer log, so every node answers the same cut. The coordinator checks
//! that, and sends the cut in every later dispatch.

use crate::control::state::SharedState;

/// A fresh cut marker watermark for one run, from this node's HLC.
pub(crate) fn new_cut_marker(state: &SharedState) -> u64 {
    state.hlc_clock.now().wall_ns.max(1)
}

/// The one read cut every owner node answered. A node that answered no cut,
/// or a different one, is an error: the run reads two graphs.
pub(crate) fn agreed_cut(
    answers: impl IntoIterator<Item = (u64, Option<i64>)>,
) -> crate::Result<i64> {
    let mut agreed: Option<(u64, i64)> = None;
    for (node_id, cut) in answers {
        let Some(cut) = cut else {
            return Err(crate::Error::Internal {
                detail: format!("graph read cut: node {node_id} answered no read cut"),
            });
        };
        match agreed {
            None => agreed = Some((node_id, cut)),
            Some((first, first_cut)) if first_cut != cut => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "graph read cut: node {first} read at {first_cut} and node {node_id} at \
                         {cut}; a run reads one graph. Retry the query"
                    ),
                });
            }
            Some(_) => {}
        }
    }
    agreed
        .map(|(_, cut)| cut)
        .ok_or_else(|| crate::Error::Internal {
            detail: "graph read cut: no owner node answered".into(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_node_must_answer_the_same_cut() {
        assert_eq!(agreed_cut([(1, Some(5)), (2, Some(5))]).unwrap(), 5);
        assert!(agreed_cut([(1, Some(5)), (2, Some(6))]).is_err());
        assert!(agreed_cut([(1, Some(5)), (2, None)]).is_err());
        assert!(agreed_cut(Vec::new()).is_err());
    }
}

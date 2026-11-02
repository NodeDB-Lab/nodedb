// SPDX-License-Identifier: BUSL-1.1

//! The closed cut a RESTORE takes effect at.
//!
//! A RESTORE re-issues its rows over many Calvin transactions, and client
//! writes run beside them on every path: Calvin, autocommit, replicated or
//! local. The RESTORE takes one cut before its first batch and closes it
//! everywhere a write can stamp:
//!
//! - on this node: the statement cap folds the cut into the ordinal clock
//!   and waits until every write reserved at or below it applied;
//! - on the sequencer: the cap's cut marker carries the cut as its floor, so
//!   every Calvin epoch sequenced later stamps above it;
//! - on the leader of every data group: each leader folds the cut, waits
//!   for its reserved writes, and raises the group's ordinal floor, so every
//!   later leader stamps above it too.
//!
//! Every write at or below the cut applied before any batch resolves. Every
//! write admitted after it stamps above it. A batch's resolve leaves a
//! destination version above the cut current (`RestoredRedo::restore_cut`),
//! so the outcome is the RESTORE applied whole at its cut, then every later
//! write, whichever batch a row falls in.

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

use crate::control::read_cut::close::close_leader_cuts;
use crate::control::read_cut::statement_cap::statement_cap;
use crate::control::security::auth_fence::cluster::routed_groups;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;

/// Take and close the cut a RESTORE takes effect at. Ends at the statement
/// deadline with `Error::DeadlineExceeded`.
pub(super) async fn take_restore_cut(state: &SharedState) -> crate::Result<i64> {
    let now_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let cap = statement_cap(state, now_ms).await?;
    let groups: Vec<u64> = data_groups(routed_groups(state));
    close_leader_cuts(
        state,
        &groups,
        cap.cap,
        StatementDeadline::of_statement(state),
    )
    .await?;
    Ok(cap.cap)
}

/// The data groups among `groups`: every group but the metadata and the
/// sequencer group, which hold no row.
fn data_groups(groups: Vec<u64>) -> Vec<u64> {
    groups
        .into_iter()
        .filter(|group| *group != METADATA_GROUP_ID && *group != SEQUENCER_GROUP_ID)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_data_groups_close_the_cut() {
        assert_eq!(
            data_groups(vec![METADATA_GROUP_ID, 4, SEQUENCER_GROUP_ID, 7]),
            vec![4, 7]
        );
    }
}

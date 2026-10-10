// SPDX-License-Identifier: BUSL-1.1

//! The write version a data-group snapshot install lands on each vShard.

use nodedb_types::{ShardVersion, WriteVersion};

use crate::control::state::SharedState;

/// The version each of `vshards` holds once a snapshot of `group_id` cut at
/// `cut_index` lands: the cut's log position in the vShard's epoch.
///
/// The installed rows carry no per-row versions. Every write the snapshot
/// holds sits at or below the cut, so a read the cut does not cover no longer
/// validates on this node.
pub fn version_floor(
    state: &SharedState,
    group_id: u64,
    vshards: &[u32],
    cut_index: u64,
) -> Vec<ShardVersion> {
    vshards
        .iter()
        .map(|&vshard| {
            let position =
                crate::event::cdc::position::entry_position(state, vshard, group_id, cut_index);
            ShardVersion {
                vshard,
                version: WriteVersion::logged(position.epoch, position.log_index),
            }
        })
        .collect()
}

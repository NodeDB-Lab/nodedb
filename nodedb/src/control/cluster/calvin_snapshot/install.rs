// SPDX-License-Identifier: BUSL-1.1

//! Install the Calvin cut a data-group snapshot carries.
//!
//! The snapshot replaced the storage of the group's vShards with the
//! leader's, which holds exactly the Calvin positions the cut names. So
//! each vShard's applied state here becomes the cut's: in the applied
//! ledger, which a scheduler starts from and a checkpoint saves from, and
//! in the catalog, which boot recovery reads. Its base becomes the cut's
//! sequencer index, which moves its generation: a scheduler started before
//! the install installs nothing more, and the next scheduler reconcile
//! starts it again from the installed state.
//!
//! A WAL `SnapshotInstalled` record of the group, written before this runs,
//! makes boot recovery drop the vShards' applied markers from before it.

use std::collections::{BTreeSet, HashSet};

use crate::control::cluster::snapshot_install::SnapshotInstallError;
use crate::control::security::catalog::calvin_applied::StoredCalvinApplied;
use crate::control::state::SharedState;
use crate::types::GroupCalvinCut;

/// Install `cut` as the Calvin state of every vShard of `group_id` in
/// `group_vshards`. A vShard the cut does not name had no scheduler on the
/// builder: it holds no Calvin position.
pub fn install_calvin_cut(
    shared: &SharedState,
    group_id: u64,
    group_vshards: &HashSet<u32>,
    cut: GroupCalvinCut,
) -> Result<(), SnapshotInstallError> {
    let mut ids: Vec<u32> = group_vshards.iter().copied().collect();
    ids.sort_unstable();
    let states: Vec<StoredCalvinApplied> = ids
        .iter()
        .map(|&vshard_id| {
            let named = cut
                .vshards
                .iter()
                .find(|state| state.vshard_id == vshard_id);
            StoredCalvinApplied {
                vshard_id,
                fully_applied_epoch: named.map_or(
                    crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH,
                    |state| state.fully_applied_epoch,
                ),
                tail: named
                    .map(|state| state.tail.iter().copied().collect())
                    .unwrap_or_else(BTreeSet::new),
            }
        })
        .collect();

    for state in &states {
        shared.calvin.applied.install(
            state.vshard_id,
            state.fully_applied_epoch,
            state.tail.clone(),
        );
    }
    let catalog = shared.credentials.catalog();
    let settle_error = |source| SnapshotInstallError::Settle {
        group_id,
        step: crate::control::cluster::snapshot_install::SettleStep::CalvinState,
        source,
    };
    catalog
        .replace_calvin_applied(&states)
        .map_err(settle_error)?;
    shared
        .calvin
        .bases
        .record_snapshot(catalog, &ids, cut.through)
        .map_err(settle_error)
}

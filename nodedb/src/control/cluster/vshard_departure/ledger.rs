// SPDX-License-Identifier: BUSL-1.1

//! Bring the residency rows in line with the vShards resident here.
//!
//! - A resident vShard with no row gets a hosted row.
//! - A resident vShard with a departed row is hosted again: its pending
//!   purge is cancelled. Its group owes a snapshot install here since it
//!   left, so the data it lost comes back from that install.
//! - A hosted row whose vShard is not resident departs: its purge is owed.

use std::collections::BTreeMap;

use crate::control::security::catalog::vshard_residency::{ResidencyChange, VShardResidency};

use super::resident::ResidencyView;

/// The row changes one reconcile makes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LedgerPlan {
    pub changes: Vec<ResidencyChange>,
    /// vShards whose hosted row departs.
    pub departed: Vec<u32>,
    /// vShards whose departed row is hosted again.
    pub rehosted: Vec<u32>,
}

/// The changes that bring `rows` in line with `residency`.
pub fn reconcile(rows: &BTreeMap<u32, VShardResidency>, residency: &ResidencyView) -> LedgerPlan {
    let mut plan = LedgerPlan::default();
    for &vshard_id in &residency.resident {
        match rows.get(&vshard_id) {
            None => plan
                .changes
                .push((vshard_id, Some(VShardResidency::HOSTED))),
            Some(row) if row.departed => {
                plan.changes
                    .push((vshard_id, Some(VShardResidency::HOSTED)));
                plan.rehosted.push(vshard_id);
            }
            Some(_) => {}
        }
    }
    for (&vshard_id, row) in rows {
        if !row.departed && !residency.holds(vshard_id) {
            plan.changes
                .push((vshard_id, Some(VShardResidency::DEPARTED)));
            plan.departed.push(vshard_id);
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn residency(resident: &[u32]) -> ResidencyView {
        ResidencyView {
            resident: resident.iter().copied().collect::<BTreeSet<u32>>(),
            group_of: BTreeMap::new(),
        }
    }

    #[test]
    fn a_hosted_vshard_that_left_departs_and_a_returning_one_is_hosted() {
        let purged = VShardResidency {
            departed: true,
            barrier_lsn: 7,
            purged: true,
        };
        let rows = BTreeMap::from([
            (1, VShardResidency::HOSTED),
            (2, VShardResidency::HOSTED),
            (3, purged),
        ]);
        let plan = reconcile(&rows, &residency(&[1, 3, 4]));
        assert_eq!(plan.departed, vec![2]);
        assert_eq!(plan.rehosted, vec![3]);
        let mut changes = plan.changes.clone();
        changes.sort_by_key(|(vshard_id, _)| *vshard_id);
        assert_eq!(
            changes,
            vec![
                (2, Some(VShardResidency::DEPARTED)),
                (3, Some(VShardResidency::HOSTED)),
                (4, Some(VShardResidency::HOSTED)),
            ]
        );
    }

    #[test]
    fn a_settled_ledger_changes_nothing() {
        let rows = BTreeMap::from([(1, VShardResidency::HOSTED), (2, VShardResidency::DEPARTED)]);
        assert_eq!(reconcile(&rows, &residency(&[1])), LedgerPlan::default());
    }
}

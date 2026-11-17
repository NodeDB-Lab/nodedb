// SPDX-License-Identifier: Apache-2.0

//! Where a committed redo record comes from, and so which checks its apply
//! runs.

/// The source of a redo record a replica installs.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoOrigin {
    /// A transaction commit. The apply runs every commit-boundary check:
    /// BALANCED, UNIQUE, and the stateless PUT and DELETE rules.
    Commit,
    /// A RESTORE re-installing rows a backup captured. Each row passed its
    /// collection's rules when it was first written, and a bitemporal row's
    /// earlier versions are history, not new writes. The apply checks only
    /// that every unique value has one owner in the post-state.
    Restore,
}

impl RedoOrigin {
    /// The origin of the redo record a slice of `plans` resolves to: a
    /// RESTORE when a plan installs a RESTORE batch, a commit otherwise.
    /// The stage that checks the slice and the install that applies its
    /// record both read it here.
    pub fn of_plans(plans: &[super::PhysicalPlan]) -> Self {
        let restores = plans.iter().any(|plan| {
            matches!(
                plan,
                super::PhysicalPlan::Meta(super::MetaOp::RestoreRedo(_))
            )
        });
        if restores {
            Self::Restore
        } else {
            Self::Commit
        }
    }
}

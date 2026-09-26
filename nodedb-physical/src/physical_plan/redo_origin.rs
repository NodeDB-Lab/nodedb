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

// SPDX-License-Identifier: BUSL-1.1

//! What applying one catalog entry did.

use crate::control::security::role_assignment::RoleRefusal;

/// The result of applying a committed catalog entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The entry was written. Its post-apply side effects run.
    Applied,
    /// The entry wrote nothing and still concludes its DDL, such as an
    /// if-absent create for a descriptor that exists. No side effects run.
    Unchanged,
    /// The entry breaks a role rule at its log position, and is skipped.
    /// Every node applies the same log in the same order, so every node
    /// skips it alike. No side effects run.
    Refused(RoleRefusal),
}

impl ApplyOutcome {
    /// Whether the entry was written, so its side effects must run.
    pub fn wrote(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

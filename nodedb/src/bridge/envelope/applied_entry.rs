// SPDX-License-Identifier: BUSL-1.1

//! The replicated data-group entry a write applies.

use nodedb_types::WriteVersion;

/// What every replica that applies one data-group entry shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedEntry {
    /// The entry's log position. The Data Plane records it as the version of
    /// every row the write touches.
    pub version: WriteVersion,
    /// The system-time ordinal the entry's leader assigned: the stamp of
    /// every bitemporal version the write lands, or the first ordinal of the
    /// run a columnar insert stamps. `None` for an entry no leader stamped.
    pub system_ordinal: Option<i64>,
}

impl AppliedEntry {
    /// The entry at `log_index` of the group the vShard moved to at `epoch`,
    /// stamped with `system_ordinal`.
    pub const fn logged(epoch: u64, log_index: u64, system_ordinal: Option<i64>) -> Self {
        Self {
            version: WriteVersion::logged(epoch, log_index),
            system_ordinal,
        }
    }

    /// An entry at `version` that carries no system-time ordinal.
    pub const fn unstamped(version: WriteVersion) -> Self {
        Self {
            version,
            system_ordinal: None,
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The unit a restored row or edge re-issues as.

use crate::control::surrogate::CarriedIdentity;
use crate::types::DatabaseId;
use crate::wal::RedoSubRecord;

/// One restored row or edge: its sub-records in apply order, and the
/// identities every replica binds before it installs them. A unit never
/// splits across two redo records.
pub(super) struct RowUnit {
    pub ops: Vec<RedoSubRecord>,
    pub identities: Vec<CarriedIdentity>,
}

impl RowUnit {
    /// The unit's encoded size, for sizing the records it goes into.
    pub(super) fn byte_len(&self) -> usize {
        self.ops.iter().map(|op| op.payload.len()).sum::<usize>()
            + self
                .identities
                .iter()
                .map(|identity| identity.collection.len() + identity.pk_bytes.len())
                .sum::<usize>()
    }
}

/// Every unit of one collection. All of them write that collection's vShard.
pub(super) struct CollectionUnits {
    pub database_id: DatabaseId,
    pub collection: String,
    pub units: Vec<RowUnit>,
}

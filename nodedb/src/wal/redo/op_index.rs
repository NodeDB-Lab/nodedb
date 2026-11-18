// SPDX-License-Identifier: BUSL-1.1

//! The op index of each record in a merged replay stream.
//!
//! A sub-record reconstituted from a `TransactionRedo` record carries its
//! index among that record's ops. A continuation of a split record carries
//! the record's later ops, so its sub-records carry their index in the
//! whole record. Every other record carries none: a standalone record, the
//! enclosing redo record itself, and a part journalled after apply. The
//! fold ledger counts a record's ops by this index.

/// Op indexes parallel to a merged replay slice.
///
/// An index shorter than its slice holds no op for the positions past its
/// end. The empty index fits a slice with no reconstituted sub-record.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RedoOpIndex {
    ops: Vec<Option<u64>>,
}

impl RedoOpIndex {
    pub(crate) fn new(ops: Vec<Option<u64>>) -> Self {
        Self { ops }
    }

    /// The op index of the record at `position` in the slice.
    pub(crate) fn of(&self, position: usize) -> Option<u64> {
        self.ops.get(position).copied().flatten()
    }
}

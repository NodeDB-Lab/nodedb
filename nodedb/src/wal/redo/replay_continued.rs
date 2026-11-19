// SPDX-License-Identifier: BUSL-1.1

//! The continuations of a split committed transaction record, as restart
//! replay reconstitutes them.

use std::collections::HashMap;

/// The op count of a `TransactionRedo` record its continuations extend.
pub(super) struct ContinuedOps {
    /// The record op index of the next continuation's first sub-record.
    pub(super) next_op: u64,
    /// The index (from 1) of the record's next continuation.
    pub(super) next_continuation: u32,
}

/// The record op index of the first sub-record of the `WriteGroup` record
/// `group`, which holds `len` sub-records. `None` unless it is a
/// continuation whose origin record this read holds: a part journalled
/// after apply is no op of the record, and a continuation without its
/// origin replays no fold.
///
/// A continuation out of order is refused: its ops would apply in another
/// order than the live install applied them.
pub(super) fn continuation_first_op(
    continued: &mut HashMap<u64, ContinuedOps>,
    group: &super::WriteGroup,
    len: usize,
) -> crate::Result<Option<u64>> {
    let is_continuation = group.opens() && group.origin != 0 && group.parts > 0;
    if !is_continuation {
        return Ok(None);
    }
    let Some(ops) = continued.get_mut(&group.origin) else {
        return Ok(None);
    };
    if group.parts != ops.next_continuation {
        return Err(crate::Error::Internal {
            detail: format!(
                "continuation {} of the transaction record at lsn {} follows continuation {}",
                group.parts,
                group.origin,
                ops.next_continuation.saturating_sub(1)
            ),
        });
    }
    let first = ops.next_op;
    ops.next_op = first.saturating_add(len as u64);
    ops.next_continuation = ops.next_continuation.saturating_add(1);
    Ok(Some(first))
}

/// The origin LSN of the record whose last continuation `group` is. `Some`
/// only for the last continuation of a record whole at append, whose
/// continuation count it names.
pub(super) fn last_continuation_of(group: &super::WriteGroup) -> Option<u64> {
    group
        .closed_continuation()
        .filter(|(index, count)| index == count)
        .map(|_| group.origin)
}

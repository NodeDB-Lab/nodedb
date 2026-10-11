// SPDX-License-Identifier: BUSL-1.1

//! Fold the dependent-read barrier entries of a waiting lane ahead of their
//! turn.
//!
//! A group's entries start in log order. A write held for this node's
//! metadata apply (see [`super::metadata_floor`]) keeps every later entry of
//! its group in the backlog until the catalog catches up. A barrier entry
//! names no collection and needs no catalog. Behind such a hold it would
//! still wait, and the txn's barrier on this replica would lack an event its
//! peers folded.
//!
//! So each pump folds every barrier entry the backlog holds past the last
//! one it folded, in log order. Barrier entries fold in log order among
//! themselves, which is the only order a barrier log depends on. Each entry
//! still concludes at its turn (see
//! [`super::calvin_read_result::prefold_barrier_entry`]).

use super::calvin_read_result::prefold_barrier_entry;
use super::context::ApplyContext;
use super::lane::Lane;

/// Fold each barrier entry of `lane`'s backlog above
/// `lane.prefolded_through`, in log order, and move the mark past every
/// backlog entry.
pub(super) fn prefold_backlog(ctx: ApplyContext<'_>, lane: &mut Lane, group_id: u64) {
    let through = lane.prefolded_through;
    let mut last = through;
    for queued in lane.backlog.iter() {
        let index = queued.entry.index;
        if index <= through {
            continue;
        }
        last = last.max(index);
        if let Some(decoded) = queued.decoded.as_ref() {
            prefold_barrier_entry(ctx, group_id, index, decoded);
        }
    }
    lane.prefolded_through = last;
}

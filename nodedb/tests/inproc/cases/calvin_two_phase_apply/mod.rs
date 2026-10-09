// SPDX-License-Identifier: BUSL-1.1

//! Staged Calvin apply on the Data Plane: `MetaOp::CalvinExecuteStatic`
//! VALIDATES + STAGES a transaction's write plans into the commit-pending
//! buffer WITHOUT mutating base, returning the local commit vote on
//! `stage_vote`. `MetaOp::CalvinResolve` then resolves the staged plans
//! into the transaction's redo record. The data group's apply of the slice's
//! stamped redo entry (`MetaOp::ApplyTransactionRedo` with a `CalvinInstall`)
//! installs it at its LSN (making the write visible), or `MetaOp::CalvinDrop`
//! discards the staged state (leaving base unchanged).
//!
//! These drive a `CoreLoop` directly through the SPSC ring so the atomicity
//! seam is observed without any scheduler timing: nothing a stage writes is
//! visible until the install, and a drop never makes it visible.

mod install;
mod phantom;
mod read_set;
mod support;

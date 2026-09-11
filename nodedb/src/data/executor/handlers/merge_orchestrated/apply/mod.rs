// SPDX-License-Identifier: BUSL-1.1

//! MERGE APPLY pass: verify the resolve→apply prediction, then atomically
//! apply every arm's writes with the Control-Plane-pre-assigned surrogates.
//!
//! This directory owns the drift verification and the single redb write
//! transaction the UPDATE and INSERT arms share. The two things that cannot
//! live under that transaction have their own sibling files: unwinding a
//! partial apply (`abort`) and the DELETE arms, whose cascade opens
//! transactions of its own and therefore runs after the commit
//! (`delete_arms`). `orchestrate` owns the setup and the commit, and calls
//! `update_rows` and `insert_rows` for the two arms that share the
//! transaction.

mod insert_rows;
mod orchestrate;
mod update_rows;

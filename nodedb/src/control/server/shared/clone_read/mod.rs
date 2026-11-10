// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral clone CoW read-path interception — the read-side twin of
//! `clone_write`. For a `Shadowed`/`Materializing` clone, merges target rows
//! with tombstone-filtered source rows into one `Response`, so every dispatch
//! entry point through [`super::clone_write::intercept_and_authorize`] reads
//! through an unmaterialized clone correctly, not only pgwire.

mod dispatch;
mod entry;
mod merge;
mod set_op;
mod shadow;
mod temporal;

pub(in crate::control::server) use entry::{
    CloneReadAnswer, CloneReadInterceptParams, CloneReadOutcome, maybe_intercept_clone_read,
};

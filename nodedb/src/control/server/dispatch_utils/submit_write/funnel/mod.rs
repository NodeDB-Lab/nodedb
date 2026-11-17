// SPDX-License-Identifier: BUSL-1.1

//! THE single Control-Plane write funnel.
//!
//! Every Control-Plane path that puts a write on the SPSC bridge routes through
//! [`submit_write`]: the autocommit / internal funnel
//! ([`crate::control::server::dispatch_utils::dispatch`]), the pgwire
//! local-dispatch path (`pgwire::handler::submit`), and the Raft apply loop
//! (`distributed_applier::apply_loop`). The funnel owns write admission, the
//! WAL redo append, the enqueue, the bounded response collect, the post-apply
//! redo, the durable-at-ack barrier, and — for the caller that owns it (see
//! [`super::params::ChangeFeedOwner`]) — the CDC publish, in that order, which
//! is the correctness contract.
//!
//! A path that reimplements these steps drifts silently: it is not a compile
//! error to omit the redo append or the change-event publish, and the omission
//! only surfaces as lost data after a crash, or as a change stream that never
//! fires. Add the step here, once, and every caller gets it.
//!
//! It also owns the mirror of the redo append: every record of the write, the
//! ones it appended and the ones a caller appended under their outcome-floor
//! window, is cancelled when the Data Plane refuses the write with a verdict
//! that proves nothing applied. See
//! [`crate::control::server::dispatch_utils::write_abort`] for which verdicts
//! qualify, and the `minted` module for how each path closes the window.
//!
//! Split by concern, run in this fixed order by [`driver::submit_write`]:
//! - [`admission`]: the write-admission gate.
//! - [`changes`]: the change set a write owes its change feed.
//! - [`wal_append`]: the array DDL refusal and the WAL redo append/stamp.
//! - [`dispatch`]: building the wire `Request` and handing it to the Data
//!   Plane.
//! - [`answer`]: where the response phase reads the answer.
//! - [`response`]: collecting the response, classifying the outcome, and the
//!   post-apply steps a successful write still owes.
//! - [`pending`]: the write between its enqueue and its response phase.

mod admission;
mod answer;
mod changes;
mod dispatch;
mod driver;
mod late_parts;
mod pending;
mod response;
mod wal_append;

pub(crate) use dispatch::dispatch_when_capacity_frees;
pub(crate) use driver::enqueue_write;
pub(crate) use pending::{PendingWrite, submit_write};

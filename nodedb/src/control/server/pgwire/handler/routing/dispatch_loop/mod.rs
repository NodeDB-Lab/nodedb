// SPDX-License-Identifier: BUSL-1.1

//! The per-task dispatch loop for non-Calvin pgwire queries.

mod finish;
mod run;
mod task;

pub(crate) use run::DispatchTaskContext;

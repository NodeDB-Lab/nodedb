// SPDX-License-Identifier: BUSL-1.1

//! One planned SQL statement, run the same way by every protocol: its atomic
//! routes, its per-task loop, the single-task dispatch primitive, and the
//! fold of its answer. Each protocol renders the answer in its own shape.

pub mod answer;
pub mod atomic;
pub mod dispatch;
pub mod edge_recon;
pub mod error;
pub mod task_loop;

pub(crate) use answer::StatementAnswer;
pub(crate) use atomic::{AtomicRoute, AtomicStatement, calvin_answer, route_atomic_statement};
pub(crate) use dispatch::authorize_one_task;
pub(crate) use edge_recon::{EdgeRecon, try_edge_recon};
pub(crate) use error::StatementError;
pub(crate) use task_loop::{
    PlannedStatement, StatementExec, run_implicit_statement, run_statement_loop,
};

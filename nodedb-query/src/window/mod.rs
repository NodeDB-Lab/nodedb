// SPDX-License-Identifier: Apache-2.0

//! Window function specification and evaluation.
//!
//! Evaluated after sort, before projection. Each spec produces a new column
//! appended to every row (e.g., ROW_NUMBER, RANK, SUM OVER).

pub mod aggregate;
pub mod arg;
pub mod eval;
pub mod frame;
pub mod helpers;
pub mod offset;
pub mod ranking;
pub mod running;
pub mod spec;
pub mod value_agg;
pub mod value_eval;
pub mod value_partition;

pub use eval::evaluate_window_functions;
pub use spec::{FrameBound, WindowFrame, WindowFuncSpec};
pub use value_eval::{WindowError, evaluate_window_functions_value};

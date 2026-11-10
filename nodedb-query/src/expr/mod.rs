// SPDX-License-Identifier: Apache-2.0

//! SqlExpr AST, on-wire codec, and row-scope evaluator.
//!
//! This module is the canonical expression type shared between the planner,
//! the Data Plane executor, the UPDATE assignment path, and the WHERE scan
//! filter. It is also the payload carried through msgpack-encoded physical
//! plans, so the zerompk codec must stay in lockstep with the AST variants.

pub mod binary;
pub mod cast_type;
pub mod codec;
pub mod eval;
pub mod math_domain;
pub mod out_of_range;
pub mod types;
pub mod unary;

pub use eval::EvalError;
pub use math_domain::{MathDomain, MathDomainClass};
pub use out_of_range::OutOfRange;
pub use types::{BinaryOp, CastType, ComputedColumn, GroupKeySpec, SqlExpr, WHOLE_ROW_COLUMN};

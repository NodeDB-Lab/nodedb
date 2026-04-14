//! SqlExpr AST, on-wire codec, and row-scope evaluator.
//!
//! This module is the canonical expression type shared between the planner,
//! the Data Plane executor, the UPDATE assignment path, and the WHERE scan
//! filter. It is also the payload carried through msgpack-encoded physical
//! plans, so the zerompk codec must stay in lockstep with the AST variants.

pub mod binary;
pub mod codec;
pub mod eval;
pub mod types;

pub use types::{BinaryOp, CastType, ComputedColumn, SqlExpr};

// SPDX-License-Identifier: Apache-2.0

//! Column DEFAULT expression compilation and evaluation at insert time.
//!
//! A declared DEFAULT is stored in the catalog as text. [`ColumnDefaults`]
//! compiles that text ONCE per statement; the per-row path evaluates the
//! compiled form and never sees a string, so it cannot re-parse.
//!
//! Supported forms: ID generation functions (UUIDv4/v7, ULID, CUID2, NANOID),
//! `NOW()`, sequence accessors (`nextval`, `currval`), literals, and any other
//! expression the plan-time const-folder can resolve.
//!
//! A DEFAULT that cannot be evaluated raises [`crate::SqlError::UnevaluableDefault`].
//! The column is never omitted: an omitted column stores NULL where the
//! declaration promised a value, and nothing reports it.
//!
//! Lives in the planner because every engine's rows materialize their
//! DEFAULTs there, before declared-type coercion and range checks run, so a
//! materialized default is validated exactly like a supplied one and a
//! `DEFAULT now()` means the same thing on every engine.

mod compiled;
mod convert;
mod kind;

pub use compiled::{
    ColumnDefaults, CompiledDefault, default_expr_references_columns, validate_default_expr,
};
pub use convert::default_value_to_sql;

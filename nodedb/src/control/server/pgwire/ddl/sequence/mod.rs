//! Sequence DDL — CREATE / DROP / ALTER / SHOW / DESCRIBE.
//!
//! Split by concern so no single file grows unboundedly:
//! - [`create`]   — `create_sequence`
//! - [`drop`]     — `drop_sequence`
//! - [`alter`]    — `alter_sequence` (RESTART + FORMAT paths)
//! - [`show`]     — `show_sequences` + `describe_sequence`
//! - [`parse`]    — shared option-parsing helpers

pub mod alter;
pub mod create;
pub mod drop;
pub mod parse;
pub mod show;

pub use alter::alter_sequence;
pub use create::create_sequence;
pub use drop::drop_sequence;
pub use show::{describe_sequence, show_sequences};

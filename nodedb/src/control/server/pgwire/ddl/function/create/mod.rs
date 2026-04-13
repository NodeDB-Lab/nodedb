//! `CREATE [OR REPLACE] FUNCTION` — split by concern.
//!
//! - [`handler`] — the `create_function` pgwire entry point
//! - [`parse`]   — `CREATE FUNCTION` grammar parser
//! - [`deps`]    — dependency extraction from function bodies

pub mod deps;
pub mod handler;
pub mod parse;

#[cfg(test)]
mod tests;

pub use handler::create_function;
// Re-export so `super::create::ParsedCreateFunction` continues to
// work for `validate.rs` and other callers.
pub use parse::ParsedCreateFunction;

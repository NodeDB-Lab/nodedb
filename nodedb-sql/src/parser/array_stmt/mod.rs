//! Recursive-descent parser for NodeDB array DDL/DML.
//!
//! Recognises `CREATE ARRAY`, `DROP ARRAY`, `INSERT INTO ARRAY`, and
//! `DELETE FROM ARRAY` — non-standard syntax that sqlparser-rs cannot
//! accept. `try_parse_array_statement` returns `None` for any other
//! input so the caller can fall through to the standard sqlparser path.

pub mod ast;
pub mod lexer;
pub mod parse;

pub use ast::{ArrayStatement, CreateArrayAst, DeleteArrayAst, DropArrayAst, InsertArrayAst};
pub use parse::try_parse_array_statement;

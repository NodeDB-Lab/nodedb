// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handler for CONVERT COLLECTION.
//!
//! Syntax:
//! - `CONVERT COLLECTION <name> TO document_schemaless`
//! - `CONVERT COLLECTION <name> TO document_strict (col1 TYPE, col2 TYPE, ...)`
//! - `CONVERT COLLECTION <name> TO kv`

pub mod column_defs;
pub mod driver;
mod support;
pub mod type_map;
pub mod typeguard_columns;

pub use driver::convert_collection;

// SPDX-License-Identifier: Apache-2.0

//! `Parser` struct and recursive-descent parse methods for array statements.

mod alter;
mod attr_meta;
mod core;
mod create;
mod delete_stmt;
mod drop;
mod georef;
mod insert;

pub(in crate::parser::array_stmt::parse) use core::Parser;

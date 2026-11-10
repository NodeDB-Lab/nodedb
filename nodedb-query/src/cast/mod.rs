// SPDX-License-Identifier: Apache-2.0

//! CAST evaluation for SqlExpr, with PostgreSQL's input and conversion rules.

mod eval;
mod hex_float;
mod int_width;
mod number;
mod text;

pub use eval::eval_cast;
pub use int_width::IntWidth;
pub use number::{float_to_int, float_to_numeric, numeric_to_int};
pub use text::{parse_bool, parse_float8, parse_int, parse_numeric};

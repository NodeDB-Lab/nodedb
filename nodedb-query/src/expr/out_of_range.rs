// SPDX-License-Identifier: Apache-2.0

//! The ways a value falls outside the range of its result type.

/// A value outside the range of its result type, with PostgreSQL's message
/// for each case. Every case is SQLSTATE `22003`
/// (numeric_value_out_of_range).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum OutOfRange {
    /// A `bigint` result past the `i64` range: arithmetic, or a cast from a
    /// float or a numeric.
    #[error("bigint out of range")]
    Bigint,
    /// An `integer` result past the `i32` range: arithmetic, or a cast.
    #[error("integer out of range")]
    Integer,
    /// A `smallint` result past the `i16` range: arithmetic, or a cast.
    #[error("smallint out of range")]
    Smallint,
    /// A numeric result past the range `Decimal` holds.
    #[error("value overflows numeric format")]
    Numeric,
    /// A float8 result that overflows from finite operands.
    #[error("value out of range: overflow")]
    FloatOverflow,
    /// A float8 result that underflows to zero from nonzero operands.
    #[error("value out of range: underflow")]
    FloatUnderflow,
    /// Text that spells an integer past the range of `type_name`:
    /// `smallint`, `integer` or `bigint`.
    #[error("value \"{text}\" is out of range for type {type_name}")]
    IntegerText {
        type_name: &'static str,
        text: String,
    },
    /// Text that spells a number past the `double precision` range.
    #[error("\"{0}\" is out of range for type double precision")]
    FloatText(String),
}

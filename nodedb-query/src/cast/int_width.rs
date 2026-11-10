// SPDX-License-Identifier: Apache-2.0

//! The integer widths a cast targets: `smallint`, `integer`, `bigint`.

use crate::expr::{EvalError, OutOfRange};

/// The width of an integer cast target. Every integer is an `i64` at run
/// time; a narrower width bounds the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntWidth {
    /// `smallint`, the `i16` range.
    Int2,
    /// `integer`, the `i32` range.
    Int4,
    /// `bigint`, the `i64` range.
    Int8,
}

impl IntWidth {
    /// The PostgreSQL type name.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Int2 => "smallint",
            Self::Int4 => "integer",
            Self::Int8 => "bigint",
        }
    }

    /// The least and greatest value of the width.
    pub fn bounds(self) -> (i64, i64) {
        match self {
            Self::Int2 => (i64::from(i16::MIN), i64::from(i16::MAX)),
            Self::Int4 => (i64::from(i32::MIN), i64::from(i32::MAX)),
            Self::Int8 => (i64::MIN, i64::MAX),
        }
    }

    /// The error for a value past the width: "smallint out of range",
    /// "integer out of range" or "bigint out of range".
    pub fn out_of_range(self) -> EvalError {
        EvalError::OutOfRange(match self {
            Self::Int2 => OutOfRange::Smallint,
            Self::Int4 => OutOfRange::Integer,
            Self::Int8 => OutOfRange::Bigint,
        })
    }

    /// `i` when the width holds it, else [`Self::out_of_range`].
    pub fn fit(self, i: i64) -> Result<i64, EvalError> {
        let (min, max) = self.bounds();
        if (min..=max).contains(&i) {
            Ok(i)
        } else {
            Err(self.out_of_range())
        }
    }
}

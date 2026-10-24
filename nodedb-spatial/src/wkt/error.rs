// SPDX-License-Identifier: Apache-2.0

//! WKT parse errors.

use nodedb_types::geometry::{Dimension, OrdinateError};

/// Why a WKT or EWKT text does not parse.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("WKT parse error at offset {offset}: {kind}")]
pub struct WktError {
    /// Byte offset into the input where the error was found.
    pub offset: usize,
    pub kind: WktErrorKind,
}

/// The cause of a [`WktError`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WktErrorKind {
    #[error("unexpected end of input, expected {expected}")]
    UnexpectedEnd { expected: &'static str },
    #[error("found '{found}', expected {expected}")]
    UnexpectedToken {
        found: Box<str>,
        expected: &'static str,
    },
    #[error("unknown geometry type '{name}'")]
    UnknownType { name: Box<str> },
    #[error("'{text}' is not a finite number")]
    BadNumber { text: Box<str> },
    #[error("coordinate has {found} numbers, {dimension} takes {expected}")]
    CoordinateCount {
        found: usize,
        expected: u8,
        dimension: Dimension,
    },
    #[error("coordinate has {found} numbers; WKT takes 2, 3 or 4")]
    UntaggedCount { found: usize },
    #[error("collection member is {member}, the collection is {parent}")]
    MemberDimension {
        parent: Dimension,
        member: Dimension,
    },
    #[error("{0}")]
    Ordinates(#[from] OrdinateError),
    #[error("EMPTY members inside {kind} are not supported")]
    EmptyMember { kind: &'static str },
    #[error("GEOMETRYCOLLECTION nesting exceeds {max} levels")]
    TooDeep { max: usize },
    #[error("SRID prefix '{text}' is not 'SRID=<digits>;'")]
    BadSridPrefix { text: Box<str> },
    #[error("trailing input '{text}'")]
    Trailing { text: Box<str> },
}

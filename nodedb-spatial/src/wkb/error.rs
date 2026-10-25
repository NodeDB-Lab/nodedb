// SPDX-License-Identifier: Apache-2.0

//! WKB decode and encode errors.

use nodedb_types::geometry::{Dimension, OrdinateError};

/// Why WKB bytes do not decode, or a geometry does not encode.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WkbError {
    #[error("WKB truncated at byte {offset}")]
    Truncated { offset: usize },
    #[error("WKB byte order marker {byte} at byte {offset} is neither 0 nor 1")]
    ByteOrder { byte: u8, offset: usize },
    #[error("WKB type word {word:#010x} at byte {offset} names no geometry type")]
    UnknownType { word: u32, offset: usize },
    #[error(
        "WKB type word {word:#010x} at byte {offset} has EWKB flags and ISO code for different dimensions"
    )]
    ConflictingDimension { word: u32, offset: usize },
    #[error("WKB {parent} member at byte {offset} is {found}")]
    MemberKind {
        parent: &'static str,
        found: &'static str,
        offset: usize,
    },
    #[error("WKB collection member at byte {offset} is {member}, the collection is {parent}")]
    MemberDimension {
        parent: Dimension,
        member: Dimension,
        offset: usize,
    },
    #[error("WKB coordinate at byte {offset} is not finite")]
    NonFinite { offset: usize },
    #[error("WKB count {count} exceeds {max} elements")]
    TooManyElements { count: usize, max: usize },
    #[error("WKB exceeds {max} bytes")]
    TooLarge { max: usize },
    #[error("WKB collection nesting exceeds {max} levels")]
    TooDeep { max: usize },
    #[error("{count} bytes follow the WKB geometry")]
    Trailing { count: usize },
    #[error("WKB ordinates: {0}")]
    Ordinates(#[from] OrdinateError),
}

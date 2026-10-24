// SPDX-License-Identifier: Apache-2.0

//! Error types for Binary Tuple encoding/decoding.

use nodedb_types::columnar::ColumnType;

/// Errors from Binary Tuple encoding and decoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StrictError {
    /// Wrong number of values for the schema.
    #[error("expected {expected} values, got {got}")]
    ValueCountMismatch { expected: usize, got: usize },

    /// A value's type doesn't match the schema column type.
    #[error("column '{column}': expected {expected}, got incompatible value")]
    TypeMismatch {
        column: String,
        expected: ColumnType,
    },

    /// A value of an accepted variant does not convert to the column type.
    /// A text that does not parse as a UUID is one example.
    #[error("column '{column}': invalid {expected} value: {detail}")]
    InvalidValue {
        column: String,
        expected: ColumnType,
        detail: String,
    },

    /// A stored variable-length cell whose bytes do not hold its type.
    #[error("column of type {column_type}: corrupt cell: {detail}")]
    CorruptCell {
        column_type: ColumnType,
        detail: String,
    },

    /// A non-nullable column received a null value with no default.
    #[error("column '{0}' is NOT NULL and has no default")]
    NullViolation(String),

    /// Tuple bytes are too short to contain the header.
    #[error("tuple too short: need at least {expected} bytes, got {got}")]
    TruncatedTuple { expected: usize, got: usize },

    /// Column index out of range for the schema.
    #[error("column index {index} out of range (schema has {count} columns)")]
    ColumnOutOfRange { index: usize, count: usize },

    /// Offset table entry points outside the tuple.
    #[error("corrupt offset table: offset {offset} exceeds tuple length {len}")]
    CorruptOffset { offset: u32, len: usize },

    /// Schema version in tuple is newer than the reader's schema.
    #[error("tuple schema version {tuple_version} is newer than reader version {reader_version}")]
    NewerSchemaVersion {
        tuple_version: u16,
        reader_version: u16,
    },

    /// Magic bytes at the start of the tuple do not match `MAGIC`.
    #[error("invalid tuple magic: expected 0x{expected:08X}, got 0x{got:08X}")]
    InvalidMagic { expected: u32, got: u32 },

    /// Format version byte does not match `FORMAT_VERSION`.
    #[error("unsupported tuple format version: expected {expected}, got {got}")]
    InvalidFormatVersion { expected: u8, got: u8 },
}

// SPDX-License-Identifier: Apache-2.0

//! Error types for columnar segment operations.

/// Errors from columnar segment encoding, decoding, and validation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnarError {
    #[error("codec error: {0}")]
    Codec(#[from] nodedb_codec::CodecError),

    #[error("segment too short: expected at least {expected} bytes, got {got}")]
    TruncatedSegment { expected: usize, got: usize },

    #[error("invalid magic bytes: expected NDBS, got {0:?}")]
    InvalidMagic([u8; 4]),

    #[error(
        "incompatible segment version: reader {reader_major}.x, segment {segment_major}.{segment_minor}"
    )]
    IncompatibleVersion {
        reader_major: u8,
        segment_major: u8,
        segment_minor: u8,
    },

    #[error("footer CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    FooterCrcMismatch { stored: u32, computed: u32 },

    #[error("column index {index} out of range (segment has {count} columns)")]
    ColumnOutOfRange { index: usize, count: usize },

    #[error("schema mismatch: expected {expected} columns, memtable has {got}")]
    SchemaMismatch { expected: usize, got: usize },

    #[error("memtable is empty — nothing to flush")]
    EmptyMemtable,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("value type mismatch for column '{column}': expected {expected}")]
    TypeMismatch { column: String, expected: String },

    #[error("JSON parse error for column '{column}': {source}")]
    JsonParse {
        column: String,
        source: sonic_rs::Error,
    },

    #[error("range literal parse error for column '{column}': invalid range literal '{literal}'")]
    RangeParse { column: String, literal: String },

    #[error("MessagePack serialization error for column '{column}': {source}")]
    MsgpackSerialize {
        column: String,
        source: zerompk::Error,
    },

    #[error("MessagePack deserialization error for column '{column}': {source}")]
    MsgpackDeserialize {
        column: String,
        source: nodedb_types::MsgpackError,
    },

    /// A value for a `Geometry` column that does not parse or encode.
    #[error("geometry for column '{column}': {source}")]
    Geometry {
        column: String,
        source: nodedb_spatial::SpatialError,
    },

    #[error("null violation: column '{0}' is NOT NULL")]
    NullViolation(String),

    #[error("duplicate primary key")]
    DuplicatePrimaryKey,

    #[error("primary key not found")]
    PrimaryKeyNotFound,

    #[error("segment ID space exhausted: u64::MAX segments have been allocated")]
    SegmentIdExhausted,

    /// Structural corruption detected while parsing a segment header or footer.
    ///
    /// `offset` is the byte position from the start of the segment where the
    /// problem was detected. `segment_id` is `None` when the parse fails before
    /// the id can be resolved (e.g. inside the footer itself).
    #[error("segment corruption in {segment_id:?} at offset {offset:?}: {reason}")]
    Corruption {
        segment_id: Option<String>,
        reason: String,
        offset: Option<u64>,
    },

    /// A memtable cell whose bytes do not hold a value of its declared type.
    ///
    /// The memtable writer encodes every cell it stores, so this is in-memory
    /// corruption. The read is refused rather than answered with `NULL`.
    #[error("memtable cell corrupt: column '{column}' row {row}: {reason}")]
    MemtableCellCorrupt {
        column: String,
        row: usize,
        reason: String,
    },

    /// A columnar WAL row whose bytes do not decode.
    ///
    /// `offset` is the byte position in the row where decoding stopped. The
    /// decode is refused rather than answered with `NULL` or replacement
    /// characters.
    #[error("columnar WAL row corrupt at byte {offset}: {reason}")]
    WalRowCorrupt { offset: usize, reason: String },

    /// A string column cell whose bytes are not UTF-8 at segment write time.
    ///
    /// The memtable push stores only valid UTF-8, so these bytes were
    /// damaged in memory. The segment write is refused rather than written
    /// with wrong block statistics.
    #[error("string column '{column}' row {row} holds bytes that are not UTF-8")]
    StringCellNotUtf8 { column: String, row: usize },

    /// Segment is encrypted (starts with `SEGV`) but no KEK was supplied.
    #[error(
        "columnar segment is encrypted but no encryption key was provided; \
         cannot read an encrypted segment without a key"
    )]
    MissingKek,

    /// Segment is plaintext (`NDBS`) but a KEK is configured (policy violation).
    #[error(
        "columnar segment is plaintext but an encryption key is configured; \
         refusing to load an unencrypted segment when encryption is required"
    )]
    KekRequired,

    /// AES-256-GCM encryption of the segment payload failed.
    #[error("columnar segment encryption failed: {0}")]
    EncryptionFailed(String),

    /// AES-256-GCM decryption of the segment payload failed.
    #[error("columnar segment decryption failed: {0}")]
    DecryptionFailed(String),

    /// Memory budget exhausted or global ceiling exceeded.
    #[error("memory budget exhausted: {0}")]
    BudgetExhausted(#[from] nodedb_mem::MemError),
}

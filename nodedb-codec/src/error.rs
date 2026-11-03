// SPDX-License-Identifier: Apache-2.0

//! Error types for codec operations.

/// Errors that can occur during encoding or decoding.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// Input data is too short or truncated.
    #[error("truncated input: expected at least {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },

    /// Input data is corrupted (invalid header, bad checksum, etc.).
    #[error("corrupt data: {detail}")]
    Corrupt { detail: String },

    /// Decompression failed (LZ4/Zstd library error).
    #[error("decompression failed: {detail}")]
    DecompressFailed { detail: String },

    /// Compression failed (LZ4/Zstd library error).
    #[error("compression failed: {detail}")]
    CompressFailed { detail: String },

    /// A decoded resource declaration exceeds the application's hard limit.
    #[error("resource limit exceeded for {resource}: requested {requested} bytes, limit {limit}")]
    ResourceLimit {
        resource: String,
        requested: usize,
        limit: usize,
    },

    /// A codec was asked to train or encode on input it cannot accept.
    #[error("invalid codec input: {detail}")]
    InvalidInput { detail: String },

    /// The codec stored in metadata doesn't match the expected codec.
    #[error("codec mismatch: expected {expected}, found {found}")]
    CodecMismatch { expected: String, found: String },

    /// Invalid layout construction or access (vector quantization).
    #[error("layout error: {detail}")]
    LayoutError { detail: String },

    /// `ColumnCodec::Auto` reached a point where a concrete codec was required.
    ///
    /// `Auto` is a user-facing selection hint that must be resolved to a
    /// concrete codec at flush time. If `Auto` appears in a serialized
    /// on-disk header or is passed directly to an encoder, the write path
    /// has a bug — `ColumnCodec::try_resolve()` was not called.
    #[error("unresolved Auto codec: codec must be resolved to a concrete variant before writing")]
    UnresolvedAuto,

    /// A codec name string did not match any canonical form.
    ///
    /// Canonical names are lowercase snake_case exactly as returned by
    /// `ColumnCodec::as_str()`. No case-insensitive matching; no hyphen or
    /// space variants. The error message lists all valid names.
    #[error("unknown codec name '{name}'; valid names: {valid}")]
    UnknownCodec { name: String, valid: &'static str },
}

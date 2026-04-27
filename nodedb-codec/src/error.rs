//! Error types for codec operations.

/// Errors that can occur during encoding or decoding.
#[derive(Debug, thiserror::Error)]
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

    /// The codec stored in metadata doesn't match the expected codec.
    #[error("codec mismatch: expected {expected}, found {found}")]
    CodecMismatch { expected: String, found: String },

    /// Invalid layout construction or access (vector quantization).
    #[error("layout error: {detail}")]
    LayoutError { detail: String },
}

// SPDX-License-Identifier: Apache-2.0

//! Vector engine error types.

use nodedb_mem::MemError;

/// Errors from vector index operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VectorError {
    #[error("memory budget exhausted: {0}")]
    BudgetExhausted(#[from] MemError),
    /// An input vector — a search query or an inserted vector — has a
    /// different dimension from the index. The caller's input is wrong; the
    /// index is intact.
    #[error("vector dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    /// Stored data — a PQ code, a segment backing, a materialized node
    /// vector — disagrees with the index dimension. The stored data is
    /// corrupt or belongs to another index.
    #[error("stored vector data has dimension {got}, index expects {expected}")]
    StoredDimensionMismatch { expected: usize, got: usize },
    /// A serialized pre-filter bitmap does not decode. Searching without it
    /// would return rows the filter excludes, so the search fails instead.
    #[error("vector search filter bitmap does not decode: {detail}")]
    InvalidFilterBitmap { detail: String },
    /// An index build or training call received input it cannot use: an
    /// empty training set, a zero dimension, or parameters that do not fit.
    #[error("invalid vector index input: {detail}")]
    InvalidInput { detail: String },
    /// A node's vector could not be materialized: the node is out of range, or
    /// its local storage is empty and no segment backing supplies the data.
    ///
    /// An empty local storage means the index was restored from a graph-only
    /// checkpoint; the vectors live in an external segment that must be
    /// attached with [`crate::HnswIndex::with_backing`] before any caller
    /// copies vectors out of the index.
    #[error(
        "vector for node {id} is unavailable: node storage is empty and no \
         segment backing provides it (graph-only checkpoint without backing?)"
    )]
    VectorUnavailable { id: u32 },
    /// A node's dtype-encoded bytes could not be decoded to f32.
    #[error("vector decode failed for node {id}: {detail}")]
    VectorDecodeFailed { id: u32, detail: String },
    #[error("unsupported HNSW checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
    #[error("invalid PQ codec magic bytes")]
    InvalidMagic,
    #[error("PQ codec deserialization failed: {0}")]
    DeserializationFailed(String),
    /// Checkpoint file is encrypted (starts with `SEGV`) but no KEK was supplied.
    #[error(
        "vector checkpoint is encrypted but no encryption key was provided; \
         cannot load plaintext from an encrypted checkpoint"
    )]
    CheckpointEncryptedNoKey,
    /// Checkpoint file is plaintext but a KEK was configured (policy violation).
    #[error(
        "vector checkpoint is plaintext but an encryption key is configured; \
         refusing to load an unencrypted checkpoint when encryption is required"
    )]
    CheckpointPlaintextKeyRequired,
    /// AES-256-GCM encryption/decryption or envelope framing of a checkpoint failed.
    #[error("vector checkpoint encryption error: {detail}")]
    CheckpointEncryptionError { detail: String },
    /// rkyv or MessagePack serialization of a vector checkpoint failed.
    #[error("vector checkpoint serialization error: {detail}")]
    CheckpointSerializationError { detail: String },
    /// rkyv or MessagePack deserialization of a vector checkpoint failed.
    #[error("vector checkpoint deserialization error: {detail}")]
    CheckpointDeserializationError { detail: String },
    /// I/O error from segment file operations (open, mmap, metadata).
    #[error("vector segment I/O error: {0}")]
    SegmentIo(#[from] std::io::Error),
    /// A quantization codec refused to train or encode.
    #[error("vector codec error: {0}")]
    Codec(#[from] nodedb_codec::CodecError),
    /// A rerank codec is untrained or failed to train. The engine reached
    /// this state; the caller's input did not cause it.
    #[error("vector rerank codec error: {detail}")]
    RerankCodec { detail: String },
}

impl VectorError {
    /// Whether a retry of the same operation can succeed: the memory budget
    /// refused it, and a later attempt can find room.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::BudgetExhausted(_))
    }
}

impl From<crate::rerank::RerankError> for VectorError {
    /// Only `BadInput` is the caller's data error. An untrained or failed
    /// rerank codec is the engine's own error.
    fn from(e: crate::rerank::RerankError) -> Self {
        use crate::rerank::RerankError;
        let detail = e.to_string();
        match e {
            RerankError::BadInput(_) => VectorError::InvalidInput { detail },
            RerankError::NotTrained(_) | RerankError::Train(_) => {
                VectorError::RerankCodec { detail }
            }
        }
    }
}

/// Check that an input vector of length `got` fits an index of dimension
/// `expected`.
pub fn check_dim(expected: usize, got: usize) -> Result<(), VectorError> {
    if expected == got {
        Ok(())
    } else {
        Err(VectorError::DimensionMismatch { expected, got })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rerank::RerankError;

    #[test]
    fn only_bad_rerank_input_is_invalid_input() {
        assert!(matches!(
            VectorError::from(RerankError::BadInput("k".into())),
            VectorError::InvalidInput { .. }
        ));
        assert!(matches!(
            VectorError::from(RerankError::NotTrained("sq8".into())),
            VectorError::RerankCodec { .. }
        ));
        assert!(matches!(
            VectorError::from(RerankError::Train("pq".into())),
            VectorError::RerankCodec { .. }
        ));
    }
}

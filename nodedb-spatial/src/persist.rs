// SPDX-License-Identifier: Apache-2.0

//! R-tree checkpoint/restore for durable persistence.
//!
//! ## On-disk framing
//!
//! **Plaintext** R-tree checkpoints start with the 6-byte magic `RKSPT\0`.
//! The first 4 bytes are never `SEGV`, so detection is unambiguous.
//!
//! **Encrypted** checkpoints use the same SEGV framing as the vector engine:
//!
//! ```text
//! [SEGV (4B)] [version_u16_le (2B)] [cipher_alg_u8 (1B)] [kid_u8 (1B)]
//! [HKDF salt (16B)] [random nonce (12B)] [AES-256-GCM ciphertext + tag]
//! ```
//!
//! The inner payload is either the raw rkyv bytes for R-tree or the msgpack
//! bytes for geohash. The complete preamble is AAD, and each envelope derives
//! a one-use data key from its KEK, salt, nonce, and domain metadata.
//!
//! Storage key scheme (in redb under Namespace::Spatial):
//! - `{collection}\x00{field}\x00rtree` → serialized R-tree entries
//! - `{collection}\x00{field}\x00meta`  → SpatialIndexMeta

use std::collections::HashSet;

use nodedb_crs::InvalidSridCode;
use nodedb_mem::ScopedMemory;
use nodedb_types::geometry::Srid;
use nodedb_types::{BboxSpace, BoundingBox};
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::rtree::{EndedEntry, HistoryRows, LiveStart, RTree, RTreeEntry, TreeSrid, UNVERSIONED};

// ── SEGV framing constants ─────────────────────────────────────────────────
//
// The encrypted envelope itself lives in `nodedb_wal::crypto`; only the
// magic constant is local to this module.

/// Magic bytes identifying an encrypted spatial checkpoint. Shared with
/// `nodedb-vector`'s collection checkpoint format.
const SEGV_MAGIC: [u8; 4] = *b"SEGV";

// ── Plaintext inner-format constants ───────────────────────────────────────

/// Magic header for rkyv-serialized R-tree snapshots (6 bytes).
const RTREE_RKYV_MAGIC: &[u8; 6] = b"RKSPT\0";

/// Format version for rkyv-serialized R-tree snapshots. A snapshot carries
/// the tree's SRID, its bbox space, and its system-time history.
pub const RTREE_FORMAT_VERSION: u8 = 1;

// ── SEGV framing helpers ───────────────────────────────────────────────────

fn encrypt_payload(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    plaintext: &[u8],
) -> Result<Vec<u8>, RTreeCheckpointError> {
    nodedb_wal::crypto::encrypt_segment_envelope(key, &SEGV_MAGIC, plaintext)
        .map_err(|e| RTreeCheckpointError::EncryptionFailed(e.to_string()))
}

fn decrypt_payload(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    blob: &[u8],
) -> Result<Vec<u8>, RTreeCheckpointError> {
    nodedb_wal::crypto::decrypt_segment_envelope(key, &SEGV_MAGIC, blob)
        .map_err(|e| RTreeCheckpointError::DecryptionFailed(e.to_string()))
}

/// Encrypt a geohash msgpack payload (called from `geohash_index.rs`).
pub(crate) fn encrypt_geohash_payload(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    plaintext: &[u8],
) -> Result<Vec<u8>, RTreeCheckpointError> {
    encrypt_payload(key, plaintext)
}

/// Decrypt a geohash msgpack payload (called from `geohash_index.rs`).
pub(crate) fn decrypt_geohash_payload(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    blob: &[u8],
) -> Result<Vec<u8>, RTreeCheckpointError> {
    decrypt_payload(key, blob)
}

// ── Metadata types ─────────────────────────────────────────────────────────

/// Metadata for a persisted spatial index.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct SpatialIndexMeta {
    /// Collection this index belongs to.
    pub collection: String,
    /// Geometry field name being indexed.
    pub field: String,
    /// Index type.
    pub index_type: SpatialIndexType,
    /// Number of entries at last checkpoint.
    pub entry_count: u64,
    /// Bounding box of all indexed geometries (spatial extent).
    pub extent: Option<BoundingBox>,
    /// SRID of every indexed geometry.
    pub srid: Srid,
}

/// Type of spatial index.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToMessagePack, FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
#[non_exhaustive]
pub enum SpatialIndexType {
    RTree = 0,
    Geohash = 1,
}

impl SpatialIndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RTree => "rtree",
            Self::Geohash => "geohash",
        }
    }
}

impl std::fmt::Display for SpatialIndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── rkyv snapshot type ─────────────────────────────────────────────────────

/// rkyv-serialized R-tree snapshot.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct RTreeSnapshotRkyv {
    /// Code of the tree's SRID.
    srid: u32,
    /// Whether the tree's bbox space is planar. A restore reads the space
    /// from here, so it never resolves the SRID against a CRS registry.
    planar: bool,
    entries: Vec<RTreeEntry>,
    /// The start ordinal of each live entry indexed at a versioned ordinal.
    live_starts: Vec<LiveStart>,
    /// The ended entries a read at a past cut still sees.
    ended: Vec<EndedEntry>,
}

impl RTreeSnapshotRkyv {
    fn validate_live_starts(&self) -> Result<(), RTreeCheckpointError> {
        let mut unmatched = HashSet::new();
        unmatched
            .try_reserve(self.live_starts.len())
            .map_err(|error| {
                RTreeCheckpointError::RkyvDeserialize(format!(
                    "cannot check {} live starts: {error}",
                    self.live_starts.len()
                ))
            })?;
        for start in &self.live_starts {
            if start.sys_from == UNVERSIONED {
                return Err(RTreeCheckpointError::RkyvDeserialize(format!(
                    "live start for entry {} uses the unversioned sentinel",
                    start.entry_id
                )));
            }
            if !unmatched.insert(start.entry_id) {
                return Err(RTreeCheckpointError::RkyvDeserialize(format!(
                    "duplicate live start for entry {}",
                    start.entry_id
                )));
            }
        }
        for entry in &self.entries {
            unmatched.remove(&entry.id);
        }
        if let Some(id) = unmatched.into_iter().min() {
            return Err(RTreeCheckpointError::RkyvDeserialize(format!(
                "live start for entry {id} has no live entry"
            )));
        }
        Ok(())
    }
}

// ── RTree checkpoint impl ──────────────────────────────────────────────────

impl RTree {
    /// Serialize the R-tree to bytes for checkpointing.
    ///
    /// When `kek` is `Some`, the inner rkyv payload is wrapped in an AES-256-GCM
    /// encrypted SEGV envelope. When `None`, the raw rkyv bytes (with `RKSPT\0`
    /// inner magic) are returned (existing plaintext format).
    pub fn checkpoint_to_bytes(
        &self,
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> Result<Vec<u8>, RTreeCheckpointError> {
        let history = self.history_rows();
        let snapshot = RTreeSnapshotRkyv {
            srid: self.srid().code(),
            planar: self.space() == BboxSpace::Planar,
            entries: self.entries().into_iter().cloned().collect(),
            live_starts: history.live,
            ended: history.ended,
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .map_err(|e| RTreeCheckpointError::RkyvSerialize(e.to_string()))?;

        // Build inner plaintext: magic + version + rkyv payload.
        let inner_len = RTREE_RKYV_MAGIC.len() + 1 + rkyv_bytes.len();
        let _guard = self.memory().reserve(inner_len).ok();
        let mut inner = Vec::with_capacity(inner_len);
        inner.extend_from_slice(RTREE_RKYV_MAGIC);
        inner.push(RTREE_FORMAT_VERSION);
        inner.extend_from_slice(&rkyv_bytes);

        if let Some(key) = kek {
            return encrypt_payload(key, &inner);
        }

        Ok(inner)
    }

    /// Restore an R-tree from checkpoint bytes.
    ///
    /// `kek` controls the expected framing:
    /// - `None` → file must be plaintext (starting with `RKSPT\0`). If it
    ///   starts with `SEGV`, returns `Err(MissingKek)`.
    /// - `Some(key)` → encryption is **required**. If the file starts with
    ///   `SEGV`, it is decrypted. If plaintext, returns `Err(KekRequired)`.
    pub fn from_checkpoint(
        bytes: &[u8],
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
        memory: ScopedMemory,
    ) -> Result<Self, RTreeCheckpointError> {
        let is_encrypted = bytes.len() >= 4 && bytes[0..4] == SEGV_MAGIC;

        let inner: Vec<u8>;
        let inner_ref: &[u8];

        if is_encrypted {
            if let Some(key) = kek {
                inner = decrypt_payload(key, bytes)?;
                inner_ref = &inner;
            } else {
                return Err(RTreeCheckpointError::MissingKek);
            }
        } else if kek.is_some() {
            return Err(RTreeCheckpointError::KekRequired);
        } else {
            inner_ref = bytes;
        }

        Self::decode_plaintext_inner(inner_ref, memory)
    }

    fn decode_plaintext_inner(
        bytes: &[u8],
        memory: ScopedMemory,
    ) -> Result<Self, RTreeCheckpointError> {
        let header_len = RTREE_RKYV_MAGIC.len() + 1; // magic + version byte
        if bytes.len() <= header_len || &bytes[..RTREE_RKYV_MAGIC.len()] != RTREE_RKYV_MAGIC {
            return Err(RTreeCheckpointError::UnrecognizedFormat);
        }
        let version = bytes[RTREE_RKYV_MAGIC.len()];
        if version != RTREE_FORMAT_VERSION {
            return Err(RTreeCheckpointError::UnsupportedVersion {
                found: version,
                expected: RTREE_FORMAT_VERSION,
            });
        }
        let rkyv_bytes = &bytes[header_len..];
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(rkyv_bytes.len());
        aligned.extend_from_slice(rkyv_bytes);
        let snapshot: RTreeSnapshotRkyv =
            rkyv::from_bytes::<RTreeSnapshotRkyv, rkyv::rancor::Error>(&aligned)
                .map_err(|e| RTreeCheckpointError::RkyvDeserialize(e.to_string()))?;
        snapshot.validate_live_starts()?;
        // The SRID is checked for syntax only.
        let srid = TreeSrid {
            srid: Srid::new(snapshot.srid)?,
            space: if snapshot.planar {
                BboxSpace::Planar
            } else {
                BboxSpace::Geographic
            },
        };
        let mut tree = RTree::bulk_load(snapshot.entries, memory, srid);
        tree.restore_history(HistoryRows {
            live: snapshot.live_starts,
            ended: snapshot.ended,
        });
        Ok(tree)
    }
}

// ── Storage key helpers ────────────────────────────────────────────────────

/// Build the storage key for an R-tree checkpoint.
///
/// Format: `{collection}\0{field}\0rtree`
pub fn rtree_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 8);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"rtree");
    key
}

/// Build the storage key for spatial index metadata.
///
/// Format: `{collection}\0{field}\0meta`
pub fn meta_storage_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + field.len() + 7);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(b"meta");
    key
}

/// Serialize index metadata to bytes.
pub fn serialize_meta(meta: &SpatialIndexMeta) -> Result<Vec<u8>, RTreeCheckpointError> {
    zerompk::to_msgpack_vec(meta).map_err(RTreeCheckpointError::Serialize)
}

/// Deserialize index metadata from bytes.
pub fn deserialize_meta(bytes: &[u8]) -> Result<SpatialIndexMeta, RTreeCheckpointError> {
    zerompk::from_msgpack(bytes).map_err(RTreeCheckpointError::Deserialize)
}

// ── Error type ─────────────────────────────────────────────────────────────

/// Errors during R-tree checkpoint operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RTreeCheckpointError {
    #[error("R-tree checkpoint serialization failed: {0}")]
    Serialize(zerompk::Error),
    #[error("R-tree checkpoint deserialization failed: {0}")]
    Deserialize(zerompk::Error),
    #[error("R-tree rkyv serialization failed: {0}")]
    RkyvSerialize(String),
    #[error("R-tree rkyv deserialization failed: {0}")]
    RkyvDeserialize(String),
    #[error("unsupported R-tree checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
    #[error("unrecognized R-tree checkpoint format (missing RKSPT\\0 magic)")]
    UnrecognizedFormat,
    /// Checkpoint is encrypted (starts with `SEGV`) but no KEK was supplied.
    #[error(
        "spatial checkpoint is encrypted but no encryption key was provided; \
         cannot load an encrypted checkpoint without a key"
    )]
    MissingKek,
    /// Checkpoint is plaintext but a KEK is configured (policy violation).
    #[error(
        "spatial checkpoint is plaintext but an encryption key is configured; \
         refusing to load an unencrypted checkpoint when encryption is required"
    )]
    KekRequired,
    /// AES-256-GCM encryption failed.
    #[error("spatial checkpoint encryption failed: {0}")]
    EncryptionFailed(String),
    /// AES-256-GCM decryption failed.
    #[error("spatial checkpoint decryption failed: {0}")]
    DecryptionFailed(String),
    /// The checkpoint names an SRID code outside the valid range.
    #[error("R-tree checkpoint: {0}")]
    InvalidSrid(#[from] InvalidSridCode),
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "persist_tests.rs"]
mod tests;

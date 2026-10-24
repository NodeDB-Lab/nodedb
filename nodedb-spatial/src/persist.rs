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

use nodedb_crs::InvalidSridCode;
use nodedb_mem::ScopedMemory;
use nodedb_types::geometry::Srid;
use nodedb_types::{BboxSpace, BoundingBox};
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::rtree::{RTree, RTreeEntry, TreeSrid};

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

/// Current format version for rkyv-serialized R-tree snapshots. Version 3
/// carries the tree's SRID and bbox space.
pub const RTREE_FORMAT_VERSION: u8 = 3;

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
        let snapshot = RTreeSnapshotRkyv {
            srid: self.srid().code(),
            planar: self.space() == BboxSpace::Planar,
            entries: self.entries().into_iter().cloned().collect(),
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
        // The SRID is checked for syntax only.
        let srid = TreeSrid {
            srid: Srid::new(snapshot.srid)?,
            space: if snapshot.planar {
                BboxSpace::Planar
            } else {
                BboxSpace::Geographic
            },
        };
        Ok(RTree::bulk_load(snapshot.entries, memory, srid))
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
mod tests {
    use super::*;
    use crate::test_support::{planar_srid, test_memory};

    fn make_entry(id: u64, lng: f64, lat: f64) -> RTreeEntry {
        RTreeEntry {
            id,
            bbox: BoundingBox::from_point(lng, lat),
        }
    }

    #[test]
    fn checkpoint_roundtrip_empty() {
        let tree = RTree::new(test_memory(), TreeSrid::WGS84);
        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn checkpoint_roundtrip_entries() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..100 {
            tree.insert(make_entry(i, (i as f64) * 0.5, (i as f64) * 0.3));
        }
        assert_eq!(tree.len(), 100);

        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert_eq!(restored.len(), 100);

        // All entries should be searchable.
        let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 100);
    }

    #[test]
    fn checkpoint_preserves_ids() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(42, 10.0, 20.0));
        tree.insert(make_entry(99, 30.0, 40.0));

        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();

        let results = restored.search(&BoundingBox::new(5.0, 15.0, 15.0, 25.0));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 42);
    }

    #[test]
    fn corrupted_bytes_returns_error() {
        assert!(matches!(
            RTree::from_checkpoint(&[0xFF, 0xFF, 0xFF], None, test_memory()),
            Err(RTreeCheckpointError::UnrecognizedFormat)
        ));
    }

    #[test]
    fn meta_roundtrip() {
        let meta = SpatialIndexMeta {
            collection: "buildings".to_string(),
            field: "geom".to_string(),
            index_type: SpatialIndexType::RTree,
            entry_count: 1000,
            extent: Some(BoundingBox::new(-180.0, -90.0, 180.0, 90.0)),
            srid: Srid::new(3375).unwrap(),
        };
        let bytes = serialize_meta(&meta).unwrap();
        let restored = deserialize_meta(&bytes).unwrap();
        assert_eq!(restored.collection, "buildings");
        assert_eq!(restored.entry_count, 1000);
        assert_eq!(restored.index_type, SpatialIndexType::RTree);
        assert_eq!(restored.srid, Srid::new(3375).unwrap());
    }

    #[test]
    fn checkpoint_preserves_srid() {
        let mut tree = RTree::new(test_memory(), planar_srid(3376));
        tree.insert(make_entry(7, 708_697.0, 662_326.0));
        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        let restored = RTree::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert_eq!(restored.tree_srid(), planar_srid(3376));
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn storage_key_format() {
        let key = rtree_storage_key("buildings", "geom");
        assert_eq!(key, b"buildings\0geom\0rtree");

        let meta_key = meta_storage_key("buildings", "geom");
        assert_eq!(meta_key, b"buildings\0geom\0meta");
    }

    #[test]
    fn checkpoint_size_reasonable() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..1000 {
            tree.insert(make_entry(i, (i as f64) * 0.01, (i as f64) * 0.01));
        }
        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        // Each entry: id(8) + 4 f64(32) = ~40 bytes + rkyv overhead.
        // 1000 entries ≈ 40-60KB is reasonable.
        assert!(
            bytes.len() < 100_000,
            "checkpoint too large: {} bytes",
            bytes.len()
        );
        assert!(
            bytes.len() > 10_000,
            "checkpoint too small: {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn golden_header_layout() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));
        let bytes = tree.checkpoint_to_bytes(None).unwrap();
        // Magic at bytes[0..6].
        assert_eq!(&bytes[0..6], b"RKSPT\0");
        // Version byte at bytes[6].
        assert_eq!(bytes[6], super::RTREE_FORMAT_VERSION);
        // rkyv payload follows immediately.
        assert!(bytes.len() > 7);
    }

    #[test]
    fn version_mismatch_returns_error() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));
        let mut bytes = tree.checkpoint_to_bytes(None).unwrap();
        // Corrupt the version byte to an unsupported value.
        bytes[6] = 0;
        match RTree::from_checkpoint(&bytes, None, test_memory()) {
            Err(RTreeCheckpointError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, 0);
                assert_eq!(expected, super::RTREE_FORMAT_VERSION);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
        }
    }

    fn make_test_kek() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    #[test]
    fn spatial_rtree_checkpoint_encrypted_at_rest() {
        let kek = make_test_kek();
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        for i in 0..50 {
            tree.insert(make_entry(i, i as f64, i as f64 * 0.5));
        }

        let enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();

        // Encrypted blob must start with SEGV, not RKSPT.
        assert_eq!(&enc_bytes[0..4], b"SEGV");

        // Round-trip: decrypt and verify all entries survive.
        let restored = RTree::from_checkpoint(&enc_bytes, Some(&kek), test_memory()).unwrap();
        assert_eq!(restored.len(), 50);
        let all = restored.search(&BoundingBox::new(-180.0, -90.0, 180.0, 90.0));
        assert_eq!(all.len(), 50);
    }

    #[test]
    fn spatial_rtree_refuses_plaintext_when_kek_required() {
        let kek = make_test_kek();
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));

        // Write plaintext checkpoint.
        let plain_bytes = tree.checkpoint_to_bytes(None).unwrap();

        // Attempting to load with a KEK must be refused.
        assert!(matches!(
            RTree::from_checkpoint(&plain_bytes, Some(&kek), test_memory()),
            Err(RTreeCheckpointError::KekRequired)
        ));
    }

    #[test]
    fn spatial_rtree_refuses_encrypted_without_kek() {
        let kek = make_test_kek();
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));

        let enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();

        // Loading without a key must be refused.
        assert!(matches!(
            RTree::from_checkpoint(&enc_bytes, None, test_memory()),
            Err(RTreeCheckpointError::MissingKek)
        ));
    }

    #[test]
    fn spatial_rtree_tampered_ciphertext_rejected() {
        let kek = make_test_kek();
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert(make_entry(1, 10.0, 20.0));

        let mut enc_bytes = tree.checkpoint_to_bytes(Some(&kek)).unwrap();
        // Flip a byte in the ciphertext region after the current preamble.
        enc_bytes[nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE + 2] ^= 0xFF;

        assert!(matches!(
            RTree::from_checkpoint(&enc_bytes, Some(&kek), test_memory()),
            Err(RTreeCheckpointError::DecryptionFailed(_))
        ));
    }
}

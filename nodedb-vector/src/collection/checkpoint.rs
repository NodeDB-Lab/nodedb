// SPDX-License-Identifier: Apache-2.0

//! Checkpoint serialization and deserialization for `VectorCollection`.
//!
//! ## On-disk framing
//!
//! **Plaintext** checkpoints are raw MessagePack bytes (existing format).
//! The first 4 bytes are never `SEGV`, so detection is unambiguous.
//!
//! **Encrypted** checkpoints use the following layout:
//!
//! ```text
//! [SEGV (4B)] [version_u16_le (2B)] [cipher_alg_u8 (1B)] [kid_u8 (1B)]
//! [HKDF salt (16B)] [random nonce (12B)] [AES-256-GCM ciphertext + tag]
//! ```
//!
//! The complete current preamble is authenticated as AAD. Each envelope uses
//! its salt and nonce to derive a one-use data key from the KEK, eliminating
//! restart-sensitive nonce state.

use std::collections::HashMap;

use nodedb_types::{Surrogate, VectorQuantization};
use serde::{Deserialize, Serialize};

use crate::collection::payload_index::PayloadIndexSetSnapshot;
use crate::collection::segment::{BuildingSegment, DEFAULT_SEAL_THRESHOLD, SealedSegment};
use crate::collection::tier::StorageTier;
use crate::error::VectorError;
use crate::flat::FlatIndex;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::ivf::IvfPqIndex;

use super::lifecycle::VectorCollection;

/// Magic bytes identifying an encrypted vector checkpoint. Shared with
/// `nodedb-spatial`'s SEGV checkpoint format.
const SEGV_MAGIC: [u8; 4] = *b"SEGV";

/// Encrypt `plaintext` into the SEGV envelope from [`nodedb_wal::crypto`].
fn encrypt_checkpoint(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    plaintext: &[u8],
) -> Result<Vec<u8>, VectorError> {
    nodedb_wal::crypto::encrypt_segment_envelope(key, &SEGV_MAGIC, plaintext).map_err(|e| {
        VectorError::CheckpointEncryptionError {
            detail: e.to_string(),
        }
    })
}

/// Decrypt an encrypted checkpoint blob (starting at byte 0, which is `SEGV`).
fn decrypt_checkpoint(
    key: &nodedb_wal::crypto::WalEncryptionKey,
    blob: &[u8],
) -> Result<Vec<u8>, VectorError> {
    nodedb_wal::crypto::decrypt_segment_envelope(key, &SEGV_MAGIC, blob).map_err(|e| {
        VectorError::CheckpointEncryptionError {
            detail: e.to_string(),
        }
    })
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct CollectionSnapshot {
    pub dim: usize,
    pub params_m: usize,
    pub params_m0: usize,
    pub params_ef_construction: usize,
    pub params_metric: u8,
    pub next_id: u32,
    pub growing_base_id: u32,
    pub growing_vectors: Vec<Vec<f32>>,
    pub growing_deleted: Vec<bool>,
    pub sealed_segments: Vec<SealedSnapshot>,
    pub building_segments: Vec<BuildingSnapshot>,
    /// `(global_vector_id, surrogate_u32)` pairs.
    pub surrogate_map: Vec<(u32, u32)>,
    /// `(document_surrogate_u32, [global_vector_ids])` pairs.
    pub multi_doc_map: Vec<(u32, Vec<u32>)>,
    /// Quantization requested at creation, as a `quantization_to_tag` tag.
    pub quantization_tag: u8,
    /// Serialised `PayloadIndexSetSnapshot` (msgpack bytes).
    /// Empty vec = no payload indexes.
    pub payload_index_bytes: Vec<u8>,
    /// Highest WAL LSN whose write is included in this checkpoint. Startup WAL
    /// replay skips records at or below it for this collection so a
    /// straddling-segment record already absorbed here is not re-applied.
    /// `0` means no write is recorded, and it gates nothing.
    pub checkpoint_wal_lsn: u64,
    /// Index type and PQ/IVF parameters. Its HNSW parameters are the
    /// collection's current ones: the restore reads the storage dtype there.
    pub index_config: crate::index_config::IndexConfig,
    /// Encoded trained IVF-PQ index. `None` for a non-IVF collection or one
    /// still buffering toward its training threshold.
    pub ivf_bytes: Option<Vec<u8>>,
    /// System-time span of every node a past cut reads:
    /// `(node id, surrogate, sys_from, sys_to)`.
    pub versions: Vec<super::versions::SpanEntry>,
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct SealedSnapshot {
    pub base_id: u32,
    pub hnsw_bytes: Vec<u8>,
    /// Serialized [`PqCodec`](crate::quantize::pq::PqCodec) bytes of a PQ segment.
    pub pq_bytes: Option<Vec<u8>>,
    /// PQ codes of a PQ segment, `m` bytes per node.
    pub pq_codes: Option<Vec<u8>>,
    /// Serialized [`Sq8Codec`](crate::quantize::sq8::Sq8Codec) bytes (magic +
    /// version + msgpack) of an SQ8 segment.
    pub sq8_bytes: Option<Vec<u8>>,
    /// SQ8 codes of an SQ8 segment.
    /// Layout: `[v0_d0, v0_d1, ..., v1_d0, ...]` (dim bytes per vector).
    pub sq8_codes: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct BuildingSnapshot {
    pub base_id: u32,
    pub vectors: Vec<Vec<f32>>,
    pub deleted: Vec<bool>,
}

impl VectorCollection {
    /// Serialize all segments for checkpointing.
    ///
    /// When `kek` is `Some`, the MessagePack payload is wrapped in an
    /// AES-256-GCM encrypted envelope with a `SEGV` preamble. When `None`,
    /// raw MessagePack bytes are returned (existing plaintext format).
    ///
    /// Returns `Err` on serialization or encryption failure — callers must
    /// propagate rather than treat a failed encode as an empty checkpoint.
    pub fn checkpoint_to_bytes(
        &self,
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> Result<Vec<u8>, VectorError> {
        let snapshot = CollectionSnapshot {
            dim: self.dim,
            params_m: self.params.m,
            params_m0: self.params.m0,
            params_ef_construction: self.params.ef_construction,
            params_metric: self.params.metric as u8,
            next_id: self.next_id,
            growing_base_id: self.growing_base_id,
            growing_vectors: (0..self.growing.len() as u32)
                .filter_map(|i| self.growing.get_vector_raw(i).map(|v| v.to_vec()))
                .collect(),
            growing_deleted: (0..self.growing.len() as u32)
                .map(|i| self.growing.is_deleted(i))
                .collect(),
            sealed_segments: self
                .sealed
                .iter()
                .map(super::checkpoint_codec::sealed_snapshot)
                .collect::<Result<Vec<_>, VectorError>>()?,
            building_segments: self
                .building
                .iter()
                .map(|b| BuildingSnapshot {
                    base_id: b.base_id,
                    vectors: (0..b.flat.len() as u32)
                        .filter_map(|i| b.flat.get_vector_raw(i).map(|v| v.to_vec()))
                        .collect(),
                    deleted: (0..b.flat.len() as u32)
                        .map(|i| b.flat.is_deleted(i))
                        .collect(),
                })
                .collect(),
            surrogate_map: self
                .surrogate_map
                .iter()
                .map(|(&k, s)| (k, s.as_u32()))
                .collect(),
            multi_doc_map: self
                .multi_doc_map
                .iter()
                .map(|(k, v)| (k.as_u32(), v.clone()))
                .collect(),
            quantization_tag: quantization_to_tag(self.quantization),
            payload_index_bytes: {
                let snap = self.payload.to_snapshot();
                match zerompk::to_msgpack_vec(&snap) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        return Err(VectorError::CheckpointSerializationError {
                            detail: e.to_string(),
                        });
                    }
                }
            },
            checkpoint_wal_lsn: self.checkpoint_wal_lsn.max(self.applied_wal_lsn),
            // The HNSW params carry the storage dtype the restore reads.
            index_config: crate::index_config::IndexConfig {
                hnsw: self.params.clone(),
                ..self.index_config.clone()
            },
            ivf_bytes: self.ivf.as_ref().map(IvfPqIndex::to_bytes).transpose()?,
            versions: self.versions.to_entries(),
        };
        let msgpack = match zerompk::to_msgpack_vec(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(VectorError::CheckpointSerializationError {
                    detail: e.to_string(),
                });
            }
        };

        if let Some(key) = kek {
            encrypt_checkpoint(key, &msgpack)
        } else {
            Ok(msgpack)
        }
    }

    /// Restore a collection from checkpoint bytes.
    ///
    /// `kek` controls the expected framing:
    /// - `None` → the file must be plaintext MessagePack (starting with bytes
    ///   that are NOT `SEGV`). If the file starts with `SEGV` and no key is
    ///   provided, returns `Err(CheckpointEncryptedNoKey)`.
    /// - `Some(key)` → encryption is **required**. If the file starts with
    ///   `SEGV`, it is decrypted with `key`. If the file is plaintext, returns
    ///   `Err(CheckpointPlaintextKeyRequired)` — refuse to silently load
    ///   unencrypted data when the operator has enabled at-rest encryption.
    ///
    /// Returns `Err(CheckpointDeserializationError)` if the (decrypted)
    /// MessagePack payload, an embedded sealed-segment HNSW checkpoint, or an
    /// embedded PQ/SQ8 codec is structurally corrupt.
    pub fn from_checkpoint(
        bytes: &[u8],
        kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
        memory: nodedb_mem::ScopedMemory,
    ) -> Result<Self, VectorError> {
        let is_encrypted = bytes.len() >= 4 && bytes[0..4] == SEGV_MAGIC;

        let msgpack: Vec<u8>;
        let msgpack_ref: &[u8];

        if is_encrypted {
            if let Some(key) = kek {
                msgpack = decrypt_checkpoint(key, bytes)?;
                msgpack_ref = &msgpack;
            } else {
                return Err(VectorError::CheckpointEncryptedNoKey);
            }
        } else if kek.is_some() {
            return Err(VectorError::CheckpointPlaintextKeyRequired);
        } else {
            msgpack_ref = bytes;
        }

        let snap: CollectionSnapshot = match zerompk::from_msgpack(msgpack_ref) {
            Ok(s) => s,
            Err(e) => {
                return Err(VectorError::CheckpointDeserializationError {
                    detail: format!("collection msgpack decode: {e}"),
                });
            }
        };
        let metric = crate::hnsw::checkpoint::metric_from_code(snap.params_metric)?;
        let params = HnswParams {
            m: snap.params_m,
            m0: snap.params_m0,
            ef_construction: snap.params_ef_construction,
            metric,
            dtype: snap.index_config.hnsw.dtype,
        };

        let mut growing = FlatIndex::new(snap.dim, metric);
        for (i, v) in snap.growing_vectors.iter().enumerate() {
            let deleted = snap.growing_deleted.get(i).copied().unwrap_or(false);
            let inserted = if deleted {
                growing.insert_tombstoned(v.clone())
            } else {
                growing.insert(v.clone())
            };
            inserted.map_err(|e| VectorError::CheckpointDeserializationError {
                detail: format!("growing-segment replay insert: {e}"),
            })?;
        }

        let mut sealed = Vec::with_capacity(snap.sealed_segments.len());
        for ss in &snap.sealed_segments {
            // Sealed segments always carry the `RKHNS\0` magic — we wrote
            // them ourselves. Both a decode `Err` and an `Ok(None)` (magic
            // missing) mean the checkpoint is structurally corrupt; either
            // way the whole restore must fail rather than silently drop the
            // segment's vectors.
            let index = HnswIndex::from_checkpoint(&ss.hnsw_bytes)?.ok_or_else(|| {
                VectorError::CheckpointDeserializationError {
                    detail: "sealed segment hnsw bytes missing RKHNS magic".into(),
                }
            })?;
            let codec = super::checkpoint_codec::restore_codec(ss, index.len(), &memory)?;
            let resident_bytes = SealedSegment::measure(&index, &codec);
            sealed.push(SealedSegment {
                index,
                base_id: ss.base_id,
                codec,
                tier: StorageTier::L0Ram,
                mmap_vectors: None,
                resident_bytes,
            });
        }

        // A segment sealed but not yet built comes back as a building
        // segment, searched by brute force; the owning core queues its build.
        let mut next_segment_id = (sealed.len() + 1) as u32;
        let mut building = Vec::with_capacity(snap.building_segments.len());
        for bs in &snap.building_segments {
            let mut flat = FlatIndex::new(snap.dim, metric);
            for (i, v) in bs.vectors.iter().enumerate() {
                let inserted = if bs.deleted.get(i).copied().unwrap_or(false) {
                    flat.insert_tombstoned(v.clone())
                } else {
                    flat.insert(v.clone())
                };
                inserted.map_err(|e| VectorError::CheckpointDeserializationError {
                    detail: format!("building-segment replay insert: {e}"),
                })?;
            }
            building.push(BuildingSegment {
                flat,
                base_id: bs.base_id,
                segment_id: next_segment_id,
            });
            next_segment_id += 1;
        }

        let index_config = crate::index_config::IndexConfig {
            hnsw: params.clone(),
            ..snap.index_config
        };
        let ivf = snap
            .ivf_bytes
            .as_deref()
            .map(|bytes| IvfPqIndex::from_bytes(bytes, memory.clone()))
            .transpose()?;
        Ok(Self {
            growing,
            growing_base_id: snap.growing_base_id,
            sealed,
            building,
            params,
            next_id: snap.next_id,
            next_segment_id,
            dim: snap.dim,
            data_dir: None,
            ram_budget_bytes: 0,
            mmap_fallback_count: 0,
            mmap_segment_count: 0,
            surrogate_map: snap
                .surrogate_map
                .iter()
                .map(|&(k, s)| (k, Surrogate::new(s)))
                .collect(),
            surrogate_to_local: snap
                .surrogate_map
                .iter()
                .map(|&(k, s)| (Surrogate::new(s), k))
                .collect(),
            multi_doc_map: snap
                .multi_doc_map
                .into_iter()
                .map(|(k, v)| (Surrogate::new(k), v))
                .collect::<HashMap<_, _>>(),
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            index_config,
            ivf,
            quantization: quantization_from_tag(snap.quantization_tag)?,
            payload: if snap.payload_index_bytes.is_empty() {
                super::payload_index::PayloadIndexSet::default()
            } else {
                zerompk::from_msgpack::<PayloadIndexSetSnapshot>(&snap.payload_index_bytes)
                    .map(super::payload_index::PayloadIndexSet::from_snapshot)
                    .map_err(|e| VectorError::CheckpointDeserializationError {
                        detail: format!("payload index decode: {e}"),
                    })?
            },
            arena_index: None,
            checkpoint_wal_lsn: snap.checkpoint_wal_lsn,
            applied_wal_lsn: snap.checkpoint_wal_lsn,
            builds_completed: 0,
            builds_failed: 0,
            codec_refused: Vec::new(),
            versions: super::versions::VectorVersions::from_entries(snap.versions),
        })
    }
}

/// Encode a `VectorQuantization` to a u8 tag for storage.
fn quantization_to_tag(q: VectorQuantization) -> u8 {
    match q {
        VectorQuantization::None => 0,
        VectorQuantization::Sq8 => 1,
        VectorQuantization::Pq => 2,
        VectorQuantization::RaBitQ => 3,
        VectorQuantization::Bbq => 4,
        VectorQuantization::Binary => 5,
        VectorQuantization::Ternary => 6,
        VectorQuantization::Opq => 7,
    }
}

/// Decode a u8 tag back to `VectorQuantization`. An unknown tag means a
/// corrupt checkpoint.
fn quantization_from_tag(tag: u8) -> Result<VectorQuantization, VectorError> {
    Ok(match tag {
        0 => VectorQuantization::None,
        1 => VectorQuantization::Sq8,
        2 => VectorQuantization::Pq,
        3 => VectorQuantization::RaBitQ,
        4 => VectorQuantization::Bbq,
        5 => VectorQuantization::Binary,
        6 => VectorQuantization::Ternary,
        7 => VectorQuantization::Opq,
        _ => {
            return Err(VectorError::CheckpointDeserializationError {
                detail: format!("unknown quantization tag {tag}"),
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::collection::lifecycle::VectorCollection;
    use crate::collection::segment_codec::SegmentCodec;
    use crate::distance::DistanceMetric;
    use crate::hnsw::HnswParams;
    use crate::test_support::test_memory;

    /// SQ8 calibration data survives a checkpoint round-trip without being
    /// recomputed: the restored segment carries the same codec bytes.
    #[test]
    fn checkpoint_roundtrip_preserves_sq8() {
        let params = HnswParams {
            metric: crate::distance::DistanceMetric::L2,
            ..HnswParams::default()
        };
        // The default quantization seals SQ8 segments.
        let mut coll = VectorCollection::with_seal_threshold(8, params, 1024);
        for i in 0..1024u32 {
            let mut v = vec![0.0f32; 8];
            for (d, slot) in v.iter_mut().enumerate() {
                *slot = ((i as f32) * 0.01 + (d as f32) * 0.1).sin();
            }
            coll.insert(v).unwrap();
        }
        let req = coll
            .seal("sq8_test", test_memory())
            .expect("seal produced request");
        assert!(
            coll.complete_build(req.segment_id, crate::test_support::build_now(&req))
                .installed()
        );

        let sealed = coll.sealed_segments();
        assert!(!sealed.is_empty(), "expected at least one sealed segment");
        let SegmentCodec::Sq8 {
            codec: orig_sq8, ..
        } = &sealed[0].codec
        else {
            panic!("an SQ8 collection seals SQ8 segments");
        };
        let orig_dim = orig_sq8.dim();
        // Capture serialized form as ground truth.
        let orig_bytes = orig_sq8.to_bytes();

        let checkpoint = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&checkpoint, None, test_memory()).unwrap();

        let restored_sealed = restored.sealed_segments();
        assert!(!restored_sealed.is_empty());
        let SegmentCodec::Sq8 {
            codec: restored_sq8,
            ..
        } = &restored_sealed[0].codec
        else {
            panic!("the restored segment carries its SQ8 codec");
        };

        assert_eq!(restored_sq8.dim(), orig_dim, "dim mismatch after restore");
        // Byte-level equality guarantees calibration data is persisted, not recomputed.
        assert_eq!(
            restored_sq8.to_bytes(),
            orig_bytes,
            "sq8 codec bytes differ — calibration data was recomputed rather than persisted"
        );
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut coll = VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        for i in 0..50u32 {
            coll.insert(vec![i as f32, 0.0, 0.0]).unwrap();
        }
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);

        let results = restored.search(&[25.0, 0.0, 0.0], 1, 64).unwrap();
        assert_eq!(results[0].id, 25);
    }

    /// The checkpoint watermark (`checkpoint_wal_lsn`) must survive a
    /// checkpoint round-trip. `note_checkpoint_lsn` only feeds the running
    /// `applied_wal_lsn` max — it does NOT move the replay gate. The gate
    /// moves only at checkpoint save (which folds `applied_wal_lsn` into the
    /// persisted watermark via `max`) and at checkpoint load (which exposes
    /// that persisted watermark as the new gate).
    #[test]
    fn checkpoint_roundtrip_preserves_wal_lsn() {
        let mut coll = VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        coll.insert(vec![1.0, 0.0, 0.0]).unwrap();
        coll.note_checkpoint_lsn(42);
        assert_eq!(coll.applied_wal_lsn(), 42);
        assert_eq!(
            coll.checkpoint_wal_lsn(),
            0,
            "gate is not advanced by note; only by load/save"
        );

        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();
        assert_eq!(
            restored.checkpoint_wal_lsn(),
            42,
            "save folds applied_wal_lsn into the persisted gate, so restore exposes it"
        );
    }

    /// `note_checkpoint_lsn` is monotonic and ignores `0` (an unassigned LSN
    /// must never move the watermark, or a stale-low checkpoint would fail to
    /// gate a straddling replay). This behavior lives on `applied_wal_lsn`,
    /// the running max `note_checkpoint_lsn` advances.
    #[test]
    fn note_checkpoint_lsn_is_monotonic_and_ignores_zero() {
        let mut coll = VectorCollection::new(3, HnswParams::default());
        coll.note_checkpoint_lsn(10);
        assert_eq!(coll.applied_wal_lsn(), 10);
        coll.note_checkpoint_lsn(5); // lower — ignored
        assert_eq!(coll.applied_wal_lsn(), 10);
        coll.note_checkpoint_lsn(0); // unassigned — ignored
        assert_eq!(coll.applied_wal_lsn(), 10);
        coll.note_checkpoint_lsn(20); // higher — advances
        assert_eq!(coll.applied_wal_lsn(), 20);
    }

    /// Payload bitmap indexes registered on a vector-primary collection
    /// must survive a checkpoint round-trip — otherwise `WHERE` filters
    /// would silently return zero rows after a node restart.
    #[test]
    fn checkpoint_roundtrip_preserves_payload_bitmap() {
        use crate::collection::PayloadIndexKind;
        use crate::collection::payload_index::FilterPredicate;
        use nodedb_types::Value;
        use std::collections::HashMap;

        let mut coll = VectorCollection::new(
            3,
            HnswParams {
                metric: DistanceMetric::L2,
                ..HnswParams::default()
            },
        );
        coll.payload
            .add_index("category".to_string(), PayloadIndexKind::Equality);
        for i in 0u32..10 {
            let node_id = coll.insert(vec![i as f32, 0.0, 0.0]).unwrap();
            let mut fields = HashMap::new();
            let cat = if i % 2 == 0 { "A" } else { "B" };
            fields.insert("category".to_string(), Value::String(cat.to_string()));
            coll.payload.insert_row(node_id, &fields);
        }

        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();

        let pred = FilterPredicate::Eq {
            field: "category".to_string(),
            value: Value::String("A".to_string()),
        };
        let bm = restored
            .payload
            .pre_filter(&pred)
            .expect("payload index 'category' must be present after restore");
        assert_eq!(
            bm.len(),
            5,
            "5 rows of category=A must survive checkpoint round-trip"
        );
    }
}

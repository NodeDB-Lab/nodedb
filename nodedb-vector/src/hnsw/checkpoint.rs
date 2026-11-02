// SPDX-License-Identifier: Apache-2.0

//! HNSW checkpoint serialization and deserialization.
//!
//! Serialization format: rkyv with a `RKHNS\0` magic header.

use std::cell::RefCell;

use nodedb_types::vector_dtype::VectorStorageDtype;

use crate::distance::DistanceMetric;
use crate::error::VectorError;
use crate::hnsw::arena::BeamSearchArena;
use crate::hnsw::flat_neighbors::FlatNeighborStore;
use crate::hnsw::graph::{ARENA_INITIAL_CAPACITY, HnswIndex, Node, NodeStorage, Xorshift64};

/// Magic header for rkyv-serialized HNSW snapshots (6 bytes).
const HNSW_RKYV_MAGIC: &[u8; 6] = b"RKHNS\0";
/// Current format version for rkyv-serialized HNSW snapshots.
pub const HNSW_FORMAT_VERSION: u8 = 1;

/// Storage dtype of every index restored from a checkpoint.
///
/// The snapshot stores f32 vectors and no dtype. An F16/BF16 index
/// checkpoints its vectors decoded to f32 and restores as an F32 index.
const CHECKPOINT_DTYPE: VectorStorageDtype = VectorStorageDtype::F32;

/// Decode a checkpoint metric code written as `DistanceMetric as u8`.
///
/// # Errors
///
/// [`VectorError::CheckpointDeserializationError`] for a code no metric uses.
pub(crate) fn metric_from_code(code: u8) -> Result<DistanceMetric, VectorError> {
    match code {
        0 => Ok(DistanceMetric::L2),
        1 => Ok(DistanceMetric::Cosine),
        2 => Ok(DistanceMetric::InnerProduct),
        3 => Ok(DistanceMetric::Manhattan),
        4 => Ok(DistanceMetric::Chebyshev),
        5 => Ok(DistanceMetric::Hamming),
        6 => Ok(DistanceMetric::Jaccard),
        7 => Ok(DistanceMetric::Pearson),
        _ => Err(VectorError::CheckpointDeserializationError {
            detail: format!("unknown distance metric code {code}"),
        }),
    }
}

/// Check that a decoded snapshot describes one consistent graph.
fn validate_snapshot(snap: &HnswSnapshotRkyv) -> Result<(), VectorError> {
    let count = snap.node_vectors.len();
    if snap.node_neighbors.len() != count || snap.node_deleted.len() != count {
        return Err(VectorError::CheckpointDeserializationError {
            detail: format!(
                "node counts disagree: {count} vectors, {} neighbor lists, {} deleted flags",
                snap.node_neighbors.len(),
                snap.node_deleted.len()
            ),
        });
    }
    // An empty vector is a graph-only placeholder served by a backing.
    if let Some(v) = snap
        .node_vectors
        .iter()
        .find(|v| !v.is_empty() && v.len() != snap.dim)
    {
        return Err(VectorError::StoredDimensionMismatch {
            expected: snap.dim,
            got: v.len(),
        });
    }
    let in_range = |id: u32| (id as usize) < count;
    let neighbors_in_range = snap
        .node_neighbors
        .iter()
        .flatten()
        .flatten()
        .all(|&id| in_range(id));
    if !neighbors_in_range || snap.entry_point.is_some_and(|ep| !in_range(ep)) {
        return Err(VectorError::CheckpointDeserializationError {
            detail: format!("a neighbor or entry point id is outside {count} nodes"),
        });
    }
    Ok(())
}

/// rkyv-serialized HNSW snapshot. SoA layout for better rkyv compatibility
/// (flat Vecs instead of Vec<struct>).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct HnswSnapshotRkyv {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub metric: u8,
    pub entry_point: Option<u32>,
    pub max_layer: usize,
    pub rng_state: u64,
    /// Per-node vectors (SoA).
    pub node_vectors: Vec<Vec<f32>>,
    /// Per-node neighbor lists (SoA).
    pub node_neighbors: Vec<Vec<Vec<u32>>>,
    /// Per-node deleted flags (SoA).
    pub node_deleted: Vec<bool>,
}

impl HnswIndex {
    /// Serialize ONLY the graph topology (neighbors, entry point, params) to
    /// rkyv bytes.  Vector data is intentionally omitted — `node_vectors` is
    /// filled with empty `Vec<f32>` placeholders.
    ///
    /// Pair with [`Self::from_graph_checkpoint`] on restore.  The same
    /// `RKHNS\0` magic and version byte are used so `from_checkpoint` can
    /// decode the bytes, but the resulting index has empty node storage.
    ///
    /// This is the Lite pagedb-segment write path: vectors live in the segment,
    /// graph topology lives in the B+ tree.  Origin never calls this method.
    pub fn graph_checkpoint_to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            // Placeholder: one empty vec per node so the node count is preserved.
            node_vectors: vec![Vec::new(); self.nodes.len()],
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).map_err(|e| {
            VectorError::CheckpointSerializationError {
                detail: e.to_string(),
            }
        })?;
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + 1 + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.push(HNSW_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// Magic header `RKHNS\0` allows `from_checkpoint` to detect format.
    pub fn checkpoint_to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            node_vectors: self.export_vectors()?,
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).map_err(|e| {
            VectorError::CheckpointSerializationError {
                detail: e.to_string(),
            }
        })?;
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + 1 + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.push(HNSW_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Returns:
    /// - `Ok(Some(index))` — successfully decoded (rkyv format).
    /// - `Ok(None)` — bytes do not start with the `RKHNS\0` magic header.
    /// - `Err(VectorError::UnsupportedVersion)` — magic matches `RKHNS\0` but
    ///   the version byte is not `HNSW_FORMAT_VERSION`; the caller must reject
    ///   the buffer.
    /// - `Err(VectorError::CheckpointDeserializationError)` — magic and
    ///   version matched but the rkyv payload itself failed to decode
    ///   (structurally corrupt checkpoint).
    pub fn from_checkpoint(bytes: &[u8]) -> Result<Option<Self>, crate::error::VectorError> {
        let header_len = HNSW_RKYV_MAGIC.len() + 1; // magic + version byte
        if bytes.len() > header_len && &bytes[..HNSW_RKYV_MAGIC.len()] == HNSW_RKYV_MAGIC {
            let version = bytes[HNSW_RKYV_MAGIC.len()];
            if version != HNSW_FORMAT_VERSION {
                return Err(crate::error::VectorError::UnsupportedVersion {
                    found: version,
                    expected: HNSW_FORMAT_VERSION,
                });
            }
            return Ok(Some(Self::from_rkyv_checkpoint(&bytes[header_len..])?));
        }
        // No recognized magic prefix — no index to restore.
        Ok(None)
    }

    /// Restore from rkyv-serialized bytes.
    fn from_rkyv_checkpoint(bytes: &[u8]) -> Result<Self, VectorError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let snap: HnswSnapshotRkyv =
            rkyv::from_bytes::<HnswSnapshotRkyv, rkyv::rancor::Error>(&aligned).map_err(|e| {
                VectorError::CheckpointDeserializationError {
                    detail: format!("hnsw rkyv decode: {e}"),
                }
            })?;
        Self::from_hnsw_snapshot(snap)
    }

    /// Reconstruct HnswIndex from deserialized snapshot fields.
    ///
    /// The snapshot is untrusted input. Inconsistent node counts, a vector of
    /// the wrong dimension, or an out-of-range node id fail the restore.
    fn from_hnsw_snapshot(snap: HnswSnapshotRkyv) -> Result<Self, VectorError> {
        use nodedb_types::hnsw::HnswParams;

        let metric = metric_from_code(snap.metric)?;
        validate_snapshot(&snap)?;

        let flat = FlatNeighborStore::from_nested(&snap.node_neighbors);

        let nodes: Vec<Node> = snap
            .node_vectors
            .into_iter()
            .zip(snap.node_deleted)
            .map(|(vector, deleted)| Node {
                storage: NodeStorage::F32(vector),
                neighbors: Vec::new(),
                deleted,
            })
            .collect();

        let tombstones = nodes.iter().filter(|n| n.deleted).count();
        let initial_capacity = snap.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Ok(Self {
            dim: snap.dim,
            params: HnswParams {
                m: snap.m,
                m0: snap.m0,
                ef_construction: snap.ef_construction,
                metric,
                dtype: CHECKPOINT_DTYPE,
            },
            nodes,
            tombstones,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng: Xorshift64::new(snap.rng_state),
            flat_neighbors: Some(flat),
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
            #[cfg(not(target_arch = "wasm32"))]
            backing: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::HnswSnapshotRkyv;
    use crate::distance::DistanceMetric;
    use crate::error::VectorError;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn make_index() -> HnswIndex {
        HnswIndex::with_seed(
            3,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 32,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            12345,
        )
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut idx = make_index();
        for i in 0..50 {
            idx.insert(vec![(i as f32) * 0.1, (i as f32) * 0.2, (i as f32) * 0.3])
                .unwrap();
        }

        let bytes = idx.checkpoint_to_bytes().unwrap();
        assert!(!bytes.is_empty());

        let restored = HnswIndex::from_checkpoint(&bytes).unwrap().unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);
        assert_eq!(restored.entry_point(), idx.entry_point());
        assert_eq!(restored.max_layer(), idx.max_layer());

        let query = vec![1.0, 2.0, 3.0];
        let orig_results = idx.search(&query, 5, 32).unwrap();
        let rest_results = restored.search(&query, 5, 32).unwrap();
        assert_eq!(orig_results.len(), rest_results.len());
        for (a, b) in orig_results.iter().zip(rest_results.iter()) {
            assert_eq!(a.id, b.id);
            assert!((a.distance - b.distance).abs() < 1e-6);
        }
    }

    #[test]
    fn golden_header_layout() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let bytes = idx.checkpoint_to_bytes().unwrap();
        // Magic at bytes[0..6].
        assert_eq!(&bytes[0..6], b"RKHNS\0");
        // Version byte at bytes[6].
        assert_eq!(bytes[6], super::HNSW_FORMAT_VERSION);
        // rkyv payload follows immediately.
        assert!(bytes.len() > 7);
    }

    #[test]
    fn version_mismatch_returns_error() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let mut bytes = idx.checkpoint_to_bytes().unwrap();
        // Corrupt the version byte to an unsupported value.
        bytes[6] = 0;
        match HnswIndex::from_checkpoint(&bytes) {
            Err(crate::error::VectorError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, 0);
                assert_eq!(expected, super::HNSW_FORMAT_VERSION);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
        }
    }

    #[test]
    fn corrupt_rkyv_payload_returns_deserialization_error() {
        // Valid magic + valid version byte, but the rkyv payload itself is
        // garbage. This must surface as a typed decode error, not silently
        // collapse to `Ok(None)` (which would look identical to "no
        // checkpoint present" and drop the restore on the floor).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RKHNS\0");
        bytes.push(super::HNSW_FORMAT_VERSION);
        bytes.extend_from_slice(&[0xFFu8; 64]);

        match HnswIndex::from_checkpoint(&bytes) {
            Err(crate::error::VectorError::CheckpointDeserializationError { .. }) => {}
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected CheckpointDeserializationError, got Ok"),
        }
    }

    /// Every metric survives a checkpoint round trip.
    #[test]
    fn every_metric_round_trips() {
        for metric in [
            DistanceMetric::L2,
            DistanceMetric::Cosine,
            DistanceMetric::InnerProduct,
            DistanceMetric::Manhattan,
            DistanceMetric::Chebyshev,
            DistanceMetric::Hamming,
            DistanceMetric::Jaccard,
            DistanceMetric::Pearson,
        ] {
            let mut idx = HnswIndex::with_seed(
                3,
                HnswParams {
                    metric,
                    ..make_index().params().clone()
                },
                1,
            );
            idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
            let restored = HnswIndex::from_checkpoint(&idx.checkpoint_to_bytes().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(restored.metric(), metric);
        }
    }

    /// A valid snapshot of three 2-dim nodes in a line, entry point 0.
    fn snapshot() -> HnswSnapshotRkyv {
        HnswSnapshotRkyv {
            dim: 2,
            m: 4,
            m0: 8,
            ef_construction: 32,
            metric: DistanceMetric::L2 as u8,
            entry_point: Some(0),
            max_layer: 0,
            rng_state: 1,
            node_vectors: vec![vec![0.0, 0.0], vec![1.0, 0.0], vec![2.0, 0.0]],
            node_neighbors: vec![vec![vec![1]], vec![vec![0, 2]], vec![vec![1]]],
            node_deleted: vec![false; 3],
        }
    }

    fn encode(snap: &HnswSnapshotRkyv) -> Vec<u8> {
        let mut buf = super::HNSW_RKYV_MAGIC.to_vec();
        buf.push(super::HNSW_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv::to_bytes::<rkyv::rancor::Error>(snap).unwrap());
        buf
    }

    #[test]
    fn a_valid_snapshot_restores() {
        let idx = HnswIndex::from_checkpoint(&encode(&snapshot()))
            .unwrap()
            .unwrap();
        assert_eq!(idx.search(&[2.0, 0.0], 1, 8).unwrap()[0].id, 2);
    }

    /// A corrupt snapshot fails the restore instead of panicking a later search.
    #[test]
    fn a_corrupt_snapshot_is_a_typed_error() {
        let mut wrong_dim = snapshot();
        wrong_dim.node_vectors[1] = vec![1.0, 0.0, 0.0];
        let mut short_flags = snapshot();
        short_flags.node_deleted.pop();
        let mut bad_neighbor = snapshot();
        bad_neighbor.node_neighbors[2][0].push(9);
        let mut bad_entry = snapshot();
        bad_entry.entry_point = Some(3);
        let mut bad_metric = snapshot();
        bad_metric.metric = 200;

        assert!(matches!(
            HnswIndex::from_checkpoint(&encode(&wrong_dim)),
            Err(VectorError::StoredDimensionMismatch {
                expected: 2,
                got: 3
            })
        ));
        for (name, snap) in [
            ("short flags", short_flags),
            ("bad neighbor", bad_neighbor),
            ("bad entry", bad_entry),
            ("bad metric", bad_metric),
        ] {
            assert!(
                matches!(
                    HnswIndex::from_checkpoint(&encode(&snap)),
                    Err(VectorError::CheckpointDeserializationError { .. })
                ),
                "{name} must fail the restore"
            );
        }
    }
}

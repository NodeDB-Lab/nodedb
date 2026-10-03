// SPDX-License-Identifier: Apache-2.0

//! CSR checkpoint serialization via rkyv. On little-endian platforms
//! dense arrays are restored zero-copy by pointing `DenseArray` at the
//! archived buffer.
//!
//! Used by both Origin (via redb storage) and Lite (via embedded checkpoint).

use std::collections::HashMap;
use std::mem::size_of;

use nodedb_mem::ScopedMemory;

use super::index::CsrIndex;
use crate::GraphError;

/// Magic header for rkyv-serialized CSR snapshots (6 bytes).
const RKYV_MAGIC: &[u8; 6] = b"RKCS2\0";
/// Current format version for rkyv-serialized CSR snapshots.
///
/// Bumped to `2` when per-edge collection tags were added (parallel
/// `out_collections` / `in_collections` arrays + collection interning). A v1
/// snapshot has no collection axis and is rejected; the CSR is instead rebuilt
/// from the collection-scoped durable edge store.
///
/// Bumped to `3` when per-node surrogates joined the snapshot. They were
/// previously dropped on checkpoint and left at zero on restore, on the theory
/// that later `EdgePut`s would refill them. That is wrong: the surrogate is the
/// node's global, WAL-durable identity, and any read keyed on it — a
/// cross-engine bitmap intersection, a surrogate-seeded traversal — answers
/// with an empty set until an unrelated write happens to touch the node. A v2
/// snapshot is rejected for the same reason v1 is, and takes the same recovery
/// path: rebuild from the durable edge store.
pub const CSR_FORMAT_VERSION: u8 = 3;

/// Errors during CSR checkpoint operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CsrCheckpointError {
    #[error("unsupported CSR checkpoint version {found}; expected {expected}")]
    UnsupportedVersion { found: u8, expected: u8 },
    #[error("CSR checkpoint rkyv deserialization failed")]
    RkyvDeserialize,
}

/// rkyv-serialized CSR snapshot for fast save/load.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CsrSnapshotRkyv {
    nodes: Vec<String>,
    labels: Vec<String>,
    collections: Vec<String>,
    out_offsets: Vec<u32>,
    out_targets: Vec<u32>,
    out_labels: Vec<u32>,
    out_collections: Vec<u32>,
    in_offsets: Vec<u32>,
    in_targets: Vec<u32>,
    in_labels: Vec<u32>,
    in_collections: Vec<u32>,
    buffer_out: Vec<Vec<(u32, u32)>>,
    buffer_in: Vec<Vec<(u32, u32)>>,
    buffer_out_collections: Vec<Vec<u32>>,
    buffer_in_collections: Vec<Vec<u32>>,
    /// Deleted-edge identities `(src, label, dst, collection)`. Collection is
    /// part of the key so per-collection copies of a shared triple tombstone
    /// independently. The v2 snapshot already carries the collection axis, so
    /// widening this key needs no new format version.
    deleted: Vec<(u32, u32, u32, u32)>,
    /// Per-node global surrogate, parallel to `nodes`. `0` means the node has
    /// no surrogate bound. The reverse map is rebuilt from this on load rather
    /// than stored twice.
    node_surrogates: Vec<u32>,
    has_weights: bool,
    out_weights: Option<Vec<f64>>,
    in_weights: Option<Vec<f64>>,
    buffer_out_weights: Vec<Vec<f64>>,
    buffer_in_weights: Vec<Vec<f64>>,
}

impl CsrIndex {
    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError::MemoryBudget`] if a memory governor is installed
    /// and the serialization buffer would exceed the `Graph` engine budget.
    pub fn checkpoint_to_bytes(&self) -> Result<Vec<u8>, GraphError> {
        let snapshot = CsrSnapshotRkyv {
            nodes: self.id_to_node.clone(),
            labels: self.id_to_label.clone(),
            collections: self.id_to_collection.clone(),
            out_offsets: self.out_offsets.clone(),
            out_targets: self.out_targets.to_vec(),
            out_labels: self.out_labels.to_vec(),
            out_collections: self.out_collections.clone(),
            in_offsets: self.in_offsets.clone(),
            in_targets: self.in_targets.to_vec(),
            in_labels: self.in_labels.to_vec(),
            in_collections: self.in_collections.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_in: self.buffer_in.clone(),
            buffer_out_collections: self.buffer_out_collections.clone(),
            buffer_in_collections: self.buffer_in_collections.clone(),
            deleted: self.deleted_edges.iter().copied().collect(),
            node_surrogates: self.node_surrogates.clone(),
            has_weights: self.has_weights,
            out_weights: self.out_weights.as_ref().map(|w| w.to_vec()),
            in_weights: self.in_weights.as_ref().map(|w| w.to_vec()),
            buffer_out_weights: self.buffer_out_weights.clone(),
            buffer_in_weights: self.buffer_in_weights.clone(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("CSR rkyv serialization should not fail");
        let buf_capacity = RKYV_MAGIC.len() + 1 + rkyv_bytes.len();
        let _budget_guard = self.memory.reserve(buf_capacity * size_of::<u8>())?;
        let mut buf = Vec::with_capacity(buf_capacity);
        buf.extend_from_slice(RKYV_MAGIC);
        buf.push(CSR_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Returns:
    /// - `Ok(Some(index))` — successfully decoded.
    /// - `Ok(None)` — buffer does not start with the magic header (no legacy
    ///   format exists for CSR; callers should treat this as an invalid buffer).
    /// - `Err(CsrCheckpointError::UnsupportedVersion)` — magic matches but the
    ///   version byte is not `CSR_FORMAT_VERSION`.
    pub fn from_checkpoint(
        bytes: &[u8],
        memory: ScopedMemory,
    ) -> Result<Option<Self>, CsrCheckpointError> {
        let header_len = RKYV_MAGIC.len() + 1; // magic + version byte
        if bytes.len() > header_len && &bytes[..RKYV_MAGIC.len()] == RKYV_MAGIC {
            let version = bytes[RKYV_MAGIC.len()];
            if version != CSR_FORMAT_VERSION {
                return Err(CsrCheckpointError::UnsupportedVersion {
                    found: version,
                    expected: CSR_FORMAT_VERSION,
                });
            }
            return Ok(Self::from_rkyv_checkpoint(&bytes[header_len..], memory));
        }
        Ok(None)
    }

    /// Restore from rkyv-serialized bytes.
    ///
    /// On little-endian platforms (x86_64, ARM), dense arrays (targets, labels,
    /// weights) are zero-copy: DenseArray points directly into the archived
    /// buffer with no per-element parsing. On big-endian, falls back to full
    /// deserialization.
    fn from_rkyv_checkpoint(bytes: &[u8], memory: ScopedMemory) -> Option<Self> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);

        #[cfg(target_endian = "little")]
        {
            Self::from_rkyv_zero_copy(aligned, memory)
        }
        #[cfg(not(target_endian = "little"))]
        {
            let snap: CsrSnapshotRkyv =
                rkyv::from_bytes::<CsrSnapshotRkyv, rkyv::rancor::Error>(&aligned).ok()?;
            Some(Self::from_snapshot_fields(snap, memory))
        }
    }

    /// Zero-copy restore on little-endian platforms.
    ///
    /// SAFETY: On little-endian, rkyv's `u32_le`/`u16_le`/`f64_le` have
    /// identical memory layout to native `u32`/`u16`/`f64`. The pointer
    /// casts are sound because `ArchivedVec<T>` stores contiguous `T_le`
    /// values, and the `Arc<AlignedVec>` keeps the buffer alive.
    #[cfg(target_endian = "little")]
    fn from_rkyv_zero_copy(aligned: rkyv::util::AlignedVec, memory: ScopedMemory) -> Option<Self> {
        use super::dense_array::DenseArray;

        let backing = std::sync::Arc::new(aligned);

        // Access archived data (zero-copy reference into the buffer).
        let archived =
            rkyv::access::<rkyv::Archived<CsrSnapshotRkyv>, rkyv::rancor::Error>(&backing).ok()?;

        // Zero-copy DenseArrays for dense CSR arrays.
        let out_targets = unsafe {
            let s = archived.out_targets.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<u32>(), s.len())
        };
        let out_labels = unsafe {
            let s = archived.out_labels.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<u32>(), s.len())
        };
        let in_targets = unsafe {
            let s = archived.in_targets.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<u32>(), s.len())
        };
        let in_labels = unsafe {
            let s = archived.in_labels.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<u32>(), s.len())
        };
        let out_weights = archived.out_weights.as_ref().map(|w| unsafe {
            let s = w.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<f64>(), s.len())
        });
        let in_weights = archived.in_weights.as_ref().map(|w| unsafe {
            let s = w.as_slice();
            DenseArray::zero_copy(backing.clone(), s.as_ptr().cast::<f64>(), s.len())
        });

        // Deserialize mutable/small fields (strings, buffers, offsets).
        let snap: CsrSnapshotRkyv =
            rkyv::from_bytes::<CsrSnapshotRkyv, rkyv::rancor::Error>(&backing).ok()?;

        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u32> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u32))
            .collect();
        let node_count = snap.nodes.len();
        let access_counts = (0..node_count).map(|_| std::cell::Cell::new(0)).collect();
        let buffer_out_weights = if snap.buffer_out_weights.len() == node_count {
            snap.buffer_out_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_weights = if snap.buffer_in_weights.len() == node_count {
            snap.buffer_in_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let collection_to_id: HashMap<String, u32> = snap
            .collections
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i as u32))
            .collect();
        let buffer_out_collections = if snap.buffer_out_collections.len() == node_count {
            snap.buffer_out_collections
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_collections = if snap.buffer_in_collections.len() == node_count {
            snap.buffer_in_collections
        } else {
            vec![Vec::new(); node_count]
        };
        let (node_surrogates, surrogate_to_local) =
            Self::restore_surrogates(snap.node_surrogates, node_count);

        Some(Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            collection_to_id,
            id_to_collection: snap.collections,
            out_offsets: snap.out_offsets,
            out_targets,
            out_labels,
            out_collections: snap.out_collections,
            out_weights,
            in_offsets: snap.in_offsets,
            in_targets,
            in_labels,
            in_collections: snap.in_collections,
            in_weights,
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            buffer_out_weights,
            buffer_in_weights,
            buffer_out_collections,
            buffer_in_collections,
            deleted_edges: snap.deleted.into_iter().collect(),
            has_weights: snap.has_weights,
            node_label_bits: vec![0; node_count],
            node_label_to_id: HashMap::new(),
            node_label_names: Vec::new(),
            node_surrogates,
            surrogate_to_local,
            access_counts,
            query_epoch: 0,
            partition_tag: crate::csr::local_node_id::next_partition_tag(),
            memory,
            rebuild_journal: None,
        })
    }

    /// Rebuild the surrogate table and its reverse index from a snapshot.
    ///
    /// A length mismatch means the snapshot's node table and surrogate table
    /// disagree, so no binding can be trusted; the table is zeroed rather than
    /// half-applied, which would silently attach a surrogate to the wrong node.
    fn restore_surrogates(persisted: Vec<u32>, node_count: usize) -> (Vec<u32>, HashMap<u32, u32>) {
        if persisted.len() != node_count {
            return (vec![0; node_count], HashMap::new());
        }
        let mut reverse = HashMap::with_capacity(persisted.len());
        for (local, &raw) in persisted.iter().enumerate() {
            if raw != 0 {
                reverse.insert(raw, local as u32);
            }
        }
        (persisted, reverse)
    }

    /// Reconstruct CsrIndex from deserialized snapshot fields.
    #[cfg(not(target_endian = "little"))]
    fn from_snapshot_fields(snap: CsrSnapshotRkyv, memory: ScopedMemory) -> Self {
        let node_to_id: HashMap<String, u32> = snap
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        let label_to_id: HashMap<String, u32> = snap
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), i as u32))
            .collect();

        let node_count = snap.nodes.len();
        let access_counts = (0..node_count).map(|_| std::cell::Cell::new(0)).collect();

        let buffer_out_weights = if snap.buffer_out_weights.len() == node_count {
            snap.buffer_out_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_weights = if snap.buffer_in_weights.len() == node_count {
            snap.buffer_in_weights
        } else {
            vec![Vec::new(); node_count]
        };
        let collection_to_id: HashMap<String, u32> = snap
            .collections
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i as u32))
            .collect();
        let buffer_out_collections = if snap.buffer_out_collections.len() == node_count {
            snap.buffer_out_collections
        } else {
            vec![Vec::new(); node_count]
        };
        let buffer_in_collections = if snap.buffer_in_collections.len() == node_count {
            snap.buffer_in_collections
        } else {
            vec![Vec::new(); node_count]
        };
        let (node_surrogates, surrogate_to_local) =
            Self::restore_surrogates(snap.node_surrogates, node_count);

        Self {
            node_to_id,
            id_to_node: snap.nodes,
            label_to_id,
            id_to_label: snap.labels,
            collection_to_id,
            id_to_collection: snap.collections,
            out_offsets: snap.out_offsets,
            out_targets: snap.out_targets.into(),
            out_labels: snap.out_labels.into(),
            out_collections: snap.out_collections,
            out_weights: snap.out_weights.map(Into::into),
            in_offsets: snap.in_offsets,
            in_targets: snap.in_targets.into(),
            in_labels: snap.in_labels.into(),
            in_collections: snap.in_collections,
            in_weights: snap.in_weights.map(Into::into),
            buffer_out: snap.buffer_out,
            buffer_in: snap.buffer_in,
            buffer_out_weights,
            buffer_in_weights,
            buffer_out_collections,
            buffer_in_collections,
            deleted_edges: snap.deleted.into_iter().collect(),
            has_weights: snap.has_weights,
            node_label_bits: vec![0; node_count],
            node_label_to_id: HashMap::new(),
            node_label_names: Vec::new(),
            node_surrogates,
            surrogate_to_local,
            access_counts,
            query_epoch: 0,
            partition_tag: crate::csr::local_node_id::next_partition_tag(),
            memory,
            rebuild_journal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::Direction;
    use crate::test_support::test_memory;

    #[test]
    fn checkpoint_roundtrip_unweighted() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.compact()
            .expect("test governor ceiling covers this reservation");

        let bytes = csr
            .checkpoint_to_bytes()
            .expect("test governor ceiling covers this reservation");
        let restored = CsrIndex::from_checkpoint(&bytes, test_memory())
            .expect("roundtrip")
            .unwrap();
        assert_eq!(restored.node_count(), 3);
        assert_eq!(restored.edge_count(), 2);
        assert!(!restored.has_weights());

        let n = restored.neighbors("a", &["KNOWS"], Direction::Out);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].1, "b");
    }

    #[test]
    fn checkpoint_roundtrip_weighted() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_weighted("a", "R", "b", 2.5).unwrap();
        csr.add_edge_weighted("b", "R", "c", 7.0).unwrap();
        csr.add_edge("c", "R", "d").unwrap();
        csr.compact()
            .expect("test governor ceiling covers this reservation");

        let bytes = csr
            .checkpoint_to_bytes()
            .expect("test governor ceiling covers this reservation");
        let restored = CsrIndex::from_checkpoint(&bytes, test_memory())
            .expect("roundtrip")
            .unwrap();
        assert!(restored.has_weights());
        assert_eq!(restored.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(restored.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(restored.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn checkpoint_roundtrip_with_buffer() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        // Don't compact — edges in buffer.
        let bytes = csr
            .checkpoint_to_bytes()
            .expect("test governor ceiling covers this reservation");
        let restored = CsrIndex::from_checkpoint(&bytes, test_memory())
            .expect("roundtrip")
            .unwrap();
        assert_eq!(restored.edge_count(), 1);
    }

    #[test]
    fn golden_header_layout() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let bytes = csr
            .checkpoint_to_bytes()
            .expect("test governor ceiling covers this reservation");
        // Magic at bytes[0..6].
        assert_eq!(&bytes[0..6], b"RKCS2\0");
        // Version byte at bytes[6].
        assert_eq!(bytes[6], super::CSR_FORMAT_VERSION);
        // rkyv payload follows immediately.
        assert!(bytes.len() > 7);
    }

    #[test]
    fn version_mismatch_returns_error() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut bytes = csr
            .checkpoint_to_bytes()
            .expect("test governor ceiling covers this reservation");
        // Corrupt the version byte to an unsupported value.
        bytes[6] = 0;
        match CsrIndex::from_checkpoint(&bytes, test_memory()) {
            Err(CsrCheckpointError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, 0);
                assert_eq!(expected, super::CSR_FORMAT_VERSION);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
        }
    }
}

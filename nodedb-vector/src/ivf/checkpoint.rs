// SPDX-License-Identifier: Apache-2.0

//! Checkpoint encoding for `IvfPqIndex`: centroids, the PQ codec, every
//! cell's ids, codes and FP32 vectors, the tombstones, and the training stamp.
//! Codes are stored, never recomputed on load.

use std::collections::HashMap;

use nodedb_mem::ScopedMemory;
use roaring::RoaringBitmap;

use crate::error::VectorError;
use crate::quantize::pq::PqCodec;

use super::index::{IvfCell, IvfPqIndex};
use super::params::IvfPqParams;

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct IvfSnapshot {
    dim: usize,
    params: IvfPqParams,
    centroids: Vec<Vec<f32>>,
    pq_bytes: Option<Vec<u8>>,
    cells: Vec<IvfCell>,
    deleted: Vec<u32>,
    next_id: u32,
    trained_on: usize,
    trained_at_ms: u64,
}

fn corrupt(detail: String) -> VectorError {
    VectorError::CheckpointDeserializationError { detail }
}

impl IvfPqIndex {
    /// Encode the whole index as MessagePack.
    pub fn to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        let snapshot = IvfSnapshot {
            dim: self.dim,
            params: self.params.clone(),
            centroids: self.centroids.clone(),
            pq_bytes: self.pq.as_ref().map(PqCodec::to_bytes).transpose()?,
            cells: self.cells.clone(),
            deleted: self.deleted.iter().collect(),
            next_id: self.next_id,
            trained_on: self.trained_on,
            trained_at_ms: self.trained_at_ms,
        };
        zerompk::to_msgpack_vec(&snapshot).map_err(|e| VectorError::CheckpointSerializationError {
            detail: format!("IVF-PQ index encode: {e}"),
        })
    }

    /// Decode an index written by [`Self::to_bytes`], charging the PQ codec
    /// to `memory`.
    ///
    /// Fails with [`VectorError::CheckpointDeserializationError`] when the
    /// bytes do not decode or the decoded cells disagree with the dimension,
    /// the PQ code width, or the centroid count.
    pub fn from_bytes(bytes: &[u8], memory: ScopedMemory) -> Result<Self, VectorError> {
        let snap: IvfSnapshot = zerompk::from_msgpack(bytes)
            .map_err(|e| corrupt(format!("IVF-PQ index decode: {e}")))?;
        let pq = snap
            .pq_bytes
            .as_deref()
            .map(|b| PqCodec::from_bytes(b, memory))
            .transpose()
            .map_err(|e| corrupt(format!("IVF-PQ codec decode: {e}")))?;
        let m = pq.as_ref().map_or(0, |pq| pq.m);
        if snap.cells.len() != snap.centroids.len() {
            return Err(corrupt(format!(
                "IVF-PQ index has {} cells for {} centroids",
                snap.cells.len(),
                snap.centroids.len()
            )));
        }
        let mut slots = HashMap::new();
        for (cell_idx, cell) in snap.cells.iter().enumerate() {
            let n = cell.ids.len();
            if cell.codes.len() != n * m || cell.vectors.len() != n * snap.dim {
                return Err(corrupt(format!(
                    "IVF-PQ cell {cell_idx} holds {n} ids, {} code bytes and {} vector \
                     components; expected {} and {}",
                    cell.codes.len(),
                    cell.vectors.len(),
                    n * m,
                    n * snap.dim
                )));
            }
            for (pos, &id) in cell.ids.iter().enumerate() {
                if slots.insert(id, (cell_idx as u32, pos as u32)).is_some() {
                    return Err(corrupt(format!("IVF-PQ index holds vector id {id} twice")));
                }
            }
        }
        let deleted: RoaringBitmap = snap.deleted.into_iter().collect();
        if let Some(id) = deleted.iter().find(|id| !slots.contains_key(id)) {
            return Err(corrupt(format!(
                "IVF-PQ index tombstones vector id {id} it does not hold"
            )));
        }
        Ok(Self {
            dim: snap.dim,
            params: snap.params,
            centroids: snap.centroids,
            pq,
            cells: snap.cells,
            slots,
            deleted,
            next_id: snap.next_id,
            trained_on: snap.trained_on,
            trained_at_ms: snap.trained_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;
    use crate::test_support::test_memory;

    #[test]
    fn round_trip_keeps_entries_tombstones_and_training() {
        let vecs: Vec<Vec<f32>> = (0..32)
            .map(|i| (0..8).map(|d| ((i * 8 + d) as f32) * 0.01).collect())
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut idx = IvfPqIndex::new(
            8,
            IvfPqParams {
                n_cells: 4,
                pq_m: 4,
                pq_k: 8,
                nprobe: 4,
                metric: DistanceMetric::L2,
            },
        );
        idx.train(&refs, test_memory()).unwrap();
        for (i, v) in vecs.iter().enumerate() {
            idx.insert_with_id(10 + i as u32, v.clone()).unwrap();
        }
        idx.delete(12);
        idx.set_trained_at_ms(1_700_000_000_000);

        let restored = IvfPqIndex::from_bytes(&idx.to_bytes().unwrap(), test_memory()).unwrap();
        assert_eq!(restored.len(), 32);
        assert!(restored.is_deleted(12));
        assert_eq!(restored.trained_on(), 32);
        assert_eq!(restored.trained_at_ms(), 1_700_000_000_000);
        assert_eq!(restored.get_vector(20), Some(vecs[10].as_slice()));
        assert_eq!(
            restored.search(&vecs[5], 3, 3).unwrap()[0].id,
            idx.search(&vecs[5], 3, 3).unwrap()[0].id
        );
    }

    #[test]
    fn garbage_is_a_typed_error() {
        assert!(matches!(
            IvfPqIndex::from_bytes(b"not an index", test_memory()),
            Err(VectorError::CheckpointDeserializationError { .. })
        ));
    }
}

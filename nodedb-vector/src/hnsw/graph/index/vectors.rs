// SPDX-License-Identifier: Apache-2.0

//! Reading vector data back out of the index.
//!
//! Three things can hold a node's vector: its own F32 arena, its own
//! dtype-encoded bytes, or an attached segment backing. Reading `node.storage`
//! directly gets the third case wrong and yields an empty placeholder, so every
//! accessor that copies data out goes through [`HnswIndex::materialize_vector`],
//! which reports what is missing instead of handing back a plausible-looking
//! empty vector.

use crate::error::VectorError;

use super::super::types::NodeStorage;
use super::state::HnswIndex;

impl HnswIndex {
    /// Returns a `&[f32]` view of the stored vector for node `id`.
    ///
    /// Returns `Some` only when the index dtype is `F32`. For `F16` or `BF16`
    /// indexes this method returns `None` — use [`Self::get_vector_bytes`]
    /// instead and decode via [`crate::dtype::cast_to_f32`] if an f32 view is
    /// needed.
    ///
    /// In debug builds, calling this on a non-F32 index triggers a
    /// `debug_assert!` to flag misuse early. In release builds the
    /// `debug_assert!` is a no-op and `None` is returned silently.
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        debug_assert!(
            self.params.dtype == nodedb_types::vector_dtype::VectorStorageDtype::F32,
            "get_vector: called on non-F32 index (dtype={}); use get_vector_bytes instead",
            self.params.dtype,
        );
        self.nodes
            .get(id as usize)
            .and_then(|n| n.storage.as_f32_slice())
    }

    /// Dtype-agnostic byte view of the stored vector for node `id`.
    ///
    /// Returns `None` if `id` is out of range. Pair the returned slice with
    /// [`Self::dtype`] to interpret the encoding.
    pub fn get_vector_bytes(&self, id: u32) -> Option<&[u8]> {
        self.nodes.get(id as usize).map(|n| n.storage.as_bytes())
    }

    /// Returns the stored f32 vector for node `id`, consulting the pagedb
    /// segment backing when the node's local storage is empty.
    ///
    /// This is the rerank-safe accessor. It covers both cases that reading
    /// `node.storage` directly gets wrong:
    ///
    /// - Lite's graph-checkpoint-only restore path: after `from_checkpoint` +
    ///   `with_backing`, per-node vectors are empty placeholders and the data
    ///   must be fetched through the backing.
    /// - A narrower storage dtype (F16/BF16): the node holds encoded bytes, not
    ///   f32, so the vector is decoded into an owned `Cow`.
    ///
    /// Returns `None` when `id` is out of range, or the node has no local vector
    /// and no backing supplies one. Decode failure also yields `None` — callers
    /// needing the reason should use [`Self::materialize_vector`].
    ///
    /// Only available on non-WASM targets (the backing type requires mmap).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_vector_or_backing(&self, id: u32) -> Option<std::borrow::Cow<'_, [f32]>> {
        use std::borrow::Cow;
        let node = self.nodes.get(id as usize)?;
        match &node.storage {
            NodeStorage::F32(v) if !v.is_empty() => Some(Cow::Borrowed(v.as_slice())),
            NodeStorage::Bytes { bytes, dtype } if !bytes.is_empty() => {
                crate::dtype::cast_to_f32(bytes, *dtype, self.dim)
                    .ok()
                    .map(Cow::Owned)
            }
            // Empty local storage — the vector lives in the segment backing.
            NodeStorage::F32(_) | NodeStorage::Bytes { .. } => self
                .backing
                .as_ref()
                .and_then(|b| b.get_vector(id))
                .map(Cow::Borrowed),
        }
    }

    /// Materialize node `id`'s vector as an owned `Vec<f32>`, consulting the
    /// segment backing when the node's local storage is empty.
    ///
    /// This is the authoritative accessor for every caller that copies vector
    /// data out of the index — segment serialization, checkpointing, snapshot
    /// export, parameter rebuilds. Reading `node.storage` directly is wrong on
    /// the graph-checkpoint-only restore path, where per-node storage is an
    /// empty placeholder and the real data lives in the attached backing.
    ///
    /// # Errors
    ///
    /// - [`VectorError::VectorUnavailable`] if `id` is out of range, or local
    ///   storage is empty and no backing provides the vector.
    /// - [`VectorError::VectorDecodeFailed`] if dtype-encoded bytes cannot be
    ///   decoded to f32.
    /// - [`VectorError::StoredDimensionMismatch`] if the materialized vector's length
    ///   is not `self.dim`.
    pub fn materialize_vector(&self, id: u32) -> Result<Vec<f32>, VectorError> {
        let node = self
            .nodes
            .get(id as usize)
            .ok_or(VectorError::VectorUnavailable { id })?;
        let local = match &node.storage {
            NodeStorage::F32(v) if !v.is_empty() => Some(v.clone()),
            NodeStorage::Bytes { bytes, dtype } if !bytes.is_empty() => Some(
                crate::dtype::cast_to_f32(bytes, *dtype, self.dim).map_err(|e| {
                    VectorError::VectorDecodeFailed {
                        id,
                        detail: e.to_string(),
                    }
                })?,
            ),
            // Empty local storage: the vector must come from the backing.
            NodeStorage::F32(_) | NodeStorage::Bytes { .. } => None,
        };
        let vector = match local {
            Some(v) => v,
            None => self.backing_vector(id)?,
        };
        if vector.len() != self.dim {
            return Err(VectorError::StoredDimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        Ok(vector)
    }

    /// Whether [`Self::vector_f32`] borrows node `id`'s vector: the node
    /// stores a full f32 vector itself.
    pub fn borrows_f32(&self, id: u32) -> bool {
        self.nodes
            .get(id as usize)
            .and_then(|n| n.storage.as_f32_slice())
            .is_some_and(|v| v.len() == self.dim)
    }

    /// Node `id`'s vector as f32. It is borrowed when the node stores a
    /// full f32 vector, and materialized otherwise.
    ///
    /// # Errors
    ///
    /// As [`Self::materialize_vector`].
    pub fn vector_f32(&self, id: u32) -> Result<std::borrow::Cow<'_, [f32]>, VectorError> {
        use std::borrow::Cow;
        match self
            .nodes
            .get(id as usize)
            .and_then(|n| n.storage.as_f32_slice())
        {
            Some(v) if v.len() == self.dim => Ok(Cow::Borrowed(v)),
            Some(_) | None => self.materialize_vector(id).map(Cow::Owned),
        }
    }

    /// Extract all node vectors as owned F32 vecs for segment serialization.
    ///
    /// The second tuple element is always empty — `HnswIndex` has no surrogate
    /// map.  Surrogates live at the `VectorCollection` layer in Origin.  Lite
    /// passes an empty slice so `write_vector_segment` writes no surrogate block.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::materialize_vector`] for the first node whose vector
    /// cannot be materialized. Serializing an empty or partial payload would
    /// write a segment whose header declares vectors it does not contain, so
    /// this fails loudly instead.
    pub fn extract_vectors_and_surrogates(&self) -> Result<(Vec<Vec<f32>>, Vec<u64>), VectorError> {
        Ok((self.export_vectors()?, Vec::new()))
    }

    /// Export all vectors as F32 for snapshot transfer.
    ///
    /// For F32 indexes this is a clone. For F16/BF16 indexes each vector is
    /// decoded to F32 on the fly. Nodes whose local storage is empty are read
    /// through the attached segment backing.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::materialize_vector`] for the first node whose vector
    /// cannot be materialized. A caller copying vectors out of the index cannot
    /// use an empty placeholder, so this reports the failure rather than
    /// yielding one.
    pub fn export_vectors(&self) -> Result<Vec<Vec<f32>>, VectorError> {
        (0..self.nodes.len() as u32)
            .map(|id| self.materialize_vector(id))
            .collect()
    }
}

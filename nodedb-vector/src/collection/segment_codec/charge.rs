// SPDX-License-Identifier: Apache-2.0

//! Memory reservations a codec build takes on the builder thread.
//!
//! A build reserves each allocation's bytes before it allocates. Scratch
//! reservations drop with their scratch. The reservations that cover a built
//! codec travel with it in a [`CodecCharge`]. The owning core drops that
//! charge when it installs the segment, and its resident charge takes over.

use std::mem::size_of;

use nodedb_codec::vector_quant::codec::VectorCodec;
use nodedb_mem::{ReservationToken, ScopedMemory};

use crate::codec_index::graph::NodeC;
use crate::error::VectorError;

/// Reservations that cover a built codec's allocations until the owning core
/// installs it.
#[derive(Default)]
#[must_use = "dropping a charge releases its reservations"]
pub struct CodecCharge {
    tokens: Vec<ReservationToken>,
    bytes: usize,
}

impl CodecCharge {
    /// Reserve `bytes` from `memory` and hold the reservation. Fails with
    /// [`VectorError::BudgetExhausted`] when the budget refuses it.
    pub fn reserve(&mut self, memory: &ScopedMemory, bytes: usize) -> Result<(), VectorError> {
        if bytes == 0 {
            return Ok(());
        }
        self.tokens.push(memory.reserve(bytes)?);
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    /// Bytes the charge holds.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Reserve what `actual` exceeds the bytes held, so the charge covers an
    /// allocation that outgrew its estimate.
    pub fn cover(&mut self, memory: &ScopedMemory, actual: usize) -> Result<(), VectorError> {
        let missing = actual.saturating_sub(self.bytes);
        self.reserve(memory, missing)
    }
}

/// Reserve `bytes` of scratch from `memory`. Bind the result for as long as
/// the scratch lives: dropping it releases the reservation.
pub(super) fn scratch(memory: &ScopedMemory, bytes: usize) -> Result<CodecCharge, VectorError> {
    let mut charge = CodecCharge::default();
    charge.reserve(memory, bytes)?;
    Ok(charge)
}

/// Bytes of the vector table a codec build reads: one `Cow` per node, the
/// `all` and `sample` slice tables, and `owned` decoded vectors of `dim`
/// floats.
pub(super) fn vector_table_bytes(nodes: usize, sample: usize, owned: usize, dim: usize) -> usize {
    let cow = size_of::<std::borrow::Cow<'_, [f32]>>();
    let slice = size_of::<&[f32]>();
    nodes
        .saturating_mul(cow + slice)
        .saturating_add(sample.saturating_mul(slice))
        .saturating_add(owned.saturating_mul(dim).saturating_mul(size_of::<f32>()))
}

/// Bytes PQ k-means training allocates beyond its codebook: one sub-vector
/// slice and one assignment per training vector, and one running centroid
/// sum per centroid.
pub(super) fn pq_training_bytes(samples: usize, k: usize, dim: usize) -> usize {
    samples
        .saturating_mul(size_of::<&[f32]>() + size_of::<usize>())
        .saturating_add(k.saturating_mul(dim).saturating_mul(size_of::<f32>()))
}

/// Peak bytes OPQ training allocates: the rotated training set, the original
/// and reconstructed matrices and the reconstruction, the `dim × dim`
/// product, its SVD factors and the new rotation, and the codebooks and
/// their k-means sums.
pub(super) fn opq_training_bytes(samples: usize, k: usize, dim: usize) -> usize {
    let f = size_of::<f32>();
    let per_set = samples.saturating_mul(dim).saturating_mul(f);
    let square = dim.saturating_mul(dim).saturating_mul(f);
    let books = k.saturating_mul(dim).saturating_mul(f);
    per_set
        .saturating_mul(4)
        .saturating_add(square.saturating_mul(6))
        .saturating_add(books.saturating_mul(2))
        .saturating_add(samples.saturating_mul(size_of::<usize>()))
}

/// Estimated bytes a codec graph of `nodes` nodes holds, each with a
/// `quantized` payload of that many bytes, under HNSW degree `m`. A layer-0
/// list grows to `m0 + 1` entries before pruning, so its capacity is the
/// next power of two. A node holds `1 / (m - 1)` upper layers on average;
/// the estimate allots one upper list per node, which bounds it.
pub(super) fn graph_bytes<C: VectorCodec>(nodes: usize, quantized: usize, m: usize) -> usize {
    let list = size_of::<Vec<u32>>();
    let pow2 = |n: usize| n.checked_next_power_of_two().unwrap_or(usize::MAX);
    let cap = |degree: usize| pow2(degree.saturating_add(1)).saturating_mul(size_of::<u32>());
    let per_node = size_of::<NodeC<C>>()
        .saturating_add(quantized)
        .saturating_add(list + cap(m.saturating_mul(2)))
        .saturating_add(list + cap(m));
    let id_map = pow2(nodes.saturating_mul(2)).saturating_mul(2 * size_of::<u32>());
    nodes.saturating_mul(per_node).saturating_add(id_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    #[test]
    fn a_charge_covers_growth_past_its_estimate() {
        let memory = test_memory();
        let mut charge = CodecCharge::default();
        charge.reserve(&memory, 100).unwrap();
        charge.cover(&memory, 80).unwrap();
        assert_eq!(charge.bytes(), 100, "an estimate above the actual stays");
        charge.cover(&memory, 150).unwrap();
        assert_eq!(charge.bytes(), 150, "growth past the estimate is reserved");
    }
}

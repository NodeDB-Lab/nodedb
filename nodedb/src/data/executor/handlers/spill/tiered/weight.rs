// SPDX-License-Identifier: BUSL-1.1

//! RAM weight of hot-tier entries. A tiered overlay spills once the summed
//! weight of its hot entries passes `[tuning.query] max_txn_overlay_bytes`.

/// Per-entry bookkeeping charged on top of a key's and a value's weight:
/// the hash-map slot and the entry's seq.
pub(in crate::data::executor) const ENTRY_OVERHEAD: usize = 48;

/// Bytes an owned string or vector header costs beside its heap bytes.
const HEADER: usize = std::mem::size_of::<Vec<u8>>();

/// The RAM a hot-tier key or value holds.
pub(in crate::data::executor) trait SpillWeight {
    fn spill_weight(&self) -> usize;
}

impl SpillWeight for u32 {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<u32>()
    }
}

impl SpillWeight for u64 {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl SpillWeight for i64 {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl SpillWeight for String {
    fn spill_weight(&self) -> usize {
        HEADER + self.len()
    }
}

impl SpillWeight for u8 {
    fn spill_weight(&self) -> usize {
        1
    }
}

impl<T: SpillWeight> SpillWeight for Vec<T> {
    fn spill_weight(&self) -> usize {
        HEADER + self.iter().map(SpillWeight::spill_weight).sum::<usize>()
    }
}

impl SpillWeight for nodedb_types::RowIdentity {
    fn spill_weight(&self) -> usize {
        HEADER + self.as_str().len()
    }
}

impl<A: SpillWeight, B: SpillWeight> SpillWeight for (A, B) {
    fn spill_weight(&self) -> usize {
        self.0.spill_weight() + self.1.spill_weight()
    }
}

impl<A: SpillWeight, B: SpillWeight, C: SpillWeight> SpillWeight for (A, B, C) {
    fn spill_weight(&self) -> usize {
        self.0.spill_weight() + self.1.spill_weight() + self.2.spill_weight()
    }
}

impl<A: SpillWeight, B: SpillWeight, C: SpillWeight, D: SpillWeight> SpillWeight for (A, B, C, D) {
    fn spill_weight(&self) -> usize {
        self.0.spill_weight()
            + self.1.spill_weight()
            + self.2.spill_weight()
            + self.3.spill_weight()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_weights_sum_their_fields() {
        assert_eq!(7u64.spill_weight(), 8);
        assert_eq!((1u32, 2u64).spill_weight(), 12);
        assert_eq!(
            (1u8, 2u32, 3u64, String::from("ab")).spill_weight(),
            1 + 4 + 8 + HEADER + 2
        );
    }
}

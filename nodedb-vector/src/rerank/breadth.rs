// SPDX-License-Identifier: Apache-2.0

//! How many candidates a vector search reads before its exact rerank.

/// Rerank candidates kept per requested row when a query names no
/// `oversample`.
pub const DEFAULT_OVERSAMPLE: u8 = 3;

/// The breadth of one search: the HNSW beam width and the rerank oversample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchBreadth {
    /// HNSW beam width at layer 0. A codec stage widens it to its pool.
    pub ef_search: usize,
    /// Rerank candidates kept per requested row. `0` reads as `1`.
    pub oversample: u8,
}

impl SearchBreadth {
    /// A breadth of beam `ef_search` and the default oversample.
    pub fn new(ef_search: usize) -> Self {
        Self {
            ef_search,
            oversample: DEFAULT_OVERSAMPLE,
        }
    }

    /// Candidates a codec stage keeps for the exact rerank of `top_k` rows.
    pub fn rerank_pool(self, top_k: usize) -> usize {
        rerank_pool(top_k, self.oversample)
    }

    /// The beam a codec stage searches `top_k` rows at: `ef_search`, widened
    /// to the rerank pool so the graph yields every candidate the rerank reads.
    pub fn beam(self, top_k: usize) -> usize {
        self.ef_search.max(self.rerank_pool(top_k))
    }
}

impl From<usize> for SearchBreadth {
    fn from(ef_search: usize) -> Self {
        Self::new(ef_search)
    }
}

/// Candidates a codec stage keeps for the exact rerank: `top_k × oversample`.
/// An `oversample` of `0` reads as `1`, so the pool holds at least `top_k`.
pub fn rerank_pool(top_k: usize, oversample: u8) -> usize {
    top_k.saturating_mul(usize::from(oversample.max(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_is_top_k_times_oversample() {
        assert_eq!(rerank_pool(10, 3), 30);
    }

    #[test]
    fn zero_oversample_reads_as_one() {
        assert_eq!(rerank_pool(7, 0), 7);
    }

    #[test]
    fn beam_widens_to_pool() {
        let breadth = SearchBreadth {
            ef_search: 20,
            oversample: 4,
        };
        assert_eq!(breadth.beam(10), 40);
        assert_eq!(breadth.beam(2), 20);
    }

    #[test]
    fn from_usize_uses_default_oversample() {
        let breadth = SearchBreadth::from(64);
        assert_eq!(breadth.ef_search, 64);
        assert_eq!(breadth.oversample, DEFAULT_OVERSAMPLE);
        assert_eq!(breadth.rerank_pool(5), 15);
    }
}

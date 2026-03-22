//! Vector index configuration: unified config for HNSW, HNSW+PQ, and IVF-PQ.

use super::hnsw::HnswParams;
use super::ivf::IvfPqParams;

/// Index type selection for vector collections.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexType {
    /// Pure HNSW with FP32 vectors. Best recall (~99%), highest memory.
    #[default]
    Hnsw,
    /// HNSW graph with PQ-compressed storage for traversal.
    /// Graph connectivity is maintained with FP32, but distance computation
    /// during search uses PQ lookup tables (O(M) per candidate vs O(D)).
    /// Trade-off: ~95% recall, 4-8x memory reduction.
    HnswPq,
    /// IVF-PQ flat index. Lowest memory (~16 bytes/vector), best for >10M vectors.
    /// Trade-off: ~85-95% recall depending on nprobe.
    IvfPq,
}

impl IndexType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "hnsw" | "" => Some(Self::Hnsw),
            "hnsw_pq" => Some(Self::HnswPq),
            "ivf_pq" | "ivfpq" => Some(Self::IvfPq),
            _ => None,
        }
    }
}

/// Unified vector index configuration.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// HNSW parameters (used for Hnsw and HnswPq types).
    pub hnsw: HnswParams,
    /// Index type.
    pub index_type: IndexType,
    /// PQ subvectors (for HnswPq and IvfPq). Must divide dim evenly.
    pub pq_m: usize,
    /// IVF cells (for IvfPq only).
    pub ivf_cells: usize,
    /// IVF probe count (for IvfPq only).
    pub ivf_nprobe: usize,
}

/// Default PQ subquantizer count (segments per vector).
pub const DEFAULT_PQ_M: usize = 8;
/// Default IVF cell count (Voronoi partitions).
pub const DEFAULT_IVF_CELLS: usize = 256;
/// Default IVF probe count (cells searched per query).
pub const DEFAULT_IVF_NPROBE: usize = 16;

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            hnsw: HnswParams::default(),
            index_type: IndexType::Hnsw,
            pq_m: DEFAULT_PQ_M,
            ivf_cells: DEFAULT_IVF_CELLS,
            ivf_nprobe: DEFAULT_IVF_NPROBE,
        }
    }
}

impl IndexConfig {
    /// Build IVF-PQ params from this config.
    pub fn to_ivf_params(&self) -> IvfPqParams {
        IvfPqParams {
            n_cells: self.ivf_cells,
            pq_m: self.pq_m,
            pq_k: 256,
            nprobe: self.ivf_nprobe,
            metric: self.hnsw.metric,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_index_type() {
        assert_eq!(IndexType::parse("hnsw"), Some(IndexType::Hnsw));
        assert_eq!(IndexType::parse(""), Some(IndexType::Hnsw));
        assert_eq!(IndexType::parse("hnsw_pq"), Some(IndexType::HnswPq));
        assert_eq!(IndexType::parse("ivf_pq"), Some(IndexType::IvfPq));
        assert_eq!(IndexType::parse("ivfpq"), Some(IndexType::IvfPq));
        assert_eq!(IndexType::parse("unknown"), None);
    }

    #[test]
    fn default_is_hnsw() {
        let cfg = IndexConfig::default();
        assert_eq!(cfg.index_type, IndexType::Hnsw);
    }
}

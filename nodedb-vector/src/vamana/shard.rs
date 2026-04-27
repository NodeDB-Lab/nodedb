//! `Shard` trait — abstracts local-mmap vs future remote-RPC partition fetch.
//!
//! Vamana's beam-search rerank step needs full-precision FP32 vectors for
//! exact distance computation.  On a single node these live in an mmap region;
//! in a distributed deployment they would come from a remote RPC or RDMA read.
//! This trait provides that seam without coupling search to any I/O mechanism.

/// Abstraction over a single partition's vector store.
///
/// `LocalShard` is the single-node implementation backed by an in-memory
/// `Vec`.  Future impls (RDMA, QUIC RPC) implement the same interface.
pub trait Shard: Send + Sync {
    /// Vector dimensionality.
    fn dim(&self) -> usize;

    /// Fetch the full-precision FP32 vector at the given node index.
    ///
    /// Returns `None` if `idx` is out of range.  Local = mmap read (or
    /// `Vec` copy in tests); remote = future RPC/RDMA fetch.
    fn fetch_fp32(&self, idx: usize) -> Option<Vec<f32>>;
}

/// Single-node shard backed by an in-memory vector slice.
///
/// In production this would wrap a mmap pointer to the SSD-resident
/// full-precision block described in `storage::VamanaStorageLayout`.
/// For the current tier we hold owned `Vec<Vec<f32>>` so the type compiles
/// and tests pass without io_uring plumbing.
pub struct LocalShard {
    /// Vector dimensionality.
    pub dim: usize,
    /// Dense FP32 vector storage indexed by node order.
    pub vectors: Vec<Vec<f32>>,
}

impl LocalShard {
    /// Create a `LocalShard` from a slice of FP32 vectors.
    ///
    /// All vectors must have the same length (`dim`).  The constructor
    /// is infallible; callers are expected to validate dimensions upstream.
    pub fn new(dim: usize, vectors: Vec<Vec<f32>>) -> Self {
        Self { dim, vectors }
    }
}

impl Shard for LocalShard {
    fn dim(&self) -> usize {
        self.dim
    }

    fn fetch_fp32(&self, idx: usize) -> Option<Vec<f32>> {
        self.vectors.get(idx).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_shard_returns_correct_vector() {
        let vecs = vec![
            vec![1.0_f32, 2.0, 3.0],
            vec![4.0_f32, 5.0, 6.0],
            vec![7.0_f32, 8.0, 9.0],
        ];
        let shard = LocalShard::new(3, vecs.clone());

        assert_eq!(shard.dim(), 3);
        assert_eq!(shard.fetch_fp32(0), Some(vecs[0].clone()));
        assert_eq!(shard.fetch_fp32(1), Some(vecs[1].clone()));
        assert_eq!(shard.fetch_fp32(2), Some(vecs[2].clone()));
    }

    #[test]
    fn local_shard_out_of_range_returns_none() {
        let shard = LocalShard::new(2, vec![vec![1.0, 2.0]]);
        assert!(shard.fetch_fp32(1).is_none());
        assert!(shard.fetch_fp32(100).is_none());
    }

    #[test]
    fn local_shard_empty_returns_none() {
        let shard = LocalShard::new(4, vec![]);
        assert!(shard.fetch_fp32(0).is_none());
    }
}

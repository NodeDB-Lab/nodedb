//! Parallel algorithm execution — range-partition nodes across threads.
//!
//! Each thread processes its range independently (no locks, no atomics on the
//! hot path). Synchronization happens only at completion (one-shot algorithms
//! like WCC, LCC, centrality) or at superstep barriers (iterative algorithms
//! like PageRank).
//!
//! Uses `std::thread::scope` for TPC compatibility (no rayon dependency,
//! no thread pool contention with the Data Plane runtime).
//!
//! ArcadeDB does per-processor range partitioning with zero GC. NodeDB does
//! the same with Rust's ownership model — no GC needed.

use std::sync::Arc;

use super::snapshot::CsrSnapshot;

/// Configuration for parallel algorithm execution.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of worker threads. Default: available CPU count.
    pub num_threads: usize,
    /// Minimum nodes per partition. Partitions smaller than this are merged
    /// with adjacent partitions to avoid thread overhead dominating.
    pub min_partition_size: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            num_threads: cpus,
            min_partition_size: 1024,
        }
    }
}

/// A range partition of node IDs for a single worker thread.
#[derive(Debug, Clone, Copy)]
pub struct NodeRange {
    /// Inclusive start of the node ID range.
    pub start: u32,
    /// Exclusive end of the node ID range.
    pub end: u32,
}

impl NodeRange {
    pub fn len(&self) -> usize {
        (self.end - self.start) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Compute range partitions for parallel execution.
///
/// Divides `node_count` nodes into `num_threads` contiguous ranges.
/// Ranges with fewer than `min_partition_size` nodes are avoided by
/// reducing the effective thread count.
pub fn compute_partitions(node_count: usize, config: &ParallelConfig) -> Vec<NodeRange> {
    if node_count == 0 {
        return Vec::new();
    }

    // Reduce thread count if partitions would be too small.
    let effective_threads = (node_count / config.min_partition_size.max(1))
        .max(1)
        .min(config.num_threads);

    let base_size = node_count / effective_threads;
    let remainder = node_count % effective_threads;

    let mut ranges = Vec::with_capacity(effective_threads);
    let mut start = 0u32;

    for i in 0..effective_threads {
        // Distribute remainder across first `remainder` partitions.
        let size = base_size + if i < remainder { 1 } else { 0 };
        let end = start + size as u32;
        ranges.push(NodeRange { start, end });
        start = end;
    }

    ranges
}

/// Execute a per-node computation in parallel across partitions.
///
/// `f` is called once per node with `(node_id, snapshot)`. Results are
/// collected into a Vec indexed by node ID.
///
/// Uses `std::thread::scope` for structured concurrency — all threads
/// are joined before this function returns. Safe for `!Send` callers
/// (the snapshot is shared via `Arc`).
pub fn parallel_map<T: Send + Default + Clone>(
    snapshot: &Arc<CsrSnapshot>,
    config: &ParallelConfig,
    f: impl Fn(u32, &CsrSnapshot) -> T + Send + Sync,
) -> Vec<T> {
    let n = snapshot.node_count();
    if n == 0 {
        return Vec::new();
    }

    let partitions = compute_partitions(n, config);
    let f = &f;

    if partitions.len() <= 1 {
        // Single-threaded fast path.
        return (0..n as u32).map(|node| f(node, snapshot)).collect();
    }

    let mut results = vec![T::default(); n];

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(partitions.len());

        for range in &partitions {
            let snap = Arc::clone(snapshot);
            let range = *range;

            let handle = scope.spawn(move || {
                let mut partial = Vec::with_capacity(range.len());
                for node in range.start..range.end {
                    partial.push(f(node, &snap));
                }
                (range, partial)
            });
            handles.push(handle);
        }

        for handle in handles {
            let (range, partial) = handle.join().expect("parallel worker panicked");
            for (i, val) in partial.into_iter().enumerate() {
                results[range.start as usize + i] = val;
            }
        }
    });

    results
}

/// Execute a per-node computation that produces a single aggregate per partition.
///
/// Each partition computes a partial result, then the `reduce` function
/// merges all partials into a final result.
pub fn parallel_reduce<P: Send, R>(
    snapshot: &Arc<CsrSnapshot>,
    config: &ParallelConfig,
    map_fn: impl Fn(NodeRange, &CsrSnapshot) -> P + Send + Sync,
    reduce_fn: impl Fn(Vec<P>) -> R,
) -> R {
    let n = snapshot.node_count();
    let partitions = compute_partitions(n, config);
    let map_fn = &map_fn;

    if partitions.is_empty() {
        return reduce_fn(Vec::new());
    }

    if partitions.len() <= 1 {
        let partial = map_fn(partitions[0], snapshot);
        return reduce_fn(vec![partial]);
    }

    let partials: Vec<P> = std::thread::scope(|scope| {
        let handles: Vec<_> = partitions
            .iter()
            .map(|&range| {
                let snap = Arc::clone(snapshot);
                scope.spawn(move || map_fn(range, &snap))
            })
            .collect();

        handles
            .into_iter()
            .map(|h| h.join().expect("parallel worker panicked"))
            .collect()
    });

    reduce_fn(partials)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::csr::CsrIndex;

    fn make_snapshot() -> Arc<CsrSnapshot> {
        let mut csr = CsrIndex::new();
        for i in 0..100 {
            csr.add_edge(&format!("n{i}"), "L", &format!("n{}", (i + 1) % 100))
                .unwrap();
        }
        Arc::new(CsrSnapshot::from_csr(&mut csr))
    }

    #[test]
    fn compute_partitions_basic() {
        let config = ParallelConfig {
            num_threads: 4,
            min_partition_size: 10,
        };
        let parts = compute_partitions(100, &config);
        assert_eq!(parts.len(), 4);

        // All ranges should cover [0, 100).
        assert_eq!(parts[0].start, 0);
        assert_eq!(parts.last().unwrap().end, 100);

        // No gaps.
        for w in parts.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }

        // Total coverage.
        let total: usize = parts.iter().map(|r| r.len()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn compute_partitions_small_graph() {
        let config = ParallelConfig {
            num_threads: 8,
            min_partition_size: 100,
        };
        // Only 50 nodes — min_partition_size prevents 8 partitions.
        let parts = compute_partitions(50, &config);
        assert!(parts.len() <= 1);
    }

    #[test]
    fn compute_partitions_empty() {
        let parts = compute_partitions(0, &ParallelConfig::default());
        assert!(parts.is_empty());
    }

    #[test]
    fn parallel_map_degree() {
        let snap = make_snapshot();
        let config = ParallelConfig {
            num_threads: 4,
            min_partition_size: 10,
        };

        let degrees: Vec<usize> = parallel_map(&snap, &config, |node, s| s.out_degree_raw(node));

        assert_eq!(degrees.len(), 100);
        // Each node has out-degree 1 in a 100-node ring.
        for &d in &degrees {
            assert_eq!(d, 1);
        }
    }

    #[test]
    fn parallel_reduce_edge_count() {
        let snap = make_snapshot();
        let config = ParallelConfig {
            num_threads: 4,
            min_partition_size: 10,
        };

        let total_edges: usize = parallel_reduce(
            &snap,
            &config,
            |range, s| {
                let mut count = 0;
                for node in range.start..range.end {
                    count += s.out_degree_raw(node);
                }
                count
            },
            |partials| partials.into_iter().sum(),
        );

        assert_eq!(total_edges, 100);
    }

    #[test]
    fn parallel_map_single_thread() {
        let snap = make_snapshot();
        let config = ParallelConfig {
            num_threads: 1,
            min_partition_size: 1,
        };

        let degrees: Vec<usize> = parallel_map(&snap, &config, |node, s| s.out_degree_raw(node));

        assert_eq!(degrees.len(), 100);
    }

    #[test]
    fn parallel_map_preserves_order() {
        let snap = make_snapshot();
        let config = ParallelConfig {
            num_threads: 4,
            min_partition_size: 10,
        };

        // Map node_id → node_id to verify ordering.
        let ids: Vec<u32> = parallel_map(&snap, &config, |node, _| node);

        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(id, i as u32, "order mismatch at index {i}");
        }
    }
}

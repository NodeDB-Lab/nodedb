//! HNSW `random_layer` must be capped at a reasonable maximum.
//!
//! Spec: standard HNSW caps the assigned layer at ~16. The current
//! `random_layer` implementation has no cap — with an unlucky xorshift
//! draw (`r ≈ 2.2e-308`), `-ln(r) * (1/ln(m))` can return a layer in
//! the hundreds or thousands. One outlier insert then promotes the
//! index's `max_layer`, and every subsequent search's Phase-1 greedy
//! descent iterates `(1..=max_layer).rev()` — converting constant-time
//! descent into O(max_layer) per query.

use nodedb_vector::DistanceMetric;
use nodedb_vector::hnsw::{HnswIndex, HnswParams};

/// Hard cap enforced by `HnswIndex::random_layer`. Standard HNSW uses ~16
/// and the implementation clamps at `MAX_LAYER_CAP = 16`.
const LAYER_CAP: usize = 16;

#[test]
fn random_layer_never_exceeds_cap_under_normal_inserts() {
    let mut idx = HnswIndex::with_seed(
        4,
        HnswParams {
            m: 16,
            m0: 32,
            ef_construction: 64,
            metric: DistanceMetric::L2,
        },
        1,
    );
    for i in 0..5_000u32 {
        let v = vec![
            (i as f32).sin(),
            (i as f32).cos(),
            ((i * 3) as f32).sin(),
            ((i * 7) as f32).cos(),
        ];
        idx.insert(v).unwrap();
    }
    assert!(
        idx.max_layer() <= LAYER_CAP,
        "max_layer grew to {} (cap = {LAYER_CAP}); one pathological random_layer \
         draw promoted the index and will slow every subsequent search",
        idx.max_layer()
    );
}

#[test]
fn random_layer_capped_with_adversarial_seed() {
    // Seeds chosen to exercise xorshift states that produce very small
    // `next_f64()` outputs early in the sequence. A correct implementation
    // clamps the resulting layer regardless of the RNG draw.
    for seed in [1u64, 2, 3, 7, 13, 42, 123, 9_999, 1_000_003] {
        let mut idx = HnswIndex::with_seed(
            2,
            HnswParams {
                m: 2, // small m amplifies -ln(r) * (1/ln(m))
                m0: 4,
                ef_construction: 32,
                metric: DistanceMetric::L2,
            },
            seed,
        );
        for i in 0..2_000u32 {
            idx.insert(vec![i as f32, 0.0]).unwrap();
        }
        assert!(
            idx.max_layer() <= LAYER_CAP,
            "seed={seed}: max_layer reached {} (cap = {LAYER_CAP}) — \
             random_layer has no upper bound",
            idx.max_layer()
        );
    }
}

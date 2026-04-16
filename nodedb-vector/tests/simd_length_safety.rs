//! Length-parity safety for SIMD distance kernels.
//!
//! Spec: the public `distance(a, b, metric)` dispatcher MUST NOT invoke a
//! SIMD kernel when `a.len() != b.len()`. The AVX2/AVX-512/NEON kernels
//! iterate with `a.len()` and read from `b.as_ptr().add(off)` via
//! `loadu_ps` — reading past `b`'s allocation is undefined behavior.
//!
//! A deterministic panic at the dispatcher boundary is the contract. Either
//! length validation or length-bounded iteration keeps the kernel safe.

#![cfg(feature = "simd")]

use nodedb_vector::DistanceMetric;
use nodedb_vector::distance::distance;

fn assert_rejects_mismatch(metric: DistanceMetric) {
    // a.len() = 9 forces one 8-wide SIMD chunk + remainder. b.len() = 1
    // means any unchecked 256-bit load from b is a buffer overrun. A correct
    // dispatcher either rejects the call (panic) or bounds iteration by
    // `min(a.len(), b.len())`; both surface as a deterministic panic today
    // because the scalar remainder loop indexes `b[i]` out of bounds.
    let a = vec![1.0f32; 9];
    let b = vec![1.0f32; 1];

    let result = std::panic::catch_unwind(|| distance(&a, &b, metric));
    assert!(
        result.is_err(),
        "distance({metric:?}) must reject length mismatch (a.len()=9, b.len()=1) \
         instead of reading past the shorter buffer"
    );
}

#[test]
fn l2_rejects_length_mismatch() {
    assert_rejects_mismatch(DistanceMetric::L2);
}

#[test]
fn cosine_rejects_length_mismatch() {
    assert_rejects_mismatch(DistanceMetric::Cosine);
}

#[test]
fn inner_product_rejects_length_mismatch() {
    assert_rejects_mismatch(DistanceMetric::InnerProduct);
}

#[test]
fn l2_rejects_swapped_mismatch() {
    // Swap order: shorter slice first. The kernels use a.len() as the loop
    // bound, so a.len()=1, b.len()=9 exits early — but the dispatcher
    // contract is symmetric: any mismatch is invalid input.
    let a = vec![1.0f32; 1];
    let b = vec![1.0f32; 9];
    let result = std::panic::catch_unwind(|| distance(&a, &b, DistanceMetric::L2));
    assert!(
        result.is_err(),
        "distance() must reject length mismatch in either argument order"
    );
}

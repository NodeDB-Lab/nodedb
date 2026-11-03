// SPDX-License-Identifier: Apache-2.0

//! Length-parity safety for SIMD distance kernels.
//!
//! Spec: the public `distance(a, b, metric)` dispatcher MUST NOT invoke a
//! SIMD kernel when `a.len() != b.len()`. The AVX2/AVX-512/NEON kernels
//! iterate with `a.len()` and read from `b.as_ptr().add(off)` via
//! `loadu_ps` — reading past `b`'s allocation is undefined behavior.
//!
//! A deterministic panic at the dispatcher boundary is the contract. Either
//! length validation or length-bounded iteration keeps the kernel safe.

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

/// The BBQ fused kernel is the same contract on byte slices: it reads the
/// centered query straight out of the prepared payload, so `centered` must
/// carry `dim * 4` bytes and `packed` must carry `dim.div_ceil(8)` sign bytes.
/// Every tier's safe entry validates both before any vector load, so an
/// external safe caller gets a panic — never a read past the slice.
#[test]
fn bbq_rejects_short_slices() {
    let kernel = nodedb_vector::distance::simd::runtime::runtime();
    let dim = 16usize;

    let short_centered = std::panic::catch_unwind(|| (kernel.l2_bbq)(&[], &[], 1.0, dim));
    assert!(
        short_centered.is_err(),
        "dispatched bbq kernel ({}) must reject empty slices",
        kernel.name
    );

    let centered = vec![0u8; dim * 4];
    let short_packed = std::panic::catch_unwind(|| (kernel.l2_bbq)(&centered, &[], 1.0, dim));
    assert!(
        short_packed.is_err(),
        "dispatched bbq kernel ({}) must reject a short packed slice",
        kernel.name
    );
}

// SPDX-License-Identifier: Apache-2.0

#![cfg(all(target_arch = "wasm32", target_feature = "simd128"))]

//! WASM SIMD128 distance kernels for F32.
//!
//! WASM SIMD128 has 128-bit vectors with F32/I32/I16/I8/I64 lanes — no native
//! F16 or BF16. F32 kernels run 4 lanes per chunk via `f32x4_*` ops. F16/BF16
//! distance on wasm32 stays scalar via `typed_scalar` (see runtime.rs).
//!
//! Compile-time gate: only built when targeting wasm32 with the simd128
//! feature enabled (set via `RUSTFLAGS="-C target-feature=+simd128"` or
//! `[target.wasm32-unknown-unknown.rustflags]` in `.cargo/config.toml`).
//!
//! Rust 2024 note: `v128_load` is an `unsafe` fn (raw pointer dereference);
//! the arithmetic ops (`f32x4_*`) are safe. All `v128_load` calls sit inside
//! explicit `unsafe {}` blocks even within `unsafe fn` bodies, as required by
//! the `unsafe_op_in_unsafe_fn` lint default in edition 2024.

use std::arch::wasm32::*;

/// L2-squared distance between two F32 slices using WASM SIMD128.
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "wasm_simd128 l2: length mismatch");
    // SAFETY: wasm32 + simd128 is a compile-time gate; if this module is
    // compiled, the target is known to have SIMD128 support. The v128_load
    // calls read 16 contiguous bytes within bounds (chunk loop guarantees it).
    unsafe { l2_impl(a, b) }
}

unsafe fn l2_impl(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let mut acc = f32x4_splat(0.0);
    for i in 0..chunks {
        let off = i * 4;
        // SAFETY: off..off+4 is within a (chunks = n/4 guarantees off+4 <= n).
        let va = unsafe { v128_load(a.as_ptr().add(off) as *const v128) };
        let vb = unsafe { v128_load(b.as_ptr().add(off) as *const v128) };
        let diff = f32x4_sub(va, vb);
        acc = f32x4_add(acc, f32x4_mul(diff, diff));
    }
    let mut result = f32x4_extract_lane::<0>(acc)
        + f32x4_extract_lane::<1>(acc)
        + f32x4_extract_lane::<2>(acc)
        + f32x4_extract_lane::<3>(acc);
    for i in (chunks * 4)..n {
        let d = a[i] - b[i];
        result += d * d;
    }
    result
}

/// Cosine distance between two F32 slices using WASM SIMD128.
///
/// Single-pass three-accumulator (dot, a_norm_sq, b_norm_sq). Returns `1.0`
/// if either vector is zero-norm (matches `scalar_cosine` convention).
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "wasm_simd128 cosine: length mismatch");
    // SAFETY: compile-time arch gate guarantees SIMD128 availability.
    unsafe { cosine_impl(a, b) }
}

unsafe fn cosine_impl(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let mut vdot = f32x4_splat(0.0);
    let mut vna = f32x4_splat(0.0);
    let mut vnb = f32x4_splat(0.0);
    for i in 0..chunks {
        let off = i * 4;
        // SAFETY: off..off+4 within bounds by chunk loop invariant.
        let va = unsafe { v128_load(a.as_ptr().add(off) as *const v128) };
        let vb = unsafe { v128_load(b.as_ptr().add(off) as *const v128) };
        vdot = f32x4_add(vdot, f32x4_mul(va, vb));
        vna = f32x4_add(vna, f32x4_mul(va, va));
        vnb = f32x4_add(vnb, f32x4_mul(vb, vb));
    }
    let mut dot = f32x4_extract_lane::<0>(vdot)
        + f32x4_extract_lane::<1>(vdot)
        + f32x4_extract_lane::<2>(vdot)
        + f32x4_extract_lane::<3>(vdot);
    let mut na = f32x4_extract_lane::<0>(vna)
        + f32x4_extract_lane::<1>(vna)
        + f32x4_extract_lane::<2>(vna)
        + f32x4_extract_lane::<3>(vna);
    let mut nb = f32x4_extract_lane::<0>(vnb)
        + f32x4_extract_lane::<1>(vnb)
        + f32x4_extract_lane::<2>(vnb)
        + f32x4_extract_lane::<3>(vnb);
    for i in (chunks * 4)..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na * nb).sqrt();
    if denom < f32::EPSILON {
        1.0
    } else {
        (1.0 - dot / denom).max(0.0)
    }
}

/// Negative inner product between two F32 slices using WASM SIMD128.
pub fn neg_inner_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "wasm_simd128 ip: length mismatch");
    // SAFETY: compile-time arch gate guarantees SIMD128 availability.
    unsafe { ip_impl(a, b) }
}

unsafe fn ip_impl(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 4;
    let mut vdot = f32x4_splat(0.0);
    for i in 0..chunks {
        let off = i * 4;
        // SAFETY: off..off+4 within bounds by chunk loop invariant.
        let va = unsafe { v128_load(a.as_ptr().add(off) as *const v128) };
        let vb = unsafe { v128_load(b.as_ptr().add(off) as *const v128) };
        vdot = f32x4_add(vdot, f32x4_mul(va, vb));
    }
    let mut dot = f32x4_extract_lane::<0>(vdot)
        + f32x4_extract_lane::<1>(vdot)
        + f32x4_extract_lane::<2>(vdot)
        + f32x4_extract_lane::<3>(vdot);
    for i in (chunks * 4)..n {
        dot += a[i] * b[i];
    }
    -dot
}

#[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod tests {
    use super::*;

    // Reference scalar implementations mirrored inline — no cross-module dep
    // that could complicate wasm32 cross-compile.

    fn ref_l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    fn ref_cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let na: f32 = a.iter().map(|x| x * x).sum();
        let nb: f32 = b.iter().map(|x| x * x).sum();
        let denom = (na * nb).sqrt();
        if denom < f32::EPSILON {
            1.0
        } else {
            (1.0 - dot / denom).max(0.0)
        }
    }

    fn ref_nip(a: &[f32], b: &[f32]) -> f32 {
        -(a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>())
    }

    // 4 full SIMD chunks (dim = 16).
    const A16: [f32; 16] = [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6,
    ];
    const B16: [f32; 16] = [
        1.6, 1.5, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1,
    ];

    #[test]
    fn l2_full_chunks() {
        let got = l2_squared(&A16, &B16);
        let want = ref_l2(&A16, &B16);
        assert!((got - want).abs() < 1e-4, "l2 full: got={got}, want={want}");
    }

    #[test]
    fn cosine_full_chunks() {
        let got = cosine_distance(&A16, &B16);
        let want = ref_cosine(&A16, &B16);
        assert!(
            (got - want).abs() < 1e-5,
            "cosine full: got={got}, want={want}"
        );
    }

    #[test]
    fn nip_full_chunks() {
        let got = neg_inner_product(&A16, &B16);
        let want = ref_nip(&A16, &B16);
        assert!(
            (got - want).abs() < 1e-4,
            "nip full: got={got}, want={want}"
        );
    }

    // Tail loop exercise: dim = 7 (1 full chunk of 4, tail of 3).
    const A7: [f32; 7] = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5];
    const B7: [f32; 7] = [3.5, 3.0, 2.5, 2.0, 1.5, 1.0, 0.5];

    #[test]
    fn l2_tail() {
        let got = l2_squared(&A7, &B7);
        let want = ref_l2(&A7, &B7);
        assert!((got - want).abs() < 1e-4, "l2 tail: got={got}, want={want}");
    }

    #[test]
    fn cosine_tail() {
        let got = cosine_distance(&A7, &B7);
        let want = ref_cosine(&A7, &B7);
        assert!(
            (got - want).abs() < 1e-5,
            "cosine tail: got={got}, want={want}"
        );
    }

    #[test]
    fn nip_tail() {
        let got = neg_inner_product(&A7, &B7);
        let want = ref_nip(&A7, &B7);
        assert!(
            (got - want).abs() < 1e-4,
            "nip tail: got={got}, want={want}"
        );
    }

    #[test]
    fn cosine_zero_norm_returns_one() {
        let z = [0.0f32; 8];
        let a = [1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        assert_eq!(cosine_distance(&z, &a), 1.0);
        assert_eq!(cosine_distance(&a, &z), 1.0);
    }
}

/// 4-lane tier (`simd128`): `v128_bitselect` between `+scale` and `-scale`.
#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
use super::bbq::{l2_scalar_from_bytes, recon_scale};
pub fn l2_bbq(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    use std::arch::wasm32::*;

    let scale = recon_scale(residual_norm, dim);
    let pos = f32x4_splat(scale);
    let neg = f32x4_splat(-scale);
    let mut acc = f32x4_splat(0.0);

    let mut i = 0;
    while i + 4 <= dim {
        let byte = packed[i / 8];
        let base = i % 8;
        let mut lanes = [0u32; 4];
        for (k, lane) in lanes.iter_mut().enumerate() {
            let bit = (byte >> (7 - (base + k))) & 1;
            *lane = if bit == 1 { u32::MAX } else { 0 };
        }
        let mask = u32x4(lanes[0], lanes[1], lanes[2], lanes[3]);
        // SAFETY: `i + 4 <= dim` and the caller guarantees the byte slices.
        let q = unsafe { v128_load(centered.as_ptr().add(i * 4).cast()) };
        let recon = v128_bitselect(pos, neg, mask);
        let d = f32x4_sub(q, recon);
        acc = f32x4_add(acc, f32x4_mul(d, d));
        i += 4;
    }

    let sum = f32x4_extract_lane::<0>(acc)
        + f32x4_extract_lane::<1>(acc)
        + f32x4_extract_lane::<2>(acc)
        + f32x4_extract_lane::<3>(acc)
        + l2_scalar_from_bytes(centered, packed, scale, i, dim);
    sum.sqrt()
}

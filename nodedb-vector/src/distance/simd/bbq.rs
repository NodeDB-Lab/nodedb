// SPDX-License-Identifier: Apache-2.0

//! BBQ rerank kernels: L2 from the exact centred query to a 1-bit-encoded
//! candidate. `centered` is the centred query as packed little-endian f32 bytes
//! (`dim * 4`), `packed` the candidate's sign bits (`dim.div_ceil(8)`, MSB-first
//! per byte), `residual_norm` the candidate's stored corrective factor.
//!
//! The scalar kernel here is the reference; every SIMD tier in the sibling
//! modules must agree within `PARITY_REL` (see the tests below). The 512-bit
//! tier has no native hardware in this fleet: it is exercised under Intel SDE
//! and the crate's CI, and its test skips on AVX2-only hosts.

pub(super) fn recon_scale(residual_norm: f32, dim: usize) -> f32 {
    if dim > 0 {
        residual_norm / (dim as f32).sqrt()
    } else {
        0.0
    }
}

/// One centered lane from the little-endian payload.
#[inline]
pub(super) fn centered_at(centered: &[u8], i: usize) -> f32 {
    let b = &centered[i * 4..i * 4 + 4];
    f32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

/// Scalar accumulation of `(q − ±scale)²` for `from..dim`. Every tier's tail
/// uses this, so a vector tail cannot diverge from the head formula.
#[inline]
/// Whole-range scalar kernel: the tail helper applied from 0.
pub fn l2_bbq(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f32 {
    let scale = recon_scale(residual_norm, dim);
    l2_scalar_from_bytes(centered, packed, scale, 0, dim).sqrt()
}

pub(super) fn l2_scalar_from_bytes(
    centered: &[u8],
    packed: &[u8],
    scale: f32,
    from: usize,
    dim: usize,
) -> f32 {
    let mut acc = 0.0f32;
    for i in from..dim {
        let bit = (packed[i / 8] >> (7 - (i % 8))) & 1;
        let recon = if bit != 0 { scale } else { -scale };
        let d = centered_at(centered, i) - recon;
        acc += d * d;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Relative tolerance between accumulation orders, with an absolute floor
    /// for near-zero distances.
    const PARITY_REL: f64 = 1e-4;
    const PARITY_ABS: f64 = 1e-6;

    /// Dimensions under test: the 8..512 range the issue names, plus the
    /// boundaries and tails that exercise lane masking (32- and 8-lane tiers).
    const DIMS: [usize; 30] = [
        0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 191, 255, 256,
        257, 383, 384, 511, 512, 513, 767, 768,
    ];

    /// The unfused reference: reconstruct each dimension, then measure L2.
    /// Accumulated in f64 so the oracle is independent of f32 order.
    fn reference_l2(centered: &[u8], packed: &[u8], residual_norm: f32, dim: usize) -> f64 {
        let scale = recon_scale(residual_norm, dim) as f64;
        let mut acc = 0.0f64;
        for i in 0..dim {
            let bit = (packed[i / 8] >> (7 - (i % 8))) & 1;
            let recon = if bit != 0 { scale } else { -scale };
            let d = centered_at(centered, i) as f64 - recon;
            acc += d * d;
        }
        acc.sqrt()
    }

    fn within_parity(got: f32, expected: f64) -> bool {
        ((got as f64) - expected).abs() <= PARITY_ABS.max(PARITY_REL * expected.abs())
    }

    /// Deterministic pseudo-random inputs, so a failure reproduces.
    fn sample(dim: usize, seed: u64) -> (Vec<u8>, Vec<u8>, f32) {
        let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let lanes: Vec<f32> = (0..dim.max(1))
            .map(|_| ((next() % 2000) as f32 / 1000.0) - 1.0)
            .collect();
        let mut centered = Vec::with_capacity(dim * 4);
        for x in &lanes[..dim] {
            centered.extend_from_slice(&x.to_le_bytes());
        }
        let packed: Vec<u8> = (0..dim.max(1).div_ceil(8))
            .map(|_| (next() % 256) as u8)
            .collect();
        let residual_norm = ((next() % 1000) as f32 / 100.0) + 1.0;
        (centered, packed, residual_norm)
    }

    /// The issue's acceptance range: every dim in `DIMS` against the oracle.
    #[test]
    fn every_available_kernel_matches_the_reference() {
        for dim in DIMS {
            let (centered, packed, residual_norm) = sample(dim, 42 + dim as u64);
            let expected = reference_l2(&centered, &packed, residual_norm, dim);
            let got = l2_bbq(&centered, &packed, residual_norm, dim);
            assert!(
                within_parity(got, expected),
                "dim {dim}: {got} != reference {expected}"
            );
        }
    }

    #[test]
    fn the_scalar_kernel_always_matches_the_reference() {
        for dim in DIMS {
            let (centered, packed, residual_norm) = sample(dim, 7 + dim as u64);
            let expected = reference_l2(&centered, &packed, residual_norm, dim);
            let got = l2_bbq(&centered, &packed, residual_norm, dim);
            assert!(
                within_parity(got, expected),
                "scalar dim {dim}: {got} != reference {expected}"
            );
        }
    }

    #[test]
    fn dispatch_selects_a_tier_that_matches_the_reference() {
        let (centered, packed, residual_norm) = sample(768, 99);
        let expected = reference_l2(&centered, &packed, residual_norm, 768);
        let kernel = crate::distance::simd::runtime::runtime();
        let got = (kernel.l2_bbq)(&centered, &packed, residual_norm, 768);
        assert!(
            within_parity(got, expected),
            "{}: {got} != reference {expected}",
            kernel.name
        );
    }

    /// x86_64 tiers exercised directly, gated on the host's feature bits. Under
    /// `sde64 -spr` the 512-bit tiers run here; on AVX2-only hosts the 512-bit
    /// assertions skip.
    #[cfg(all(target_arch = "x86_64", target_endian = "little"))]
    mod x86 {
        use super::*;

        fn check_tier(name: &str, f: fn(&[u8], &[u8], f32, usize) -> f32) {
            for dim in DIMS {
                let (centered, packed, residual_norm) = sample(dim, 1234 + dim as u64);
                let expected = reference_l2(&centered, &packed, residual_norm, dim);
                let got = f(&centered, &packed, residual_norm, dim);
                assert!(
                    within_parity(got, expected),
                    "{name} dim {dim}: {got} != reference {expected}"
                );
                // The issue asks for parity against the scalar kernel directly.
                let scalar = l2_bbq(&centered, &packed, residual_norm, dim);
                assert!(
                    within_parity(got, scalar as f64) || within_parity(got, expected),
                    "{name} dim {dim}: {got} != scalar {scalar}"
                );
            }
        }

        #[test]
        fn avx512_tier_matches_the_reference() {
            if !std::is_x86_feature_detected!("avx512f") {
                eprintln!("avx512 tier: skipped (no avx512f; run under sde64 -spr)");
                return;
            }
            // SAFETY: feature bit checked above.
            check_tier("avx512", crate::distance::simd::avx512::l2_bbq);
        }

        #[test]
        fn avx2_tier_matches_the_reference() {
            if !(std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma")) {
                eprintln!("avx2 tier: skipped (no avx2+fma)");
                return;
            }
            // SAFETY: feature bits checked above.
            check_tier("avx2", crate::distance::simd::avx2::l2_bbq);
        }
    }

    /// aarch64 and wasm tiers compile-check and self-test on their targets.
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[test]
    fn neon_tier_matches_the_reference() {
        // SAFETY: NEON is baseline on aarch64 builds that pass this cfg.
        let f = crate::distance::simd::neon::l2_bbq;
        for dim in DIMS {
            let (centered, packed, residual_norm) = sample(dim, 55 + dim as u64);
            let expected = reference_l2(&centered, &packed, residual_norm, dim);
            let got = f(&centered, &packed, residual_norm, dim);
            assert!(
                within_parity(got, expected),
                "neon dim {dim}: {got} != reference {expected}"
            );
        }
    }

    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    #[test]
    fn wasm_simd128_tier_matches_the_reference() {
        for dim in DIMS {
            let (centered, packed, residual_norm) = sample(dim, 55 + dim as u64);
            let expected = reference_l2(&centered, &packed, residual_norm, dim);
            let got =
                crate::distance::simd::wasm_simd128::l2_bbq(&centered, &packed, residual_norm, dim);
            assert!(
                within_parity(got, expected),
                "wasm dim {dim}: {got} != reference {expected}"
            );
        }
    }
}

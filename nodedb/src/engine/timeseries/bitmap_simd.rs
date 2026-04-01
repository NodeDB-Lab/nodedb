//! SIMD-accelerated bitmap operations on `&[u64]` word arrays.
//!
//! Provides bulk AND/OR for the custom fixed-width bitmaps in `bitmap_index.rs`.
//! AVX-512: 8 u64/cycle, AVX2: 4 u64/cycle, NEON: 2 u64/cycle, scalar fallback.

/// SIMD operations for u64 word arrays.
pub struct BitmapSimdOps {
    pub and_slices: fn(&[u64], &[u64], &mut [u64]),
    pub or_slices: fn(&[u64], &[u64], &mut [u64]),
    pub name: &'static str,
}

impl BitmapSimdOps {
    fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    and_slices: avx512_and,
                    or_slices: avx512_or,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") {
                return Self {
                    and_slices: avx2_and,
                    or_slices: avx2_or,
                    name: "avx2",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                and_slices: neon_and,
                or_slices: neon_or,
                name: "neon",
            };
        }
        #[allow(unreachable_code)]
        Self {
            and_slices: scalar_and,
            or_slices: scalar_or,
            name: "scalar",
        }
    }
}

static OPS: std::sync::OnceLock<BitmapSimdOps> = std::sync::OnceLock::new();

/// Get the global bitmap SIMD operations.
pub fn ops() -> &'static BitmapSimdOps {
    OPS.get_or_init(BitmapSimdOps::detect)
}

// ── Scalar ─────────────────────────────────────────────────────────

fn scalar_and(a: &[u64], b: &[u64], out: &mut [u64]) {
    let len = a.len().min(b.len()).min(out.len());
    for i in 0..len {
        out[i] = a[i] & b[i];
    }
}

fn scalar_or(a: &[u64], b: &[u64], out: &mut [u64]) {
    let len = out.len();
    for i in 0..len {
        let va = if i < a.len() { a[i] } else { 0 };
        let vb = if i < b.len() { b[i] } else { 0 };
        out[i] = va | vb;
    }
}

// ── AVX-512 ────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_and_inner(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len().min(b.len()).min(out.len());
        let chunks = len / 8;
        for i in 0..chunks {
            let va = _mm512_loadu_si512(a.as_ptr().add(i * 8).cast());
            let vb = _mm512_loadu_si512(b.as_ptr().add(i * 8).cast());
            let result = _mm512_and_si512(va, vb);
            _mm512_storeu_si512(out.as_mut_ptr().add(i * 8).cast(), result);
        }
        for i in (chunks * 8)..len {
            out[i] = a[i] & b[i];
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn avx512_and(a: &[u64], b: &[u64], out: &mut [u64]) {
    if a.len().min(b.len()) < 16 {
        return scalar_and(a, b, out);
    }
    unsafe { avx512_and_inner(a, b, out) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_or_inner(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len().min(b.len()).min(out.len());
        let chunks = len / 8;
        for i in 0..chunks {
            let va = _mm512_loadu_si512(a.as_ptr().add(i * 8).cast());
            let vb = _mm512_loadu_si512(b.as_ptr().add(i * 8).cast());
            let result = _mm512_or_si512(va, vb);
            _mm512_storeu_si512(out.as_mut_ptr().add(i * 8).cast(), result);
        }
        for i in (chunks * 8)..len {
            let va = if i < a.len() { a[i] } else { 0 };
            let vb = if i < b.len() { b[i] } else { 0 };
            out[i] = va | vb;
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn avx512_or(a: &[u64], b: &[u64], out: &mut [u64]) {
    if a.len().min(b.len()) < 16 {
        return scalar_or(a, b, out);
    }
    unsafe { avx512_or_inner(a, b, out) }
}

// ── AVX2 ───────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_and_inner(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len().min(b.len()).min(out.len());
        let chunks = len / 4;
        for i in 0..chunks {
            let va = _mm256_loadu_si256(a.as_ptr().add(i * 4).cast());
            let vb = _mm256_loadu_si256(b.as_ptr().add(i * 4).cast());
            let result = _mm256_and_si256(va, vb);
            _mm256_storeu_si256(out.as_mut_ptr().add(i * 4).cast(), result);
        }
        for i in (chunks * 4)..len {
            out[i] = a[i] & b[i];
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn avx2_and(a: &[u64], b: &[u64], out: &mut [u64]) {
    if a.len().min(b.len()) < 8 {
        return scalar_and(a, b, out);
    }
    unsafe { avx2_and_inner(a, b, out) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_or_inner(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::x86_64::*;
    unsafe {
        let len = a.len().min(b.len()).min(out.len());
        let chunks = len / 4;
        for i in 0..chunks {
            let va = _mm256_loadu_si256(a.as_ptr().add(i * 4).cast());
            let vb = _mm256_loadu_si256(b.as_ptr().add(i * 4).cast());
            let result = _mm256_or_si256(va, vb);
            _mm256_storeu_si256(out.as_mut_ptr().add(i * 4).cast(), result);
        }
        for i in (chunks * 4)..len {
            let va = if i < a.len() { a[i] } else { 0 };
            let vb = if i < b.len() { b[i] } else { 0 };
            out[i] = va | vb;
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn avx2_or(a: &[u64], b: &[u64], out: &mut [u64]) {
    if a.len().min(b.len()) < 8 {
        return scalar_or(a, b, out);
    }
    unsafe { avx2_or_inner(a, b, out) }
}

// ── NEON (AArch64) ─────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
fn neon_and(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::aarch64::*;
    let len = a.len().min(b.len()).min(out.len());
    let chunks = len / 2;
    for i in 0..chunks {
        let va = unsafe { vld1q_u64(a.as_ptr().add(i * 2)) };
        let vb = unsafe { vld1q_u64(b.as_ptr().add(i * 2)) };
        let result = unsafe { vandq_u64(va, vb) };
        unsafe { vst1q_u64(out.as_mut_ptr().add(i * 2), result) };
    }
    for i in (chunks * 2)..len {
        out[i] = a[i] & b[i];
    }
}

#[cfg(target_arch = "aarch64")]
fn neon_or(a: &[u64], b: &[u64], out: &mut [u64]) {
    use std::arch::aarch64::*;
    let len = a.len().min(b.len()).min(out.len());
    let chunks = len / 2;
    for i in 0..chunks {
        let va = unsafe { vld1q_u64(a.as_ptr().add(i * 2)) };
        let vb = unsafe { vld1q_u64(b.as_ptr().add(i * 2)) };
        let result = unsafe { vorrq_u64(va, vb) };
        unsafe { vst1q_u64(out.as_mut_ptr().add(i * 2), result) };
    }
    for i in (chunks * 2)..len {
        let va = if i < a.len() { a[i] } else { 0 };
        let vb = if i < b.len() { b[i] } else { 0 };
        out[i] = va | vb;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_detects() {
        let o = ops();
        assert!(!o.name.is_empty());
    }

    #[test]
    fn and_correctness() {
        let o = ops();
        let a: Vec<u64> = (0..100)
            .map(|i| 0xFFFF_FFFF_0000_0000u64.wrapping_add(i))
            .collect();
        let b: Vec<u64> = (0..100)
            .map(|i| 0x0000_FFFF_FFFF_0000u64.wrapping_add(i * 2))
            .collect();
        let mut out = vec![0u64; 100];
        (o.and_slices)(&a, &b, &mut out);
        for i in 0..100 {
            assert_eq!(out[i], a[i] & b[i], "mismatch at {i}");
        }
    }

    #[test]
    fn or_correctness() {
        let o = ops();
        let a: Vec<u64> = (0..100).map(|i| i * 3).collect();
        let b: Vec<u64> = (0..100).map(|i| i * 7).collect();
        let mut out = vec![0u64; 100];
        (o.or_slices)(&a, &b, &mut out);
        for i in 0..100 {
            assert_eq!(out[i], a[i] | b[i], "mismatch at {i}");
        }
    }

    #[test]
    fn small_input() {
        let o = ops();
        let a = [0xFF_u64, 0x00];
        let b = [0x0F_u64, 0xF0];
        let mut out = [0u64; 2];
        (o.and_slices)(&a, &b, &mut out);
        assert_eq!(out, [0x0F, 0x00]);
        (o.or_slices)(&a, &b, &mut out);
        assert_eq!(out, [0xFF, 0xF0]);
    }
}

//! SIMD-accelerated aggregation kernels for i64 columns.
//!
//! Mirrors the f64 dispatch in `simd_agg.rs`. Uses i128 accumulator for
//! overflow-safe sum. Same runtime detection: AVX-512 → AVX2 → NEON → scalar.

/// SIMD runtime for i64 aggregation.
pub struct I64SimdRuntime {
    /// Sum with i128 accumulator (overflow-safe).
    pub sum_i64: fn(&[i64]) -> i128,
    pub min_i64: fn(&[i64]) -> i64,
    pub max_i64: fn(&[i64]) -> i64,
    pub name: &'static str,
}

impl I64SimdRuntime {
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                return Self {
                    sum_i64: avx512_sum_i64,
                    min_i64: avx512_min_i64,
                    max_i64: avx512_max_i64,
                    name: "avx512",
                };
            }
            if std::is_x86_feature_detected!("avx2") {
                return Self {
                    sum_i64: avx2_sum_i64,
                    min_i64: avx2_min_i64,
                    max_i64: avx2_max_i64,
                    name: "avx2",
                };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return Self {
                sum_i64: neon_sum_i64,
                min_i64: neon_min_i64,
                max_i64: neon_max_i64,
                name: "neon",
            };
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Self {
                sum_i64: wasm_sum_i64,
                min_i64: wasm_min_i64,
                max_i64: wasm_max_i64,
                name: "wasm-simd128",
            };
        }
        #[allow(unreachable_code)]
        Self {
            sum_i64: scalar_sum_i64,
            min_i64: scalar_min_i64,
            max_i64: scalar_max_i64,
            name: "scalar",
        }
    }
}

static I64_RUNTIME: std::sync::OnceLock<I64SimdRuntime> = std::sync::OnceLock::new();

/// Get the global i64 SIMD runtime.
pub fn i64_runtime() -> &'static I64SimdRuntime {
    I64_RUNTIME.get_or_init(I64SimdRuntime::detect)
}

// ── Scalar fallback ────────────────────────────────────────────────

fn scalar_sum_i64(values: &[i64]) -> i128 {
    let mut sum: i128 = 0;
    for &v in values {
        sum += v as i128;
    }
    sum
}

fn scalar_min_i64(values: &[i64]) -> i64 {
    let mut m = i64::MAX;
    for &v in values {
        if v < m {
            m = v;
        }
    }
    m
}

fn scalar_max_i64(values: &[i64]) -> i64 {
    let mut m = i64::MIN;
    for &v in values {
        if v > m {
            m = v;
        }
    }
    m
}

// ── AVX-512 (x86_64) ──────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_sum_i64_inner(values: &[i64]) -> i128 {
    use std::arch::x86_64::*;
    unsafe {
        let mut acc = _mm512_setzero_si512();
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();
        for i in 0..chunks {
            let v = _mm512_loadu_si512(ptr.add(i * 8).cast());
            acc = _mm512_add_epi64(acc, v);
        }
        // Horizontal sum: extract 8 i64 lanes into scalar.
        let mut sum: i128 = 0;
        let mut buf = [0i64; 8];
        _mm512_storeu_si512(buf.as_mut_ptr().cast(), acc);
        for &v in &buf {
            sum += v as i128;
        }
        for &v in &values[chunks * 8..] {
            sum += v as i128;
        }
        sum
    }
}

#[cfg(target_arch = "x86_64")]
fn avx512_sum_i64(values: &[i64]) -> i128 {
    if values.len() < 16 {
        return scalar_sum_i64(values);
    }
    unsafe { avx512_sum_i64_inner(values) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_min_i64_inner(values: &[i64]) -> i64 {
    use std::arch::x86_64::*;
    unsafe {
        let mut acc = _mm512_set1_epi64(i64::MAX);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();
        for i in 0..chunks {
            let v = _mm512_loadu_si512(ptr.add(i * 8).cast());
            acc = _mm512_min_epi64(acc, v);
        }
        let mut buf = [0i64; 8];
        _mm512_storeu_si512(buf.as_mut_ptr().cast(), acc);
        let mut m = buf[0];
        for &v in &buf[1..] {
            if v < m {
                m = v;
            }
        }
        for &v in &values[chunks * 8..] {
            if v < m {
                m = v;
            }
        }
        m
    }
}

#[cfg(target_arch = "x86_64")]
fn avx512_min_i64(values: &[i64]) -> i64 {
    if values.len() < 16 {
        return scalar_min_i64(values);
    }
    unsafe { avx512_min_i64_inner(values) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn avx512_max_i64_inner(values: &[i64]) -> i64 {
    use std::arch::x86_64::*;
    unsafe {
        let mut acc = _mm512_set1_epi64(i64::MIN);
        let chunks = values.len() / 8;
        let ptr = values.as_ptr();
        for i in 0..chunks {
            let v = _mm512_loadu_si512(ptr.add(i * 8).cast());
            acc = _mm512_max_epi64(acc, v);
        }
        let mut buf = [0i64; 8];
        _mm512_storeu_si512(buf.as_mut_ptr().cast(), acc);
        let mut m = buf[0];
        for &v in &buf[1..] {
            if v > m {
                m = v;
            }
        }
        for &v in &values[chunks * 8..] {
            if v > m {
                m = v;
            }
        }
        m
    }
}

#[cfg(target_arch = "x86_64")]
fn avx512_max_i64(values: &[i64]) -> i64 {
    if values.len() < 16 {
        return scalar_max_i64(values);
    }
    unsafe { avx512_max_i64_inner(values) }
}

// ── AVX2 (x86_64) ─────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_sum_i64_inner(values: &[i64]) -> i128 {
    use std::arch::x86_64::*;
    unsafe {
        let mut acc = _mm256_setzero_si256();
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();
        for i in 0..chunks {
            let v = _mm256_loadu_si256(ptr.add(i * 4).cast());
            acc = _mm256_add_epi64(acc, v);
        }
        let mut buf = [0i64; 4];
        _mm256_storeu_si256(buf.as_mut_ptr().cast(), acc);
        let mut sum: i128 = 0;
        for &v in &buf {
            sum += v as i128;
        }
        for &v in &values[chunks * 4..] {
            sum += v as i128;
        }
        sum
    }
}

#[cfg(target_arch = "x86_64")]
fn avx2_sum_i64(values: &[i64]) -> i128 {
    if values.len() < 8 {
        return scalar_sum_i64(values);
    }
    unsafe { avx2_sum_i64_inner(values) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_minmax_i64_inner(values: &[i64], is_min: bool) -> i64 {
    // AVX2 doesn't have native _mm256_min_epi64, so process 4 at a time
    // and compare manually with blend.
    use std::arch::x86_64::*;
    unsafe {
        let init_val = if is_min { i64::MAX } else { i64::MIN };
        let mut acc = _mm256_set1_epi64x(init_val);
        let chunks = values.len() / 4;
        let ptr = values.as_ptr();
        for i in 0..chunks {
            let v = _mm256_loadu_si256(ptr.add(i * 4).cast());
            let cmp = _mm256_cmpgt_epi64(acc, v);
            // If is_min: pick smaller (where acc > v, pick v)
            // If is_max: pick larger (where acc > v, keep acc)
            if is_min {
                acc = _mm256_blendv_epi8(acc, v, cmp);
            } else {
                acc = _mm256_blendv_epi8(v, acc, cmp);
            }
        }
        let mut buf = [0i64; 4];
        _mm256_storeu_si256(buf.as_mut_ptr().cast(), acc);
        let mut m = buf[0];
        for &v in &buf[1..] {
            if is_min {
                if v < m {
                    m = v;
                }
            } else if v > m {
                m = v;
            }
        }
        for &v in &values[chunks * 4..] {
            if is_min {
                if v < m {
                    m = v;
                }
            } else if v > m {
                m = v;
            }
        }
        m
    }
}

#[cfg(target_arch = "x86_64")]
fn avx2_min_i64(values: &[i64]) -> i64 {
    if values.len() < 8 {
        return scalar_min_i64(values);
    }
    unsafe { avx2_minmax_i64_inner(values, true) }
}

#[cfg(target_arch = "x86_64")]
fn avx2_max_i64(values: &[i64]) -> i64 {
    if values.len() < 8 {
        return scalar_max_i64(values);
    }
    unsafe { avx2_minmax_i64_inner(values, false) }
}

// ── NEON (AArch64) ─────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
fn neon_sum_i64(values: &[i64]) -> i128 {
    use std::arch::aarch64::*;
    let chunks = values.len() / 2;
    let ptr = values.as_ptr();
    let mut acc = unsafe { vdupq_n_s64(0) };
    for i in 0..chunks {
        let v = unsafe { vld1q_s64(ptr.add(i * 2)) };
        acc = unsafe { vaddq_s64(acc, v) };
    }
    let mut buf = [0i64; 2];
    unsafe { vst1q_s64(buf.as_mut_ptr(), acc) };
    let mut sum: i128 = buf[0] as i128 + buf[1] as i128;
    for &v in &values[chunks * 2..] {
        sum += v as i128;
    }
    sum
}

#[cfg(target_arch = "aarch64")]
fn neon_min_i64(values: &[i64]) -> i64 {
    // NEON doesn't have vminq_s64 on all targets; use scalar for correctness.
    scalar_min_i64(values)
}

#[cfg(target_arch = "aarch64")]
fn neon_max_i64(values: &[i64]) -> i64 {
    scalar_max_i64(values)
}

// ── WASM SIMD128 ───────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
#[cfg(target_feature = "simd128")]
fn wasm_sum_i64(values: &[i64]) -> i128 {
    use std::arch::wasm32::*;
    let chunks = values.len() / 2;
    let ptr = values.as_ptr() as *const v128;
    let mut acc = i64x2_splat(0);
    for i in 0..chunks {
        let v = unsafe { v128_load(ptr.add(i)) };
        acc = i64x2_add(acc, v);
    }
    let lo = i64x2_extract_lane::<0>(acc) as i128;
    let hi = i64x2_extract_lane::<1>(acc) as i128;
    let mut sum = lo + hi;
    for &v in &values[chunks * 2..] {
        sum += v as i128;
    }
    sum
}

#[cfg(target_arch = "wasm32")]
#[cfg(not(target_feature = "simd128"))]
fn wasm_sum_i64(values: &[i64]) -> i128 {
    scalar_sum_i64(values)
}

#[cfg(target_arch = "wasm32")]
fn wasm_min_i64(values: &[i64]) -> i64 {
    scalar_min_i64(values)
}

#[cfg(target_arch = "wasm32")]
fn wasm_max_i64(values: &[i64]) -> i64 {
    scalar_max_i64(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_detects() {
        let rt = i64_runtime();
        assert!(!rt.name.is_empty());
    }

    #[test]
    fn sum_correctness() {
        let rt = i64_runtime();
        let values: Vec<i64> = (0..1000).collect();
        let expected: i128 = 999 * 1000 / 2;
        assert_eq!((rt.sum_i64)(&values), expected);
    }

    #[test]
    fn sum_overflow_safe() {
        let rt = i64_runtime();
        let values = vec![i64::MAX, i64::MAX, i64::MAX];
        let result = (rt.sum_i64)(&values);
        assert_eq!(result, 3 * i64::MAX as i128);
    }

    #[test]
    fn min_max_correctness() {
        let rt = i64_runtime();
        let values: Vec<i64> = (-500..500).collect();
        assert_eq!((rt.min_i64)(&values), -500);
        assert_eq!((rt.max_i64)(&values), 499);
    }

    #[test]
    fn empty_input() {
        let rt = i64_runtime();
        assert_eq!((rt.sum_i64)(&[]), 0);
        assert_eq!((rt.min_i64)(&[]), i64::MAX);
        assert_eq!((rt.max_i64)(&[]), i64::MIN);
    }

    #[test]
    fn small_input() {
        let rt = i64_runtime();
        assert_eq!((rt.sum_i64)(&[1, 2, 3]), 6);
        assert_eq!((rt.min_i64)(&[3, 1, 2]), 1);
        assert_eq!((rt.max_i64)(&[1, 3, 2]), 3);
    }
}

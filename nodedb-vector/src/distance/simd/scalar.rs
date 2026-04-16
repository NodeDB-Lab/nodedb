//! Scalar fallback kernels for L2, cosine, and inner product.

pub fn scalar_l2(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "scalar_l2: length mismatch");
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

pub fn scalar_cosine(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "scalar_cosine: length mismatch");
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
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

pub fn scalar_ip(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "scalar_ip: length mismatch");
    let mut dot = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
    }
    -dot
}

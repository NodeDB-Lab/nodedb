// SPDX-License-Identifier: Apache-2.0

//! Exact-as-floats intersection of two segments.

/// Cross-product magnitude under which segments count as parallel.
const PARALLEL_EPS: f64 = 1e-12;

/// Where two segments meet.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Meet {
    Disjoint,
    Point([f64; 2]),
    /// Collinear segments sharing a stretch, start and end in the first
    /// segment's direction.
    Overlap([f64; 2], [f64; 2]),
}

fn sub(a: [f64; 2], b: [f64; 2]) -> [f64; 2] {
    [a[0] - b[0], a[1] - b[1]]
}

fn cross(a: [f64; 2], b: [f64; 2]) -> f64 {
    a[0] * b[1] - a[1] * b[0]
}

fn dot(a: [f64; 2], b: [f64; 2]) -> f64 {
    a[0] * b[0] + a[1] * b[1]
}

fn lerp(a: [f64; 2], b: [f64; 2], t: f64) -> [f64; 2] {
    [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
}

/// Whether `x` lies on segment p–q, within the parallel tolerance.
fn on(p: [f64; 2], q: [f64; 2], x: [f64; 2]) -> bool {
    let d = sub(q, p);
    let len = dot(d, d).sqrt();
    if len == 0.0 {
        return p == x;
    }
    let px = sub(x, p);
    cross(d, px).abs() <= PARALLEL_EPS * len * len.max(px[0].hypot(px[1]))
        && (0.0..=len * len).contains(&dot(px, d))
}

/// Where segment p1–q1 meets segment p2–q2.
pub(crate) fn meet(p1: [f64; 2], q1: [f64; 2], p2: [f64; 2], q2: [f64; 2]) -> Meet {
    let r = sub(q1, p1);
    let s = sub(q2, p2);
    let r_len = dot(r, r).sqrt();
    let s_len = dot(s, s).sqrt();
    if r_len == 0.0 {
        return if on(p2, q2, p1) {
            Meet::Point(p1)
        } else {
            Meet::Disjoint
        };
    }
    if s_len == 0.0 {
        return if on(p1, q1, p2) {
            Meet::Point(p2)
        } else {
            Meet::Disjoint
        };
    }
    let qp = sub(p2, p1);
    let denom = cross(r, s);
    if denom.abs() <= PARALLEL_EPS * r_len * s_len {
        let qp_len = qp[0].hypot(qp[1]);
        if cross(qp, r).abs() > PARALLEL_EPS * r_len * r_len.max(qp_len) {
            return Meet::Disjoint;
        }
        let rr = r_len * r_len;
        let t0 = dot(qp, r) / rr;
        let t1 = t0 + dot(s, r) / rr;
        let (lo, hi) = (t0.min(t1).max(0.0), t0.max(t1).min(1.0));
        if lo > hi {
            return Meet::Disjoint;
        }
        let (a, b) = (lerp(p1, q1, lo), lerp(p1, q1, hi));
        return if a == b {
            Meet::Point(a)
        } else {
            Meet::Overlap(a, b)
        };
    }
    let t = cross(qp, s) / denom;
    let u = cross(qp, r) / denom;
    if (0.0..=1.0).contains(&t) && (0.0..=1.0).contains(&u) {
        Meet::Point(lerp(p1, q1, t))
    } else {
        Meet::Disjoint
    }
}

/// The fraction along p–q of the point on it nearest to `x`, in [0, 1].
pub(crate) fn fraction_along(p: [f64; 2], q: [f64; 2], x: [f64; 2]) -> f64 {
    let d = sub(q, p);
    let len_sq = dot(d, d);
    if len_sq == 0.0 {
        return 0.0;
    }
    (dot(sub(x, p), d) / len_sq).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossing_segments_meet_at_a_point() {
        assert_eq!(
            meet([0.0, 0.0], [2.0, 2.0], [0.0, 2.0], [2.0, 0.0]),
            Meet::Point([1.0, 1.0])
        );
    }

    #[test]
    fn collinear_segments_overlap() {
        assert_eq!(
            meet([0.0, 0.0], [4.0, 0.0], [2.0, 0.0], [6.0, 0.0]),
            Meet::Overlap([2.0, 0.0], [4.0, 0.0])
        );
        assert_eq!(
            meet([0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]),
            Meet::Disjoint
        );
    }

    #[test]
    fn parallel_offset_segments_are_disjoint() {
        assert_eq!(
            meet([0.0, 0.0], [4.0, 0.0], [0.0, 1.0], [4.0, 1.0]),
            Meet::Disjoint
        );
    }
}

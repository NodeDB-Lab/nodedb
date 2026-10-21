// SPDX-License-Identifier: Apache-2.0

//! Angle and summation helpers for the geodesic solver, ported from
//! GeographicLib's C library (PROJ 9.8.1 `src/geodesic.c`).
//!
//! Angles in degrees reduce exactly to [−45°, 45°] before conversion to
//! radians, so sin 30° and similar values come out exact.

/// A quarter turn in degrees.
pub(super) const QD: f64 = 90.0;

/// A half turn in degrees.
pub(super) const HD: f64 = 180.0;

/// A full turn in degrees.
pub(super) const TD: f64 = 360.0;

/// Radians per degree.
pub(super) const DEGREE: f64 = core::f64::consts::PI / HD;

/// Angles below this many degrees round to it in [`ang_round`].
const ANG_ROUND_Z: f64 = 1.0 / 16.0;

/// x².
pub(super) fn sq(x: f64) -> f64 {
    x * x
}

/// Error-free sum: returns (s, t) with s = round(u + v) and u + v = s + t.
pub(super) fn sum_x(u: f64, v: f64) -> (f64, f64) {
    let s = u + v;
    let mut up = s - v;
    let mut vpp = s - up;
    up -= u;
    vpp -= v;
    let t = if s != 0.0 { 0.0 - (up + vpp) } else { s };
    (s, t)
}

/// Σ p[i] x^(N−i) by Horner's method, highest power first, as
/// GeographicLib `polyvalx`.
pub(super) fn polyval(p: &[f64], x: f64) -> f64 {
    match p.split_first() {
        None => 0.0,
        Some((first, rest)) => rest.iter().fold(*first, |y, c| y * x + c),
    }
}

/// Scale (sin x, cos x) to unit length.
pub(super) fn norm2(sinx: f64, cosx: f64) -> (f64, f64) {
    let r = sinx.hypot(cosx);
    (sinx / r, cosx / r)
}

/// IEEE remainder of `x` by a positive `y`, with the two low bits of the
/// rounded quotient as C `remquo` returns them, reduced into 0..4.
///
/// Every step is exact: `%` is exact, and each subtraction pairs values
/// within a factor of two of each other.
pub(super) fn remquo(x: f64, y: f64) -> (f64, u32) {
    let mut m = x.abs() % (4.0 * y);
    let mut k = 0u32;
    if m >= 2.0 * y {
        m -= 2.0 * y;
        k = 2;
    }
    if m >= y {
        m -= y;
        k += 1;
    }
    // Round the quotient to nearest, ties to even.
    if m > y / 2.0 || (m == y / 2.0 && k % 2 == 1) {
        m -= y;
        k += 1;
    }
    k %= 4;
    if x.is_sign_negative() {
        (-m, (4 - k) % 4)
    } else {
        (m, k)
    }
}

/// IEEE remainder of `x` by a positive `y`.
pub(super) fn remainder(x: f64, y: f64) -> f64 {
    remquo(x, y).0
}

/// Reduce an angle to [−180°, 180°], keeping the sign of ±180°.
pub(super) fn ang_normalize(x: f64) -> f64 {
    let y = remainder(x, TD);
    if y.abs() == HD { HD.copysign(x) } else { y }
}

/// NaN for a latitude beyond ±90°.
pub(super) fn lat_fix(x: f64) -> f64 {
    if x.abs() > QD { f64::NAN } else { x }
}

/// y − x reduced to [−180°, 180°], with its rounding error.
pub(super) fn ang_diff(x: f64, y: f64) -> (f64, f64) {
    let (d, t) = sum_x(remainder(-x, TD), remainder(y, TD));
    let (mut d, t) = sum_x(remainder(d, TD), t);
    if d == 0.0 || d.abs() == HD {
        d = d.copysign(if t == 0.0 { y - x } else { -t });
    }
    (d, t)
}

/// Round tiny angles to multiples of 1/16 · 2⁻⁵³ so an angle of
/// −1e−300 behaves as −0.
pub(super) fn ang_round(x: f64) -> f64 {
    let y = x.abs();
    let w = ANG_ROUND_Z - y;
    let y = if w > 0.0 { ANG_ROUND_Z - w } else { y };
    y.copysign(x)
}

/// Map a reduced angle r in radians with quadrant q to (sin, cos).
fn quadrant_sin_cos(r: f64, q: u32) -> (f64, f64) {
    let (s, c) = r.sin_cos();
    match q {
        0 => (s, c),
        1 => (c, -s),
        2 => (-s, -c),
        _ => (-c, s),
    }
}

/// Fix the signs of zero results as C99 F.10.1.12–13 specifies.
fn signed_zero(sinx: f64, cosx: f64, x: f64) -> (f64, f64) {
    let cosx = cosx + 0.0;
    let sinx = if sinx == 0.0 { sinx.copysign(x) } else { sinx };
    (sinx, cosx)
}

/// sin and cos of `x` degrees.
pub(super) fn sin_cos_d(x: f64) -> (f64, f64) {
    let (r, q) = remquo(x, QD);
    let (s, c) = quadrant_sin_cos(r * DEGREE, q);
    signed_zero(s, c, x)
}

/// sin and cos of `x + t` degrees, where `t` is a small correction.
pub(super) fn sin_cos_de(x: f64, t: f64) -> (f64, f64) {
    let (r, q) = remquo(x, QD);
    let (s, c) = quadrant_sin_cos(ang_round(r + t) * DEGREE, q);
    signed_zero(s, c, x)
}

/// atan2(y, x) in degrees, computed in the octant [−45°, 45°] first.
pub(super) fn atan2_d(y: f64, x: f64) -> f64 {
    let (mut x, mut y) = (x, y);
    let mut q = 0;
    if y.abs() > x.abs() {
        core::mem::swap(&mut x, &mut y);
        q = 2;
    }
    if x.is_sign_negative() {
        x = -x;
        q += 1;
    }
    let ang = y.atan2(x) / DEGREE;
    match q {
        1 => HD.copysign(y) - ang,
        2 => QD - ang,
        3 => -QD + ang,
        _ => ang,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remquo_matches_c() {
        assert_eq!(remquo(135.0, 90.0), (-45.0, 2));
        assert_eq!(remquo(45.0, 90.0), (45.0, 0));
        assert_eq!(remquo(-135.0, 90.0), (45.0, 2));
        assert_eq!(remquo(100.0, 90.0), (10.0, 1));
        assert_eq!(remquo(-100.0, 90.0), (-10.0, 3));
        assert_eq!(remquo(-0.0, 90.0).0.to_bits(), (-0.0f64).to_bits());
        assert_eq!(remainder(540.0, 360.0), -180.0);
        assert_eq!(remainder(-540.0, 360.0), 180.0);
        assert_eq!(remainder(190.0, 360.0), -170.0);
    }

    #[test]
    fn exact_special_angles() {
        assert_eq!(sin_cos_d(30.0).0, (30.0 * DEGREE).sin());
        assert_eq!(
            sin_cos_d(120.0),
            ((30.0 * DEGREE).cos(), -(30.0 * DEGREE).sin())
        );
        assert_eq!(sin_cos_d(90.0), (1.0, 0.0));
        assert_eq!(sin_cos_d(-180.0).1, -1.0);
        assert_eq!(atan2_d(1.0, 1.0), 45.0);
        assert_eq!(atan2_d(0.0, -1.0), 180.0);
        assert_eq!(atan2_d(-0.0, -1.0), -180.0);
        assert_eq!(ang_normalize(-180.0), -180.0);
        assert_eq!(ang_normalize(540.0), 180.0);
    }

    #[test]
    fn ang_diff_is_exact_near_half_turn() {
        assert_eq!(ang_diff(0.0, 180.0).0, 180.0);
        assert_eq!(ang_diff(10.0, -170.0).0, -180.0);
        assert_eq!(ang_diff(-170.0, 170.0).0, -20.0);
    }
}

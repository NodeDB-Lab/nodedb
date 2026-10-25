// SPDX-License-Identifier: Apache-2.0

//! Angle helpers shared by the projection methods.

use core::f64::consts::{PI, TAU};

/// Wrap an angle in radians into [−π, π].
pub fn wrap_pi(angle: f64) -> f64 {
    let a = angle % TAU;
    if a > PI {
        a - TAU
    } else if a < -PI {
        a + TAU
    } else {
        a
    }
}

/// Half a turn in degrees.
const HALF_TURN_DEG: f64 = 180.0;

/// A full turn in degrees.
const FULL_TURN_DEG: f64 = 360.0;

/// Wrap a longitude in degrees into [−180, 180]. A value already inside
/// comes back bit-identical.
pub fn wrap_lon_deg(lon: f64) -> f64 {
    if (-HALF_TURN_DEG..=HALF_TURN_DEG).contains(&lon) {
        return lon;
    }
    let d = lon % FULL_TURN_DEG;
    if d > HALF_TURN_DEG {
        d - FULL_TURN_DEG
    } else if d < -HALF_TURN_DEG {
        d + FULL_TURN_DEG
    } else {
        d
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_keeps_offset_in_half_turn() {
        assert!((wrap_pi(200f64.to_radians()) - (-160f64).to_radians()).abs() < 1e-15);
        assert!((wrap_pi((-200f64).to_radians()) - 160f64.to_radians()).abs() < 1e-15);
        assert_eq!(wrap_pi(0.5), 0.5);
        assert_eq!(wrap_lon_deg(200.0), -160.0);
        assert_eq!(wrap_lon_deg(-200.0), 160.0);
        assert_eq!(wrap_lon_deg(101.7117), 101.7117);
    }
}

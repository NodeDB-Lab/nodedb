// SPDX-License-Identifier: Apache-2.0

//! The coordinate epoch: the decimal year a set of coordinates refers to.

use crate::error::InvalidEpoch;

/// Earliest coordinate epoch accepted, decimal year.
pub const MIN_COORDINATE_EPOCH: f64 = 1900.0;

/// Latest coordinate epoch accepted, decimal year.
pub const MAX_COORDINATE_EPOCH: f64 = 2200.0;

/// A coordinate epoch, a decimal year in
/// [`MIN_COORDINATE_EPOCH`]`..=`[`MAX_COORDINATE_EPOCH`].
///
/// A time-dependent Helmert transformation evaluates its parameters at this
/// epoch.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct CoordinateEpoch(f64);

impl CoordinateEpoch {
    /// The epoch `year`. A non-finite or out-of-range year is refused.
    pub fn new(year: f64) -> Result<CoordinateEpoch, InvalidEpoch> {
        if (MIN_COORDINATE_EPOCH..=MAX_COORDINATE_EPOCH).contains(&year) {
            Ok(CoordinateEpoch(year))
        } else {
            Err(InvalidEpoch { epoch: year })
        }
    }

    /// The decimal year.
    pub fn year(self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_is_inclusive() {
        assert_eq!(
            CoordinateEpoch::new(1900.0).map(CoordinateEpoch::year),
            Ok(1900.0)
        );
        assert_eq!(
            CoordinateEpoch::new(2200.0).map(CoordinateEpoch::year),
            Ok(2200.0)
        );
        assert_eq!(
            CoordinateEpoch::new(2020.5).map(CoordinateEpoch::year),
            Ok(2020.5)
        );
    }

    #[test]
    fn out_of_range_and_non_finite_are_refused() {
        for year in [1899.999, 2200.001, f64::NAN, f64::INFINITY, -2020.0] {
            let err = CoordinateEpoch::new(year).unwrap_err();
            assert!(err.epoch.is_nan() || err.epoch == year);
            assert!(err.to_string().contains("1900"), "{err}");
        }
    }
}

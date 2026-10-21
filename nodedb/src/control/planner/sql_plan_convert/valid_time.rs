// SPDX-License-Identifier: BUSL-1.1

//! The valid-time instant a point read carries.

use nodedb_sql::temporal::ValidTime;

/// The valid-time instant of `valid_time` for a read on `engine`: `None`
/// reads every valid time, `Some(ms)` reads the versions whose valid time
/// contains `ms`.
///
/// A `FOR VALID_TIME FROM lo TO hi` range answers `FeatureNotSupported`:
/// the read carries one instant only, so a range is refused, never widened
/// to every valid time.
pub fn valid_time_point(valid_time: ValidTime, engine: &'static str) -> crate::Result<Option<i64>> {
    match valid_time {
        ValidTime::Any => Ok(None),
        ValidTime::At(ms) => Ok(Some(ms)),
        ValidTime::Range(lo, hi) => Err(crate::Error::FeatureNotSupported {
            detail: format!(
                "FOR VALID_TIME FROM {lo} TO {hi} is not supported on {engine} reads; use \
                 AS OF VALID TIME <t> or FOR VALID_TIME CONTAINS <t>"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_point_passes_and_a_range_is_refused() {
        assert_eq!(valid_time_point(ValidTime::Any, "graph").ok(), Some(None));
        assert_eq!(
            valid_time_point(ValidTime::At(5), "graph").ok(),
            Some(Some(5))
        );
        assert!(matches!(
            valid_time_point(ValidTime::Range(1, 2), "document"),
            Err(crate::Error::FeatureNotSupported { .. })
        ));
    }
}

// SPDX-License-Identifier: Apache-2.0

//! The source of each dim's coordinate: the GEOREF axes, then `TIME` for
//! the one `TIMESTAMP` dim. Any other dim has no source in a raster.

use crate::error::RasterLoadError;
use crate::view::{ArrayView, DimType, DomainBound, ViewDim};

/// The source of one dim's coordinate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CoordSource {
    /// The GEOREF x axis.
    X,
    /// The GEOREF y axis.
    Y,
    /// The `TIME` instant, in ms since the Unix epoch.
    Time(i64),
}

/// The source of every dim of `view`, in schema order.
pub fn plan_coords(
    view: &ArrayView,
    x_index: usize,
    y_index: usize,
    time_ms: Option<i64>,
) -> Result<Vec<CoordSource>, RasterLoadError> {
    let mut time_used = false;
    let mut coords = Vec::with_capacity(view.dims.len());
    for (index, dim) in view.dims.iter().enumerate() {
        let source = if index == x_index {
            CoordSource::X
        } else if index == y_index {
            CoordSource::Y
        } else if dim.dtype == DimType::TimestampMs && !time_used {
            time_used = true;
            CoordSource::Time(time_coord(dim, time_ms)?)
        } else {
            return Err(RasterLoadError::DimWithoutSource {
                dim: dim.name.clone(),
            });
        };
        coords.push(source);
    }
    if time_ms.is_some() && !time_used {
        return Err(RasterLoadError::TimeWithoutDim {
            array: view.name.clone(),
        });
    }
    Ok(coords)
}

/// The `TIME` instant for `dim`, checked against its domain.
fn time_coord(dim: &ViewDim, time_ms: Option<i64>) -> Result<i64, RasterLoadError> {
    let time = time_ms.ok_or_else(|| RasterLoadError::TimeMissing {
        dim: dim.name.clone(),
    })?;
    let inside = match (&dim.lo, &dim.hi) {
        (
            DomainBound::TimestampMs(lo) | DomainBound::Int64(lo),
            DomainBound::TimestampMs(hi) | DomainBound::Int64(hi),
        ) => *lo <= time && time <= *hi,
        _ => false,
    };
    if inside {
        return Ok(time);
    }
    Err(RasterLoadError::TimeOutside {
        time,
        dim: dim.name.clone(),
        lo: bound_text(&dim.lo),
        hi: bound_text(&dim.hi),
    })
}

/// A domain bound as the DDL writes it.
pub(crate) fn bound_text(bound: &DomainBound) -> String {
    match bound {
        DomainBound::Int64(v) | DomainBound::TimestampMs(v) => v.to_string(),
        DomainBound::Float64(v) => v.to_string(),
        DomainBound::String(v) => format!("'{v}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dim(name: &str, dtype: DimType, lo: DomainBound, hi: DomainBound) -> ViewDim {
        ViewDim {
            name: name.into(),
            dtype,
            lo,
            hi,
        }
    }

    fn view(dims: Vec<ViewDim>) -> ArrayView {
        ArrayView {
            name: "scene".into(),
            dims,
            attrs: Vec::new(),
            georef: None,
        }
    }

    #[test]
    fn time_fills_the_timestamp_dim() {
        let ts = || {
            dim(
                "t",
                DimType::TimestampMs,
                DomainBound::TimestampMs(0),
                DomainBound::TimestampMs(1_000),
            )
        };
        let axis = |name: &str| {
            dim(
                name,
                DimType::Int64,
                DomainBound::Int64(0),
                DomainBound::Int64(9),
            )
        };
        let v = view(vec![ts(), axis("row"), axis("col")]);
        let coords = plan_coords(&v, 2, 1, Some(500)).expect("time given");
        assert_eq!(
            coords,
            vec![CoordSource::Time(500), CoordSource::Y, CoordSource::X]
        );
        let sqlstate =
            |r: Result<Vec<CoordSource>, RasterLoadError>| r.expect_err("refused").sqlstate();
        assert_eq!(sqlstate(plan_coords(&v, 2, 1, None)), "42601");
        assert_eq!(sqlstate(plan_coords(&v, 2, 1, Some(5_000))), "22003");
        let plain = view(vec![axis("row"), axis("col")]);
        assert_eq!(sqlstate(plan_coords(&plain, 1, 0, Some(5))), "42601");
        let band = view(vec![axis("band"), axis("row"), axis("col")]);
        assert_eq!(sqlstate(plan_coords(&band, 2, 1, None)), "0A000");
    }
}

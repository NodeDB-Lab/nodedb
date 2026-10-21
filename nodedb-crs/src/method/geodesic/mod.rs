// SPDX-License-Identifier: Apache-2.0

//! Geodesics on an ellipsoid of revolution: the direct and inverse
//! problems by C. F. F. Karney, "Algorithms for geodesics", J. Geodesy 87
//! (2013), ported from GeographicLib's C library as bundled in PROJ 9.8.1
//! (`src/geodesic.c`). Accurate to about 15 nm for |f| ≤ 1/50.

pub mod line;
pub mod model;
pub mod polygon;

mod inverse;
mod math;
mod series;
mod start;

pub use line::{GeodesicLine, GeodesicSolution};
pub use model::Geodesic;
pub use polygon::PolygonMeasure;

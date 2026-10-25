// SPDX-License-Identifier: Apache-2.0

//! Geodesic measures on the ellipsoid of a geographic CRS.

pub mod measure;
pub(crate) mod segment;

pub use measure::GeodesicMeasure;

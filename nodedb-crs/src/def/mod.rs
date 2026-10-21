// SPDX-License-Identifier: Apache-2.0

//! CRS definition types.

pub mod area;
pub mod axis;
pub mod crs_def;
pub mod datum;
pub mod ellipsoid;
pub mod epoch;
pub mod helmert_rates;
pub mod unit;

pub use area::AreaOfUse;
pub use axis::{AxisDirection, AxisOrder};
pub use crs_def::{CrsDef, CrsKind};
pub use datum::{
    Datum, DatumShift, Helmert, NamedDatum, RotationConvention, datum_by_proj_name,
    prime_meridian_by_proj_name,
};
pub use ellipsoid::Ellipsoid;
pub use epoch::{CoordinateEpoch, MAX_COORDINATE_EPOCH, MIN_COORDINATE_EPOCH};
pub use helmert_rates::HelmertRates;
pub use unit::LinearUnit;

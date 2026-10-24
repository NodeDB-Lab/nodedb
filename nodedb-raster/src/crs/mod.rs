// SPDX-License-Identifier: Apache-2.0

//! The raster CRS, from the GeoKey directory.

pub mod projection_keys;
pub mod registry_match;
pub mod srid;
pub mod units;
pub mod user_defined;

pub use registry_match::match_registered;
pub use srid::{crs_ref_from_geokeys, resolve_crs, srid_from_geokeys};
pub use units::USER_DEFINED;
pub use user_defined::decode_user_crs;

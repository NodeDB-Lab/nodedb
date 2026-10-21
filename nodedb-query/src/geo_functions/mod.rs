// SPDX-License-Identifier: Apache-2.0

//! Geospatial SQL function evaluation, driven by a shared catalog.

pub mod accessors;
pub mod catalog;
pub mod constructors;
pub mod crs;
pub mod dispatch;
pub mod encode;
pub mod helpers;
pub mod indexing;
pub mod linear;
pub mod measures;
pub mod overlay;
pub mod predicates;
pub mod srid_refs;

pub use catalog::{GEO_FUNCTIONS, GeoArgShape, GeoFunctionSpec, GeoReturn, returns_geometry};
pub use dispatch::eval_geo_function;
pub use helpers::geometry_from_text;
pub use srid_refs::{
    expr_names_srid, literal_geometry_srid, literal_srid_code, srid_arg_position, type_names_srid,
};

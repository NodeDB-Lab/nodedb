// SPDX-License-Identifier: Apache-2.0

//! Well-Known Text (WKT) and PostGIS EWKT, with Z / M / ZM and EMPTY.
//!
//! - `POINT(x y)`, `POINT Z (x y z)`, `POINTM(x y m)`, `POINT ZM (x y z m)`
//! - `LINESTRING(x1 y1, x2 y2)`, `POLYGON((...), (hole...))`
//! - `MULTIPOINT((x y), (x y))` or `MULTIPOINT(x y, x y)`
//! - `GEOMETRYCOLLECTION(POINT(...), LINESTRING(...))`
//! - `POINT EMPTY`, `GEOMETRYCOLLECTION EMPTY`

pub mod error;
pub mod read;
pub mod write;

pub use error::{WktError, WktErrorKind};
pub use read::{Ewkt, geometry_from_ewkt, geometry_from_wkt};
pub use write::{geometry_to_ewkt, geometry_to_wkt, shape_to_wkt};

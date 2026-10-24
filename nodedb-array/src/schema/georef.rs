// SPDX-License-Identifier: Apache-2.0

//! Georeference of an array: which dims carry the x and y axes of a
//! coordinate reference system, and how cell coordinates map to it.
//!
//! The georef stays out of [`super::ArraySchema::content_msgpack`]. Adding,
//! changing, or removing it leaves `schema_hash` and every segment as they are.

pub use nodedb_crs::GeoTransform;
use nodedb_types::geometry::Srid;
use serde::{Deserialize, Serialize};

/// Georeference of an array.
///
/// `x_dim` and `y_dim` index into [`super::ArraySchema::dims`].
///
/// - `transform: None`: the dims hold pixel-centre world coordinates in
///   `srid`. Each dim is `INT64` or `FLOAT64`.
/// - `transform: Some`: the dims hold `INT64` pixel indices, `x_dim` the
///   column and `y_dim` the row. The transform maps them to world coordinates.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayGeoref {
    pub srid: Srid,
    pub x_dim: u32,
    pub y_dim: u32,
    pub transform: Option<GeoTransform>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn georef_msgpack_round_trip() {
        let g = ArrayGeoref {
            srid: Srid::new(32647).unwrap(),
            x_dim: 1,
            y_dim: 0,
            transform: Some(GeoTransform([600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0])),
        };
        let bytes = zerompk::to_msgpack_vec(&g).unwrap();
        let back: ArrayGeoref = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, g);

        let world = ArrayGeoref {
            srid: Srid::WGS84,
            x_dim: 0,
            y_dim: 1,
            transform: None,
        };
        let bytes = zerompk::to_msgpack_vec(&world).unwrap();
        let back: ArrayGeoref = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, world);
    }
}

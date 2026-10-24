//! Geometry decode over arbitrary bytes: WKB and EWKB with Z/M, the
//! bounding-box scan, and WKT/EWKT when the bytes are UTF-8. Each geometry
//! that decodes is written back out.
//!
//! Every outcome must be `Ok` or a typed error. These readers take client
//! EWKB and WKT literals.

use nodedb_spatial::{
    geometry_from_ewkt, geometry_from_wkb, geometry_from_wkt, geometry_to_ewkb, geometry_to_ewkt,
    geometry_to_wkb, value_of_ewkb, wkb::wkb_bbox,
};
use nodedb_types::geometry::SridGeometry;

/// Input cap. A hostile count must fail on the declared size, not on a
/// large input.
const MAX_INPUT_BYTES: usize = 256 * 1024;

pub fn run(data: &[u8]) {
    let bounded = &data[..data.len().min(MAX_INPUT_BYTES)];
    if let Ok(wkb) = geometry_from_wkb(bounded) {
        write_back(&wkb.geometry);
    }
    let _ = wkb_bbox(bounded);
    let _ = value_of_ewkb(bounded);
    if let Ok(text) = std::str::from_utf8(bounded) {
        if let Ok(geometry) = geometry_from_wkt(text) {
            write_back(&geometry);
        }
        if let Ok(ewkt) = geometry_from_ewkt(text) {
            write_back(&ewkt.geometry);
        }
    }
}

fn write_back(geometry: &SridGeometry) {
    let _ = geometry_to_wkb(geometry);
    let _ = geometry_to_ewkb(geometry);
    let _ = geometry_to_ewkt(geometry);
}

//! CRS definition text over arbitrary bytes: the PROJ string, WKT2 and
//! PROJJSON parsers, then every writer and a transform through each
//! definition that parses.
//!
//! Every outcome must be `Ok` or a typed error. These parsers read user
//! `CREATE SPATIAL_REF_SYS` text and raster GeoKeys.

use std::sync::Arc;

use nodedb_crs::{Srid, Transformer, parse_proj_string, parse_projjson, parse_wkt2};

/// Definition text cap. Real definitions are a few KiB.
const MAX_TEXT_BYTES: usize = 64 * 1024;

/// A user-range code. Parsing does not check it against the registry.
const FUZZ_SRID: u32 = 990_100;

/// Points pushed through each definition: origin, mid-latitude, near-pole.
const POINTS: [[f64; 3]; 3] = [[0.0, 0.0, 0.0], [101.5, 3.1, 10.0], [-45.0, 89.9, 0.0]];

pub fn run(data: &[u8]) {
    let bounded = &data[..data.len().min(MAX_TEXT_BYTES)];
    let Ok(text) = std::str::from_utf8(bounded) else {
        return;
    };
    let Ok(srid) = Srid::new(FUZZ_SRID) else {
        return;
    };
    for parsed in [
        parse_proj_string(srid, "fuzz", text),
        parse_wkt2(srid, "fuzz", text),
        parse_projjson(srid, "fuzz", text),
    ] {
        let Ok(def) = parsed else {
            continue;
        };
        let _ = def.to_proj_string();
        let _ = def.to_wkt2();
        let _ = def.to_projjson();
        let def = Arc::new(def);
        let transformer = Transformer::new(Arc::clone(&def), def);
        for point in POINTS {
            let _ = transformer.apply(point);
        }
    }
}

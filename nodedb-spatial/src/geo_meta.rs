// SPDX-License-Identifier: Apache-2.0

//! GeoParquet and GeoArrow metadata for geometry columns.
//!
//! GeoParquet: JSON metadata in Parquet file key-value metadata that tells
//! external tools (DuckDB, QGIS, GeoPandas) which columns contain geometry
//! and what encoding/CRS is used.
//!
//! GeoArrow: Arrow extension type metadata on Binary columns so Arrow-native
//! tools can recognize NodeDB's spatial columns.
//!
//! References:
//! - GeoParquet spec: https://geoparquet.org/releases/v1.1.0/
//! - GeoArrow spec: https://geoarrow.org/extension-types

use nodedb_crs::CrsDef;
use nodedb_types::geometry::{Dimension, GeometryKind, SridGeometry};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

/// Why geometry column metadata cannot be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GeoMetaError {
    /// The CRS writer produced text that is not JSON.
    #[error("PROJJSON for SRID {code} does not parse: {reason}")]
    Projjson { code: u32, reason: Box<str> },
}

/// The CRS of `def` as a PROJJSON value.
pub fn projjson(def: &CrsDef) -> Result<serde_json::Value, GeoMetaError> {
    sonic_rs::from_str(&def.to_projjson()).map_err(|e| GeoMetaError::Projjson {
        code: def.srid.code(),
        reason: e.to_string().into(),
    })
}

/// A GeoParquet geometry type name: the OGC name with its dimension
/// suffix, `"Point"`, `"Point Z"`, `"Point M"` or `"Point ZM"`.
pub fn geometry_type_name(kind: GeometryKind, dimension: Dimension) -> String {
    let suffix = dimension.wkt_tag();
    if suffix.is_empty() {
        kind.name().to_string()
    } else {
        format!("{} {suffix}", kind.name())
    }
}

/// The distinct geometry type names of a column, sorted.
pub fn column_geometry_types<'a>(geoms: impl IntoIterator<Item = &'a SridGeometry>) -> Vec<String> {
    geoms
        .into_iter()
        .map(|g| geometry_type_name(g.geometry().kind(), g.dimension()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The bounding box of a column: `[xmin, ymin, xmax, ymax]`, or
/// `[xmin, ymin, zmin, xmax, ymax, zmax]` when every geometry has Z. EMPTY
/// geometries add nothing. `None` when nothing has a coordinate.
pub fn column_bbox<'a>(geoms: impl IntoIterator<Item = &'a SridGeometry>) -> Option<Vec<f64>> {
    let mut lo = [f64::INFINITY; 3];
    let mut hi = [f64::NEG_INFINITY; 3];
    let mut all_z = true;
    let mut any = false;
    for g in geoms {
        all_z &= g.has_z() || g.is_empty();
        for p in g.positions() {
            any = true;
            for (i, v) in [p.x, p.y, p.z.unwrap_or(f64::NAN)].into_iter().enumerate() {
                if !v.is_nan() {
                    lo[i] = lo[i].min(v);
                    hi[i] = hi[i].max(v);
                }
            }
        }
    }
    if !any {
        return None;
    }
    Some(if all_z {
        vec![lo[0], lo[1], lo[2], hi[0], hi[1], hi[2]]
    } else {
        vec![lo[0], lo[1], hi[0], hi[1]]
    })
}

/// GeoParquet metadata for a Parquet file containing geometry columns.
///
/// Stored as JSON in the Parquet file's key-value metadata under the key "geo".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    /// GeoParquet spec version.
    pub version: String,
    /// Primary geometry column name.
    pub primary_column: String,
    /// Per-column geometry metadata.
    pub columns: HashMap<String, GeoParquetColumnMeta>,
}

/// Metadata for a single geometry column in GeoParquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoParquetColumnMeta {
    /// Encoding: "WKB" (ISO WKB, Z and M in the type code).
    pub encoding: String,
    /// Geometry types present, dimension suffix included.
    pub geometry_types: Vec<String>,
    /// The column CRS as PROJJSON.
    pub crs: serde_json::Value,
    /// Bounding box: 4 values, or 6 when every geometry has Z.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,
}

impl GeoParquetMetadata {
    /// Metadata for one geometry column of `geoms` in `crs`.
    pub fn single_column<'a>(
        column_name: &str,
        geoms: impl IntoIterator<Item = &'a SridGeometry> + Clone,
        crs: &CrsDef,
    ) -> Result<Self, GeoMetaError> {
        let mut columns = HashMap::new();
        columns.insert(
            column_name.to_string(),
            GeoParquetColumnMeta {
                encoding: "WKB".to_string(),
                geometry_types: column_geometry_types(geoms.clone()),
                crs: projjson(crs)?,
                bbox: column_bbox(geoms),
            },
        );
        Ok(Self {
            version: "1.1.0".to_string(),
            primary_column: column_name.to_string(),
            columns,
        })
    }

    /// Serialize to JSON string for Parquet file metadata.
    pub fn to_json(&self) -> Result<String, sonic_rs::Error> {
        sonic_rs::to_string(self)
    }

    /// The Parquet metadata key for GeoParquet.
    pub const PARQUET_KEY: &'static str = "geo";
}

/// GeoArrow extension type name for WKB-encoded geometry columns. Z and M
/// travel in the WKB type code.
///
/// Register this on Arrow `DataType::Binary` columns so Arrow-native tools
/// (DuckDB, GeoPolars) recognize them as geometry.
pub const GEOARROW_EXTENSION_NAME: &str = "geoarrow.wkb";

/// GeoArrow extension metadata (JSON): `{"crs": <PROJJSON>}`.
///
/// Stored in Arrow schema's field metadata under the key
/// `ARROW:extension:metadata`.
pub fn geoarrow_extension_metadata(crs: &CrsDef) -> Result<String, GeoMetaError> {
    Ok(serde_json::json!({ "crs": projjson(crs)? }).to_string())
}

/// Arrow field metadata keys for extension types.
pub const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const ARROW_EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Arrow field metadata for a WKB geometry column in `crs`.
pub fn geoarrow_field_metadata(crs: &CrsDef) -> Result<HashMap<String, String>, GeoMetaError> {
    let mut meta = HashMap::new();
    meta.insert(
        ARROW_EXTENSION_NAME_KEY.to_string(),
        GEOARROW_EXTENSION_NAME.to_string(),
    );
    meta.insert(
        ARROW_EXTENSION_METADATA_KEY.to_string(),
        geoarrow_extension_metadata(crs)?,
    );
    Ok(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::CrsContext;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid};

    fn def(code: u32) -> std::sync::Arc<CrsDef> {
        CrsContext::builtin_only()
            .resolve(Srid::new(code).unwrap())
            .unwrap()
    }

    fn point_z(x: f64, y: f64, z: f64) -> SridGeometry {
        SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(x, y),
            Ordinates::new(Some(vec![z]), None),
        )
        .unwrap()
    }

    #[test]
    fn type_names_carry_the_dimension_suffix() {
        assert_eq!(
            geometry_type_name(GeometryKind::Point, Dimension::Xy),
            "Point"
        );
        assert_eq!(
            geometry_type_name(GeometryKind::Point, Dimension::Xyz),
            "Point Z"
        );
        assert_eq!(
            geometry_type_name(GeometryKind::Polygon, Dimension::Xym),
            "Polygon M"
        );
        assert_eq!(
            geometry_type_name(GeometryKind::LineString, Dimension::Xyzm),
            "LineString ZM"
        );
    }

    #[test]
    fn geoparquet_metadata_json() {
        let geoms = [point_z(1.0, 2.0, 3.0), point_z(4.0, 5.0, 6.0)];
        let meta = GeoParquetMetadata::single_column("geom", geoms.iter(), &def(4326)).unwrap();
        let col = &meta.columns["geom"];
        assert_eq!(col.geometry_types, vec!["Point Z".to_string()]);
        assert_eq!(col.bbox, Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        assert!(
            col.crs.get("id").is_some() || col.crs.get("name").is_some(),
            "{}",
            col.crs
        );
        let json = meta.to_json().unwrap();
        assert!(json.contains("\"version\":\"1.1.0\""));
        assert!(json.contains("\"encoding\":\"WKB\""));
        let parsed: GeoParquetMetadata = sonic_rs::from_str(&json).unwrap();
        assert_eq!(parsed.primary_column, "geom");
    }

    #[test]
    fn mixed_dimensions_give_a_2d_bbox() {
        let geoms = [
            point_z(1.0, 2.0, 3.0),
            SridGeometry::wgs84(Geometry::point(-1.0, 0.0)),
        ];
        assert_eq!(column_bbox(geoms.iter()), Some(vec![-1.0, 0.0, 1.0, 2.0]));
        assert_eq!(
            column_geometry_types(geoms.iter()),
            vec!["Point".to_string(), "Point Z".to_string()]
        );
    }

    #[test]
    fn geoarrow_field_meta_holds_projjson() {
        let meta = geoarrow_field_metadata(&def(32648)).unwrap();
        assert_eq!(meta[ARROW_EXTENSION_NAME_KEY], "geoarrow.wkb");
        assert!(meta[ARROW_EXTENSION_METADATA_KEY].contains("32648"));
    }
}

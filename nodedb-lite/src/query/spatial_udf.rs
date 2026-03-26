//! DataFusion UDF registration for spatial functions in Lite.
//!
//! Registers ST_* predicates and geo_* utility functions as DataFusion
//! scalar UDFs so they can be used in SQL WHERE clauses.
//!
//! The UDFs operate on Utf8 (JSON) columns containing GeoJSON geometry
//! objects. They deserialize to Geometry, execute the predicate, and
//! return Bool/Float64/Utf8 as appropriate.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};

/// Register all spatial UDFs on a DataFusion SessionContext.
pub fn register_spatial_udfs(ctx: &datafusion::execution::context::SessionContext) {
    ctx.register_udf(make_geo_distance_udf());
    ctx.register_udf(make_geo_type_udf());
    ctx.register_udf(make_geo_is_valid_udf());
    ctx.register_udf(make_st_dwithin_point_udf());
}

/// geo_distance(lng1, lat1, lng2, lat2) → Float64
fn make_geo_distance_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "geo_distance",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let lng1 = extract_f64_scalar(args, 0)?;
            let lat1 = extract_f64_scalar(args, 1)?;
            let lng2 = extract_f64_scalar(args, 2)?;
            let lat2 = extract_f64_scalar(args, 3)?;

            let dist = nodedb_types::geometry::haversine_distance(lng1, lat1, lng2, lat2);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Float64(Some(dist)),
            ))
        }),
    )
}

/// geo_type(geom_json) → Utf8
fn make_geo_type_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "geo_type",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let json_str = extract_utf8_scalar(args, 0)?;
            let geom_type =
                match serde_json::from_str::<nodedb_types::geometry::Geometry>(&json_str) {
                    Ok(g) => g.geometry_type().to_string(),
                    Err(_) => "Unknown".to_string(),
                };
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Utf8(Some(geom_type)),
            ))
        }),
    )
}

/// geo_is_valid(geom_json) → Boolean
fn make_geo_is_valid_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "geo_is_valid",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let json_str = extract_utf8_scalar(args, 0)?;
            let valid = match serde_json::from_str::<nodedb_types::geometry::Geometry>(&json_str) {
                Ok(g) => nodedb_spatial::is_valid(&g),
                Err(_) => false,
            };
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Boolean(Some(valid)),
            ))
        }),
    )
}

/// st_dwithin_point(geom_json, query_lng, query_lat, distance_meters) → Boolean
///
/// Simplified scalar UDF for the most common spatial query pattern:
/// `WHERE st_dwithin_point(location, -73.98, 40.75, 500)`
fn make_st_dwithin_point_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "st_dwithin_point",
        vec![
            DataType::Utf8,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let json_str = extract_utf8_scalar(args, 0)?;
            let query_lng = extract_f64_scalar(args, 1)?;
            let query_lat = extract_f64_scalar(args, 2)?;
            let distance = extract_f64_scalar(args, 3)?;

            let result = match serde_json::from_str::<nodedb_types::geometry::Geometry>(&json_str) {
                Ok(geom) => {
                    let query = nodedb_types::geometry::Geometry::point(query_lng, query_lat);
                    nodedb_spatial::st_dwithin(&geom, &query, distance)
                }
                Err(_) => false,
            };
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Boolean(Some(result)),
            ))
        }),
    )
}

// ── Helpers ──

fn extract_f64_scalar(args: &[ColumnarValue], idx: usize) -> DfResult<f64> {
    match args.get(idx) {
        Some(ColumnarValue::Scalar(datafusion::common::ScalarValue::Float64(Some(v)))) => Ok(*v),
        Some(ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(v)))) => {
            Ok(*v as f64)
        }
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "expected numeric scalar at argument {idx}"
        ))),
    }
}

fn extract_utf8_scalar(args: &[ColumnarValue], idx: usize) -> DfResult<String> {
    match args.get(idx) {
        Some(ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s)))) => {
            Ok(s.clone())
        }
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "expected string scalar at argument {idx}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udf_registration_does_not_panic() {
        use datafusion::execution::FunctionRegistry;
        let ctx = datafusion::execution::context::SessionContext::new();
        register_spatial_udfs(&ctx);
        // Verify UDFs are registered.
        assert!(ctx.udf("geo_distance").is_ok());
        assert!(ctx.udf("geo_type").is_ok());
        assert!(ctx.udf("geo_is_valid").is_ok());
        assert!(ctx.udf("st_dwithin_point").is_ok());
    }
}

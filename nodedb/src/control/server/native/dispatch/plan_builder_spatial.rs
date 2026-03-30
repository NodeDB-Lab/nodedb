//! Spatial scan plan builder for the native protocol.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{SpatialOp, SpatialPredicate};

pub(super) fn build_spatial_scan(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_geometry = fields
        .query_geometry
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_geometry'".to_string(),
        })?
        .clone();

    let predicate_str = fields.spatial_predicate.as_deref().unwrap_or("dwithin");
    let predicate = match predicate_str.to_lowercase().as_str() {
        "dwithin" => SpatialPredicate::DWithin,
        "contains" => SpatialPredicate::Contains,
        "intersects" => SpatialPredicate::Intersects,
        "within" => SpatialPredicate::Within,
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("unknown spatial predicate: {other}"),
            });
        }
    };

    let distance_meters = fields.distance_meters.unwrap_or(0.0);
    let field = fields.field.clone().unwrap_or_else(|| "geom".to_string());
    let limit = fields.limit.unwrap_or(1000) as usize;

    Ok(PhysicalPlan::Spatial(SpatialOp::Scan {
        collection: collection.to_string(),
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters: Vec::new(),
        limit,
        projection: Vec::new(),
        rls_filters: Vec::new(),
    }))
}

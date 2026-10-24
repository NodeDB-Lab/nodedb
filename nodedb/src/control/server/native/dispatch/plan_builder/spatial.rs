// SPDX-License-Identifier: BUSL-1.1

//! Spatial scan plan builder.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::{SpatialOp, SpatialPredicate};

pub(crate) async fn build_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let raw_bytes = fields
        .query_geometry
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_geometry'".to_string(),
        })?;

    // GeoJSON, with the optional `crs` member naming a supported SRID.
    let geometry: nodedb_types::geometry::SridGeometry =
        crate::util::bounded_json::from_slice(raw_bytes).map_err(|e| crate::Error::BadRequest {
            detail: format!("invalid query geometry: {e}"),
        })?;

    let issues = nodedb_spatial::validate::validate_geometry(geometry.geometry());
    if !issues.is_empty() {
        let reasons: Vec<String> = issues.iter().map(ToString::to_string).collect();
        return Err(crate::Error::BadRequest {
            detail: format!("invalid query geometry: {}", reasons.join("; ")),
        });
    }

    let predicate_str = fields.spatial_predicate.as_deref().unwrap_or("dwithin");
    let predicate = match predicate_str.to_lowercase().as_str() {
        "dwithin" => SpatialPredicate::DWithin,
        "3ddwithin" => SpatialPredicate::DWithin3D,
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
    let limit = super::requested_limit(fields)?;

    Ok(PhysicalPlan::Spatial(SpatialOp::Scan {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
        predicate,
        query_geometry: geometry,
        distance_meters,
        attribute_filters: Vec::new(),
        limit,
        projection: Vec::new(),
        rls_filters: Vec::new(),
        prefilter: None,
    }))
}

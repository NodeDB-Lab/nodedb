// SPDX-License-Identifier: BUSL-1.1

//! Spatial scan converter.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::*;

use super::super::aggregate::extract_projection_names;
use super::super::filter::serialize_filters;
use super::super::scan_params::SpatialScanParams;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in crate::control::planner::sql_plan_convert) fn convert_spatial_scan(
    p: SpatialScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let SpatialScanParams {
        collection,
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters,
        limit,
        projection,
        tenant_id,
        database_id,
    } = p;
    let collection_key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(database_id, collection);
    let vshard = collection_key.vshard();
    let attr_bytes = serialize_filters(attribute_filters)?;
    let proj_names = extract_projection_names(projection, &[]);
    let sp = match predicate {
        nodedb_sql::types::SpatialPredicate::DWithin => SpatialPredicate::DWithin,
        nodedb_sql::types::SpatialPredicate::DWithin3D => SpatialPredicate::DWithin3D,
        nodedb_sql::types::SpatialPredicate::Contains => SpatialPredicate::Contains,
        nodedb_sql::types::SpatialPredicate::Intersects => SpatialPredicate::Intersects,
        nodedb_sql::types::SpatialPredicate::Within => SpatialPredicate::Within,
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: qualified_collection,
            field: field.to_string(),
            predicate: sp,
            query_geometry: query_geometry.clone(),
            distance_meters: *distance_meters,
            attribute_filters: attr_bytes,
            limit: *limit,
            projection: proj_names,
            rls_filters: Vec::new(),
            prefilter: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

//! Engine rules for spatial columnar collections.

use crate::engine_rules::*;
use crate::error::Result;
use crate::types::*;

pub struct SpatialRules;

impl EngineRules for SpatialRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Insert {
            collection: p.collection,
            engine: EngineType::Spatial,
            rows: p.rows,
            column_defaults: p.column_defaults,
            if_absent: p.if_absent,
        }])
    }

    /// Spatial extends columnar and inherits the same upsert semantics:
    /// duplicate PK tombstones the prior row; the new row (or merged form
    /// when `on_conflict_updates` is non-empty) is appended.
    fn plan_upsert(&self, p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Upsert {
            collection: p.collection,
            engine: EngineType::Spatial,
            rows: p.rows,
            column_defaults: p.column_defaults,
            on_conflict_updates: p.on_conflict_updates,
        }])
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        // Plain scan on spatial collection — no spatial predicate involved.
        // Spatial predicates (ST_DWithin, ST_Contains, etc.) are detected
        // in select.rs and routed to SpatialScan directly, bypassing this.
        Ok(SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::Spatial,
            filters: p.filters,
            projection: p.projection,
            sort_keys: p.sort_keys,
            limit: p.limit,
            offset: p.offset,
            distinct: p.distinct,
            window_functions: p.window_functions,
        })
    }

    fn plan_point_get(&self, p: PointGetParams) -> Result<SqlPlan> {
        Ok(SqlPlan::PointGet {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::Spatial,
            key_column: p.key_column,
            key_value: p.key_value,
        })
    }

    fn plan_update(&self, p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Update {
            collection: p.collection,
            engine: EngineType::Spatial,
            assignments: p.assignments,
            filters: p.filters,
            target_keys: p.target_keys,
            returning: p.returning,
        }])
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::Spatial,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        let base_scan = SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::Spatial,
            filters: p.filters,
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
        };
        Ok(SqlPlan::Aggregate {
            input: Box::new(base_scan),
            group_by: p.group_by,
            aggregates: p.aggregates,
            having: p.having,
            limit: p.limit,
        })
    }
}

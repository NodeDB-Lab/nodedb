// SPDX-License-Identifier: Apache-2.0

//! Engine rules for spatial columnar collections.

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct SpatialRules;

impl EngineRules for SpatialRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Insert(InsertPlan {
            collection: p.collection,
            engine: EngineType::Spatial,
            route: WriteRoute::ColumnarFamily,
            rows: p.rows,
            volatile_defaults: p.volatile_defaults,
            if_absent: p.if_absent,
            column_schema: p.column_schema,
            primary_key: p.primary_key,
        })])
    }

    /// Spatial extends columnar and inherits the same upsert semantics:
    /// duplicate PK tombstones the prior row; the new row (or merged form
    /// when `on_conflict_updates` is non-empty) is appended.
    fn plan_insert_select(&self, p: InsertSelectParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "INSERT ... SELECT into spatial collection '{}' is not supported; \
                 the target must be a document collection",
                p.collection
            ),
        })
    }

    fn plan_upsert(&self, p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Upsert(UpsertPlan {
            collection: p.collection,
            engine: EngineType::Spatial,
            route: WriteRoute::ColumnarFamily,
            rows: p.rows,
            volatile_defaults: p.volatile_defaults,
            on_conflict_updates: p.on_conflict_updates,
            column_schema: p.column_schema,
            primary_key: p.primary_key,
        })])
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        // A plain spatial scan reads the columnar rows, which keep
        // system-time versions. A spatial collection has no valid-time
        // columns.
        p.temporal.require_bitemporal(&p.collection, false)?;
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
            temporal: p.temporal,
        })
    }

    fn plan_point_get(&self, p: PointGetParams) -> Result<SqlPlan> {
        Ok(SqlPlan::PointGet {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::Spatial,
            key_column: p.key_column,
            key_value: p.key_value,
            projection: p.projection,
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

    /// `UPDATE ... FROM` writes through the document join-update, which a
    /// spatial collection's columnar storage does not take.
    fn plan_update_from(&self, p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "UPDATE ... FROM is not supported on spatial collection '{}'; \
                 the target must be a document collection",
                p.collection
            ),
        })
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::Spatial,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_truncate(&self, p: TruncateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Truncate {
            collection: p.collection,
            engine: EngineType::Spatial,
            restart_identity: p.restart_identity,
        }])
    }

    fn plan_bulk_load(&self, p: BulkLoadParams) -> Result<BulkLoadPlan> {
        Err(SqlError::Unsupported {
            detail: format!(
                "COPY: collection '{}' uses the spatial engine; \
                 use INSERT with a WKT/GeoJSON geometry column instead",
                p.collection
            ),
        })
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        // An aggregate reads at the statement's system time like a scan.
        p.temporal.require_bitemporal(&p.collection, false)?;
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
            temporal: p.temporal,
        };
        Ok(SqlPlan::Aggregate {
            input: Box::new(base_scan),
            group_by: p.group_by,
            group_by_aliases: Vec::new(),
            output_order: Vec::new(),
            aggregates: p.aggregates,
            having: p.having,
            limit: p.limit,
            grouping_sets: p.grouping_sets,
            sort_keys: Vec::new(),
        })
    }

    fn plan_merge(&self, p: MergeParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "MERGE is not supported on spatial collection '{}'",
                p.collection
            ),
        })
    }

    /// Every column is declared at creation and encoded per column.
    fn accepts_undeclared_columns(&self) -> bool {
        false
    }
}

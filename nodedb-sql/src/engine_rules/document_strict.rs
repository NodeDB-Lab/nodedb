// SPDX-License-Identifier: Apache-2.0

//! Engine rules for strict document collections.

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct StrictRules;

impl EngineRules for StrictRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Insert {
            collection: p.collection,
            engine: EngineType::DocumentStrict,
            route: WriteRoute::Document,
            rows: p.rows,
            column_defaults: p.column_defaults,
            if_absent: p.if_absent,
            column_schema: vec![],
            primary_key: p.primary_key,
        }])
    }

    fn plan_upsert(&self, p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Upsert {
            collection: p.collection,
            engine: EngineType::DocumentStrict,
            route: WriteRoute::Document,
            rows: p.rows,
            column_defaults: p.column_defaults,
            on_conflict_updates: p.on_conflict_updates,
            column_schema: vec![],
            primary_key: p.primary_key,
        }])
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        if p.temporal.is_temporal() && !p.bitemporal {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "FOR SYSTEM_TIME / FOR VALID_TIME requires a bitemporal \
                     collection; '{}' was not created WITH bitemporal = true",
                    p.collection
                ),
            });
        }
        if let Some(plan) =
            crate::engine_rules::try_document_index_lookup(&p, EngineType::DocumentStrict)
        {
            return Ok(plan);
        }
        Ok(SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::DocumentStrict,
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
            engine: EngineType::DocumentStrict,
            key_column: p.key_column,
            key_value: p.key_value,
            projection: p.projection,
        })
    }

    fn plan_update(&self, p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Update {
            collection: p.collection,
            engine: EngineType::DocumentStrict,
            assignments: p.assignments,
            filters: p.filters,
            target_keys: p.target_keys,
            returning: p.returning,
        }])
    }

    fn plan_update_from(&self, p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::UpdateFrom {
            collection: p.collection,
            engine: EngineType::DocumentStrict,
            source: p.source,
            target_join_col: p.target_join_col,
            source_join_col: p.source_join_col,
            assignments: p.assignments,
            target_filters: p.target_filters,
            returning: p.returning,
        }])
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::DocumentStrict,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_merge(&self, p: MergeParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Merge {
            target: p.collection,
            engine: EngineType::DocumentStrict,
            source: p.source,
            target_join_col: p.target_join_col,
            source_join_col: p.source_join_col,
            source_alias: p.source_alias,
            clauses: p.clauses,
            returning: p.returning,
        }])
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        let base_scan = SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::DocumentStrict,
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
            grouping_sets: None,
            sort_keys: Vec::new(),
        })
    }

    /// A Binary Tuple is positional over the declared schema, so an
    /// undeclared field cannot exist.
    fn accepts_undeclared_columns(&self) -> bool {
        false
    }
}

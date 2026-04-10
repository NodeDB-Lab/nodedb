//! Engine rules for key-value collections.

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct KvRules;

impl EngineRules for KvRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        // KV INSERT is handled separately via KvInsert plan variant.
        // The caller (dml.rs) builds KvInsert directly because it needs
        // to split key vs value columns. This method handles the case
        // where a generic INSERT reaches KV — which should not happen
        // because dml.rs checks engine type before calling engine_rules.
        // But if it does, we error clearly.
        Err(SqlError::Unsupported {
            detail: format!(
                "INSERT into KV collection '{}' must use the KV insert path",
                p.collection
            ),
        })
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        // KV upsert is handled via the KvInsert path (KV PUT is naturally an upsert).
        // Same as plan_insert — KV INSERT uses KvInsert which is already upsert semantics.
        Err(SqlError::Unsupported {
            detail: "KV UPSERT must use the KV insert path (KV PUT is naturally an upsert)".into(),
        })
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        Ok(SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::KeyValue,
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
            engine: EngineType::KeyValue,
            key_column: p.key_column,
            key_value: p.key_value,
        })
    }

    fn plan_update(&self, p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Update {
            collection: p.collection,
            engine: EngineType::KeyValue,
            assignments: p.assignments,
            filters: p.filters,
            target_keys: p.target_keys,
            returning: p.returning,
        }])
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::KeyValue,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        let base_scan = SqlPlan::Scan {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::KeyValue,
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

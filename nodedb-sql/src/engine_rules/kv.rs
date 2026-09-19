// SPDX-License-Identifier: Apache-2.0

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
        if p.temporal.is_temporal() {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "FOR SYSTEM_TIME / FOR VALID_TIME is not supported on KV collection '{}'",
                    p.collection
                ),
            });
        }
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
            temporal: p.temporal,
        })
    }

    fn plan_point_get(&self, p: PointGetParams) -> Result<SqlPlan> {
        Ok(SqlPlan::PointGet {
            collection: p.collection,
            alias: p.alias,
            engine: EngineType::KeyValue,
            key_column: p.key_column,
            key_value: p.key_value,
            projection: p.projection,
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

    fn plan_update_from(&self, p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "UPDATE ... FROM is not supported on KV collection '{}'; \
                 KV keys are opaque and cannot participate in cross-table join updates",
                p.collection
            ),
        })
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::KeyValue,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_truncate(&self, p: TruncateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Truncate {
            collection: p.collection,
            engine: EngineType::KeyValue,
            restart_identity: p.restart_identity,
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
            temporal: crate::temporal::TemporalScope::default(),
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

    fn plan_merge(&self, p: MergeParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "MERGE is not supported on key-value collection '{}'",
                p.collection
            ),
        })
    }

    /// The key/value/ttl shape is fixed.
    fn accepts_undeclared_columns(&self) -> bool {
        false
    }

    /// The key/value/ttl triple is the engine's fixed shape, so a collection
    /// carries all three even when the DDL names only the key.
    fn implicit_columns(&self) -> &'static [&'static str] {
        &["key", "value", "ttl"]
    }

    fn value_column_name_is_free(&self) -> bool {
        true
    }
}

//! Engine rules for the ND sparse array engine.
//!
//! Array operations live behind a dedicated DDL/DML surface
//! (`CREATE ARRAY`, `INSERT INTO ARRAY`, `DELETE FROM ARRAY`,
//! `DROP ARRAY`). The standard SQL DML pathways are unsupported by
//! design — the planner refuses them with a hint pointing to the
//! correct surface (and to sub-pass 3 for table-valued reads).

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct ArrayRules;

impl EngineRules for ArrayRules {
    fn plan_insert(&self, _p: InsertParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "INSERT",
            "use INSERT INTO ARRAY <name> COORDS (...) VALUES (...)",
        ))
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported("UPSERT", "arrays do not support UPSERT"))
    }

    fn plan_scan(&self, _p: ScanParams) -> Result<SqlPlan> {
        Err(unsupported(
            "SELECT",
            "table-valued array reads (ARRAY_SLICE, ARRAY_AGG, ...) land in sub-pass 3",
        ))
    }

    fn plan_point_get(&self, _p: PointGetParams) -> Result<SqlPlan> {
        Err(unsupported(
            "point lookup",
            "arrays have no primary key; use ARRAY_SLICE (sub-pass 3)",
        ))
    }

    fn plan_update(&self, _p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "UPDATE",
            "arrays are write-by-coord; re-INSERT to overwrite",
        ))
    }

    fn plan_delete(&self, _p: DeleteParams) -> Result<Vec<SqlPlan>> {
        Err(unsupported(
            "DELETE",
            "use DELETE FROM ARRAY <name> WHERE COORDS IN ((...), ...)",
        ))
    }

    fn plan_aggregate(&self, _p: AggregateParams) -> Result<SqlPlan> {
        Err(unsupported(
            "GROUP BY",
            "table-valued array aggregates land in sub-pass 3",
        ))
    }
}

fn unsupported(op: &str, hint: &str) -> SqlError {
    SqlError::Unsupported {
        detail: format!("operation {op} not supported on array engine; {hint}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip() -> InsertParams {
        InsertParams {
            collection: "g".into(),
            columns: vec![],
            rows: vec![],
            column_defaults: vec![],
            if_absent: false,
        }
    }

    #[test]
    fn every_arm_is_unsupported() {
        let r = ArrayRules;
        assert!(matches!(
            r.plan_insert(ip()).unwrap_err(),
            SqlError::Unsupported { .. }
        ));
        assert!(matches!(
            r.plan_upsert(UpsertParams {
                collection: "g".into(),
                columns: vec![],
                rows: vec![],
                column_defaults: vec![],
                on_conflict_updates: vec![],
            })
            .unwrap_err(),
            SqlError::Unsupported { .. }
        ));
    }
}

// SPDX-License-Identifier: Apache-2.0

//! `MERGE` plan payload.

use crate::types::query::EngineType;

use super::super::merge_types::MergePlanClause;
use super::plan::SqlPlan;

/// Payload of [`SqlPlan::Merge`](crate::types::SqlPlan::Merge).
#[derive(Debug, Clone)]
pub struct MergePlan {
    pub target: String,
    pub engine: EngineType,
    /// Source plan (Scan, DocumentIndexLookup, or Join of a sub-select).
    pub source: Box<SqlPlan>,
    /// Column in the target used for the equi-join (from ON clause).
    pub target_join_col: String,
    /// Column in the source used for the equi-join (from ON clause).
    pub source_join_col: String,
    /// Alias used to qualify source columns in expressions (e.g. `src.col`).
    pub source_alias: String,
    /// WHEN arms in declaration order.
    pub clauses: Vec<MergePlanClause>,
    pub returning: bool,
}

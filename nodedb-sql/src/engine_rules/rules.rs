// SPDX-License-Identifier: Apache-2.0

//! The `EngineRules` trait: single source of truth for per-engine DML routing.

use crate::error::Result;
use crate::types::SqlPlan;

use super::params::{
    AggregateParams, DeleteParams, InsertParams, MergeParams, PointGetParams, ScanParams,
    TruncateParams, UpdateFromParams, UpdateParams, UpsertParams,
};

/// Engine-specific planning rules.
///
/// Each engine type implements this trait to produce the correct `SqlPlan`
/// variant for each operation, or return an error if the operation is not
/// supported. This is the single source of truth for operation routing —
/// no downstream code should ever check engine type to decide routing.
pub trait EngineRules {
    /// Plan an INSERT. Returns `Err` if the engine does not support inserts
    /// (e.g. timeseries routes to `TimeseriesIngest` instead).
    fn plan_insert(&self, params: InsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan an UPSERT (insert-or-merge). Returns `Err` for append-only or
    /// columnar engines that don't support merge semantics.
    fn plan_upsert(&self, params: UpsertParams) -> Result<Vec<SqlPlan>>;
    /// Plan a table scan (SELECT without point-get optimization).
    fn plan_scan(&self, params: ScanParams) -> Result<SqlPlan>;
    /// Plan a point lookup by primary key. Returns `Err` for engines
    /// that don't support O(1) key lookups (e.g. timeseries).
    fn plan_point_get(&self, params: PointGetParams) -> Result<SqlPlan>;
    /// Plan an UPDATE. Returns `Err` for append-only engines.
    fn plan_update(&self, params: UpdateParams) -> Result<Vec<SqlPlan>>;
    /// Plan an `UPDATE target SET ... FROM src WHERE ...`.
    ///
    /// Returns `Err(SqlError::Unsupported)` for engines that cannot participate
    /// as an update target in a cross-table update (timeseries, kv-with-opaque-keys).
    fn plan_update_from(&self, params: UpdateFromParams) -> Result<Vec<SqlPlan>>;
    /// Plan a DELETE (point or bulk).
    fn plan_delete(&self, params: DeleteParams) -> Result<Vec<SqlPlan>>;
    /// Plan a TRUNCATE. Returns `Err(SqlError::Unsupported)` for engines
    /// that carry no whole-collection clear on the SQL surface (array).
    fn plan_truncate(&self, params: TruncateParams) -> Result<Vec<SqlPlan>>;
    /// Plan a GROUP BY / aggregate query.
    fn plan_aggregate(&self, params: AggregateParams) -> Result<SqlPlan>;
    /// Plan a MERGE statement.
    ///
    /// Returns `Err(SqlError::Unsupported)` for engines that do not support
    /// MERGE semantics (everything except `document_schemaless` and
    /// `document_strict`).
    fn plan_merge(&self, params: MergeParams) -> Result<Vec<SqlPlan>>;
    /// True when the engine accepts fields the schema never declares, so a
    /// read of an undeclared name resolves to NULL instead of raising
    /// `SqlError::UnknownColumn`.
    fn accepts_undeclared_columns(&self) -> bool;
    /// Columns every collection on this engine carries whether or not the
    /// DDL declares them. A reference to one resolves like a declared column.
    fn implicit_columns(&self) -> &'static [&'static str] {
        &[]
    }
    /// True when the engine stores one value whose column name the statement
    /// picks. Such a collection resolves any name while it declares no value
    /// column, and closes to the declared set once it names one.
    fn value_column_name_is_free(&self) -> bool {
        false
    }
}

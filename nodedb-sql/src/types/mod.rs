// SPDX-License-Identifier: Apache-2.0

//! SqlPlan intermediate representation types.
//!
//! These types represent the output of the nodedb-sql planner. Both Origin
//! (server) and Lite (embedded) map these to their own execution model.

pub mod collection;
pub mod filter;
pub mod plan;
pub mod query;

pub use collection::{CollectionInfo, ColumnInfo, IndexSpec, IndexState};
pub use filter::{CompareOp, Filter, FilterExpr};
pub use plan::{
    ArrayPrefilter, DistanceMetric, KvInsertIntent, MergeClauseKind, MergePlanAction,
    MergePlanClause, PlanCacheEligibility, SqlPlan, VectorAnnOptions, VectorPrimaryRow,
    VectorQuantization,
};
pub use query::{
    AggOutputSlot, AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate,
    WindowSpec,
};

// ── SQL value / expression / operator types ──
// Re-exported so downstream `use crate::types::*` continues to resolve these
// symbols without change.
pub use crate::types_expr::{BinaryOp, SqlDataType, SqlExpr, SqlPayloadAtom, SqlValue, UnaryOp};

// ── Catalog trait ──
// Re-exported here so downstream modules that `use crate::types::*`
// keep resolving `SqlCatalog` without changing their imports.
pub use crate::catalog::{ArrayCatalogView, SqlCatalog, SqlCatalogError};
pub use crate::fts_types::FtsQuery;

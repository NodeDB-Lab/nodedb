// SPDX-License-Identifier: Apache-2.0

//! Secondary-index read plan payloads: index lookup and range scan.

use crate::temporal::TemporalScope;
use crate::types::filter::Filter;
use crate::types::query::{EngineType, Projection, SortKey, WindowSpec};
use crate::types_expr::SqlValue;

/// Payload of [`SqlPlan::DocumentIndexLookup`](crate::types::SqlPlan::DocumentIndexLookup).
#[derive(Debug, Clone)]
pub struct DocumentIndexLookupPlan {
    pub collection: String,
    pub alias: Option<String>,
    pub engine: EngineType,
    /// Indexed field path used for the lookup.
    pub field: String,
    /// Equality value from the WHERE clause.
    pub value: SqlValue,
    /// Remaining filters after extracting the equality used for lookup.
    pub filters: Vec<Filter>,
    pub projection: Vec<Projection>,
    pub sort_keys: Vec<SortKey>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
    pub window_functions: Vec<WindowSpec>,
    /// Whether the chosen index is COLLATE NOCASE — the executor
    /// lowercases the lookup value before probing.
    pub case_insensitive: bool,
    /// Bitemporal qualifier — mirrors `Scan::temporal`. Document
    /// engines must honor it at the Ceiling stage.
    pub temporal: TemporalScope,
}

/// Payload of [`SqlPlan::RangeScan`](crate::types::SqlPlan::RangeScan).
#[derive(Debug, Clone)]
pub struct RangeScanPlan {
    pub collection: String,
    pub field: String,
    pub lower: Option<SqlValue>,
    pub upper: Option<SqlValue>,
    pub limit: usize,
    /// Resolved SELECT target list, for output-schema derivation.
    pub projection: Vec<Projection>,
}

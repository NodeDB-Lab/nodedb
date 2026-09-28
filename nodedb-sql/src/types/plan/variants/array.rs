// SPDX-License-Identifier: Apache-2.0

//! ND sparse array plan payloads.

use crate::temporal::TemporalScope;
use crate::types_array;

/// Payload of [`SqlPlan::CreateArray`](crate::types::SqlPlan::CreateArray).
#[derive(Debug, Clone)]
pub struct CreateArrayPlan {
    pub name: String,
    pub dims: Vec<types_array::ArrayDimAst>,
    pub attrs: Vec<types_array::ArrayAttrAst>,
    pub tile_extents: Vec<i64>,
    pub cell_order: types_array::ArrayCellOrderAst,
    pub tile_order: types_array::ArrayTileOrderAst,
    /// Hilbert-prefix bits for vShard routing (1–16, default 8).
    pub prefix_bits: u8,
    /// Audit-retention horizon in milliseconds. `None` = non-bitemporal.
    pub audit_retain_ms: Option<u64>,
    /// Compliance floor for `audit_retain_ms`. `None` = no floor.
    pub minimum_audit_retain_ms: Option<u64>,
}

/// Payload of [`SqlPlan::AlterArray`](crate::types::SqlPlan::AlterArray).
#[derive(Debug, Clone)]
pub struct AlterArrayPlan {
    pub name: String,
    /// New value for `audit_retain_ms`. `Some(None)` unregisters
    /// the array from the bitemporal retention registry.
    pub audit_retain_ms: Option<Option<i64>>,
    /// New value for `minimum_audit_retain_ms`. Cannot be NULL.
    pub minimum_audit_retain_ms: Option<u64>,
}

/// Payload of [`SqlPlan::InsertArray`](crate::types::SqlPlan::InsertArray).
#[derive(Debug, Clone)]
pub struct InsertArrayPlan {
    pub name: String,
    pub rows: Vec<types_array::ArrayInsertRow>,
}

/// Payload of [`SqlPlan::DeleteArray`](crate::types::SqlPlan::DeleteArray).
#[derive(Debug, Clone)]
pub struct DeleteArrayPlan {
    pub name: String,
    pub coords: Vec<Vec<types_array::ArrayCoordLiteral>>,
}

/// Payload of [`SqlPlan::ArraySlice`](crate::types::SqlPlan::ArraySlice).
#[derive(Debug, Clone)]
pub struct ArraySlicePlan {
    pub name: String,
    pub slice: types_array::ArraySliceAst,
    /// Attribute names. Empty = all attrs.
    pub attr_projection: Vec<String>,
    /// 0 = unlimited.
    pub limit: u32,
    /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
    /// Plane returns the live (current) state — the default fast path.
    /// Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME` clauses.
    pub temporal: TemporalScope,
}

/// Payload of [`SqlPlan::ArrayProject`](crate::types::SqlPlan::ArrayProject).
#[derive(Debug, Clone)]
pub struct ArrayProjectPlan {
    pub name: String,
    /// Attribute names. Must be non-empty.
    pub attr_projection: Vec<String>,
}

/// Payload of [`SqlPlan::ArrayAgg`](crate::types::SqlPlan::ArrayAgg).
#[derive(Debug, Clone)]
pub struct ArrayAggPlan {
    pub name: String,
    pub attr: String,
    pub reducer: types_array::ArrayReducerAst,
    /// `None` = scalar fold; `Some(name)` = group by that dim.
    pub group_by_dim: Option<String>,
    /// Bitemporal qualifier. When both axes are `None` / `Any`, the Data
    /// Plane aggregates against the live (current) state — the default
    /// fast path. Populated from `AS OF SYSTEM TIME` / `AS OF VALID TIME`.
    pub temporal: TemporalScope,
}

/// Payload of [`SqlPlan::ArrayElementwise`](crate::types::SqlPlan::ArrayElementwise).
#[derive(Debug, Clone)]
pub struct ArrayElementwisePlan {
    pub left: String,
    pub right: String,
    pub op: types_array::ArrayBinaryOpAst,
    pub attr: String,
}

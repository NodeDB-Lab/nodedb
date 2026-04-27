//! Typed AST returned by the array-statement parser.

use crate::types_array::{
    ArrayAttrAst, ArrayCellOrderAst, ArrayCoordLiteral, ArrayDimAst, ArrayInsertRow,
    ArrayTileOrderAst,
};

/// Top-level array-statement AST. One variant per surface command.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayStatement {
    Create(CreateArrayAst),
    Drop(DropArrayAst),
    Insert(InsertArrayAst),
    Delete(DeleteArrayAst),
    Alter(AlterArrayAst),
}

/// `ALTER NDARRAY <name> SET ( key = value [, key = value]* )`
///
/// Supported keys:
/// - `audit_retain_ms`         — `NULL` or non-negative integer.
/// - `minimum_audit_retain_ms` — non-negative integer.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterArrayAst {
    pub name: String,
    /// Key-value pairs from the SET clause. Values are either integer
    /// literals or NULL (represented as `None`).
    pub set: Vec<(String, Option<i64>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateArrayAst {
    pub name: String,
    pub dims: Vec<ArrayDimAst>,
    pub attrs: Vec<ArrayAttrAst>,
    pub tile_extents: Vec<i64>,
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    /// Number of Hilbert-prefix bits used for vShard routing.
    /// Accepted via optional `WITH (prefix_bits = N)` clause; default 8.
    pub prefix_bits: u8,
    /// Audit-retention horizon in milliseconds. `Some(0)` means "retain
    /// forever"; `None` means non-bitemporal (no purge runs). Maps to
    /// `ArrayCatalogEntry::audit_retain_ms`.
    pub audit_retain_ms: Option<u64>,
    /// Minimum allowed `audit_retain_ms` — compliance floor. `None`
    /// defaults to 0 (no floor). Validated via `BitemporalRetention::validate()`
    /// in the planner.
    pub minimum_audit_retain_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropArrayAst {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertArrayAst {
    pub name: String,
    pub rows: Vec<ArrayInsertRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteArrayAst {
    pub name: String,
    pub coords: Vec<Vec<ArrayCoordLiteral>>,
}

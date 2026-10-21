// SPDX-License-Identifier: Apache-2.0

//! Typed AST returned by the array-statement parser.

use crate::types_array::{
    ArrayAttrAst, ArrayAttrMetaAlter, ArrayCellOrderAst, ArrayCoordLiteral, ArrayDimAst,
    ArrayGeorefAlter, ArrayGeorefAst, ArrayInsertRow, ArrayTileOrderAst,
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

/// `ALTER ARRAY <name> SET ( key = value [, key = value]* )`,
/// `ALTER ARRAY <name> SET GEOREF (...)`, `ALTER ARRAY <name> DROP GEOREF`,
/// `ALTER ARRAY <name> ALTER ATTR <attr> SET (NODATA n, ...)`, or
/// `ALTER ARRAY <name> ALTER ATTR <attr> DROP NODATA [, ...]`.
///
/// Supported SET keys:
/// - `audit_retain_ms`         — `NULL` or non-negative integer.
/// - `minimum_audit_retain_ms` — non-negative integer.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterArrayAst {
    pub name: String,
    pub action: AlterArrayAction,
}

/// The one change an `ALTER ARRAY` statement makes.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterArrayAction {
    /// Key-value pairs from the SET clause. Values are either integer
    /// literals or NULL (represented as `None`).
    SetOptions(Vec<(String, Option<i64>)>),
    /// `SET GEOREF (...)` or `DROP GEOREF`.
    Georef(ArrayGeorefAlter),
    /// `ALTER ATTR <attr> SET (...)` or `ALTER ATTR <attr> DROP ...`.
    AttrMeta(ArrayAttrMetaAlter),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateArrayAst {
    pub name: String,
    pub dims: Vec<ArrayDimAst>,
    pub attrs: Vec<ArrayAttrAst>,
    pub tile_extents: Vec<i64>,
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    /// Optional `GEOREF (...)` clause.
    pub georef: Option<ArrayGeorefAst>,
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

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

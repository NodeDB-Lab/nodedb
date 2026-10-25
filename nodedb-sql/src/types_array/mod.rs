// SPDX-License-Identifier: Apache-2.0

//! Engine-agnostic AST shapes for array DDL/DML carried on `SqlPlan`.
//!
//! Kept free of any dependency on `nodedb-array` so the SQL crate stays
//! lean. The Origin-side SqlPlan→PhysicalPlan converter is responsible
//! for translating these into typed `nodedb_array::ArraySchema` and
//! engine-typed coord/cell values before crossing the bridge.

pub mod attr;
pub mod ops;
pub mod query;
pub mod schema;

pub use attr::{
    ArrayAttrAst, ArrayAttrMetaAlter, ArrayAttrMetaAst, ArrayAttrMetaChange, ArrayAttrMetaKey,
};
pub use ops::{ArrayBinaryOpAst, ArrayReducerAst, ArrayReducerName};
pub use query::{
    ArrayAttrLiteral, ArrayCoordLiteral, ArrayGeorefAlter, ArrayGeorefAst, ArrayInsertRow,
    ArraySliceAst, ArrayZoneAst, NamedDimRange,
};
pub use schema::{
    ArrayAttrType, ArrayCellOrderAst, ArrayDimAst, ArrayDimType, ArrayDomainBound,
    ArrayTileOrderAst,
};

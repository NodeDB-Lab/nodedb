// SPDX-License-Identifier: Apache-2.0

//! The array a load writes into, and the rows and metadata it produces, as
//! plain types. The server converts its catalog view into these and the
//! results back, so the crate holds no SQL types.

pub mod row;
pub mod schema;

pub use row::{AdoptedMeta, AttrLiteral, CoordLiteral, LoadRow};
pub use schema::{
    ArrayView, AttrMeta, AttrType, DimType, DomainBound, ViewAttr, ViewDim, ViewGeoref,
};

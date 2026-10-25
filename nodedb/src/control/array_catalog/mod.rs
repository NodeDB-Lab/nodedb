// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane catalog for ND-array metadata.
//!
//! Persists schema digests for every array created via DDL, loaded at
//! server startup from the system catalog's `_system.arrays` table.
//! The Data Plane never touches this directly — dispatch handlers
//! consult the in-memory registry via the shared [`ArrayCatalogHandle`].

pub(crate) mod attr_meta;
pub(crate) mod cell_route;
pub(crate) mod ddl;
pub mod entry;
pub(crate) mod georef;
pub mod persist;
pub mod registry;
pub(crate) mod retention;
pub(crate) mod schema_recode;

pub use entry::{ArrayCatalogEntry, ArrayMove};
pub use registry::{ArrayCatalog, ArrayCatalogHandle};

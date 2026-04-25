//! Control-Plane catalog for ND-array metadata.
//!
//! Persists schema digests for every array created via DDL, loaded at
//! server startup from the system catalog's `_system.arrays` table.
//! The Data Plane never touches this directly — dispatch handlers in
//! sub-pass 1c consult the in-memory registry via the shared
//! [`ArrayCatalogHandle`].

pub mod entry;
pub mod persist;
pub mod registry;

pub use entry::ArrayCatalogEntry;
pub use registry::{ArrayCatalog, ArrayCatalogHandle};

// SPDX-License-Identifier: BUSL-1.1

//! Catalog data source: identity-scoped row production for `pg_catalog` and
//! `_system` relations, consumed by the unified query engine.

pub mod audit_log;
pub mod dropped_collections;
pub mod l2_cleanup_queue;
pub mod oid;
pub mod provider;
pub mod schema;
pub mod spatial_ref_sys;
pub mod tables;

pub use provider::{CatalogProvider, catalog_rows};
pub use schema::{KNOWN_TABLES, catalog_collection_info};

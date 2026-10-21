// SPDX-License-Identifier: BUSL-1.1

//! Read-only handle to the system catalog.
//!
//! A read-write `redb::Database` writes on drop: it commits the allocator
//! state table so the next open can skip a full repair. That makes any boot
//! path holding a read-write handle rewrite `system.redb` even when it only
//! ever read — which defeats verifying a catalog (or a backup of one) by
//! hash, and means a boot that later fails its integrity check has already
//! mutated persistent catalog state on its way out.
//!
//! Boot paths that only read the catalog take this handle instead. It wraps
//! `redb::ReadOnlyDatabase`, which never writes, and reuses the same loader
//! bodies as [`SystemCatalog`] so the two can never drift.

use std::path::Path;
use std::sync::Arc;

use nodedb_types::DatabaseId;
use redb::{ReadOnlyDatabase, ReadableDatabase};

use super::database_types::DatabaseDescriptor;
use super::types::{StoredCollection, catalog_err};
use crate::control::array_catalog::ArrayCatalogEntry;
use nodedb_types::StoredVectorIndexParams;

/// Read-only view of the system catalog. Opening and dropping one is
/// guaranteed not to modify a byte of `system.redb`.
#[derive(Clone)]
pub struct ReadOnlySystemCatalog {
    db: Arc<ReadOnlyDatabase>,
}

impl ReadOnlySystemCatalog {
    /// Open the catalog read-only.
    ///
    /// Returns `Ok(None)` when the file does not exist yet, so a cold boot
    /// can proceed without treating "no catalog" as an error.
    ///
    /// A catalog that was not shut down cleanly cannot be opened read-only —
    /// redb needs to write to repair it. That case reports
    /// [`ReadOnlyOpenError::RepairRequired`] so the caller can decide to fall
    /// back to a read-write open; repairing legitimately mutates the file, and
    /// silently doing so behind a read-only API would be the very surprise
    /// this type exists to prevent.
    pub fn open(path: &Path) -> Result<Option<Self>, ReadOnlyOpenError> {
        if !path.exists() {
            return Ok(None);
        }
        match ReadOnlyDatabase::open(path) {
            Ok(db) => Ok(Some(Self { db: Arc::new(db) })),
            Err(redb::DatabaseError::RepairAborted) => Err(ReadOnlyOpenError::RepairRequired),
            Err(e) => Err(ReadOnlyOpenError::Open(e.to_string())),
        }
    }

    fn read_txn(&self, op: &str) -> crate::Result<redb::ReadTransaction> {
        self.db.begin_read().map_err(|e| catalog_err(op, e))
    }

    /// See [`SystemCatalog::load_all_arrays`].
    pub fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        super::arrays::load_all_arrays_in(&self.read_txn("read txn")?)
    }

    /// See [`SystemCatalog::load_wal_tombstones`].
    pub fn load_wal_tombstones(&self) -> crate::Result<nodedb_wal::TombstoneSet> {
        super::wal_tombstones::load_wal_tombstones_in(
            &self.read_txn("load_wal_tombstones read txn")?,
        )
    }

    /// See [`SystemCatalog::list_all_vector_index_params`].
    pub fn list_all_vector_index_params(&self) -> crate::Result<Vec<StoredVectorIndexParams>> {
        super::vector_index_params::list_all_vector_index_params_in(&self.read_txn("read txn")?)
    }

    /// The user-defined CRS set the catalog holds.
    pub fn load_custom_crs_set(&self) -> crate::Result<nodedb_crs::CustomCrsSet> {
        super::spatial_ref_sys::load_custom_crs_set_in(
            &self.read_txn("load_custom_crs_set read txn")?,
        )
    }

    /// See [`SystemCatalog::list_databases`].
    pub fn list_databases(&self) -> crate::Result<Vec<DatabaseDescriptor>> {
        super::database::list_databases_in(&self.read_txn("list_databases read txn")?)
    }

    /// See [`SystemCatalog::load_collections_for_tenant`].
    pub fn load_collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        super::collections::load_collections_for_tenant_in(
            &self.read_txn("read txn")?,
            database_id,
            tenant_id,
        )
    }

    /// See [`SystemCatalog::load_all_collections`].
    pub fn load_all_collections(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StoredCollection>> {
        super::collections::scan_collections_filtered_in(
            &self.read_txn("read txn")?,
            database_id,
            |_| true,
        )
    }
}

/// Why a read-only open could not be served.
#[derive(Debug)]
pub enum ReadOnlyOpenError {
    /// The catalog was not shut down cleanly; redb must write to repair it.
    RepairRequired,
    /// The file could not be opened at all.
    Open(String),
}

impl std::fmt::Display for ReadOnlyOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RepairRequired => {
                write!(f, "catalog requires repair; cannot be opened read-only")
            }
            Self::Open(detail) => write!(f, "open catalog read-only: {detail}"),
        }
    }
}

impl std::error::Error for ReadOnlyOpenError {}

// SPDX-License-Identifier: BUSL-1.1

//! Catalog handle for boot steps that only read.
//!
//! redb commits its allocator state when a read-write `Database` drops, so a
//! boot step holding a read-write handle rewrites `system.redb` even when it
//! only read from it. These steps take [`ReadOnlySystemCatalog`] instead, and
//! fall back to a read-write open only when redb reports the file needs
//! repair — repairing has to write, and refusing to boot would be worse.

use std::path::Path;

use nodedb_types::DatabaseId;
use tracing::warn;

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::security::catalog::{
    ReadOnlyOpenError, ReadOnlySystemCatalog, StoredCollection, SystemCatalog,
};

/// A catalog opened for a read-only boot step.
pub(crate) enum CatalogForRead {
    /// The normal path: cannot modify the file.
    ReadOnly(ReadOnlySystemCatalog),
    /// The catalog needed repair, so it had to be opened read-write.
    Repaired(SystemCatalog),
}

/// Why a catalog that EXISTS could not be opened.
///
/// Separate from "there is no catalog", which is [`CatalogForRead::open`]
/// returning `Ok(None)`. Collapsing the two is what made a redb lock conflict
/// indistinguishable from a fresh data directory: both yielded `None`, every
/// boot loader read that as "nothing to load", and cores came up with an empty
/// schema registry that looked exactly like a first start.
#[derive(Debug)]
pub(crate) struct CatalogOpenFailure {
    pub detail: String,
}

impl std::fmt::Display for CatalogOpenFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.detail)
    }
}

impl CatalogForRead {
    /// Open `path` for reading.
    ///
    /// - `Ok(None)` — no catalog exists yet. A legitimate fresh start; a caller
    ///   may safely treat it as "nothing to load".
    /// - `Err(_)` — a catalog exists and could not be read: locked by another
    ///   handle in this process, corrupt, or unreadable. NOT a fresh start, and
    ///   a caller that degrades to empty here degrades silently on a real fault.
    ///
    /// The distinction is the point. redb is single-writer, so a second open
    /// while another handle is alive fails — and a boot loader that cannot tell
    /// that from an absent catalog seeds nothing and reports success.
    pub(crate) fn open(path: &Path) -> Result<Option<Self>, CatalogOpenFailure> {
        match ReadOnlySystemCatalog::open(path) {
            Ok(Some(catalog)) => Ok(Some(Self::ReadOnly(catalog))),
            Ok(None) => Ok(None),
            Err(ReadOnlyOpenError::RepairRequired) => {
                warn!(
                    path = %path.display(),
                    "catalog was not shut down cleanly; opening read-write to repair it"
                );
                match SystemCatalog::open(path) {
                    Ok(catalog) => Ok(Some(Self::Repaired(catalog))),
                    Err(error) => Err(CatalogOpenFailure {
                        detail: format!("failed to open catalog for repair: {error}"),
                    }),
                }
            }
            Err(error) => Err(CatalogOpenFailure {
                detail: format!("failed to open catalog read-only: {error}"),
            }),
        }
    }

    pub(crate) fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        match self {
            Self::ReadOnly(c) => c.load_all_arrays(),
            Self::Repaired(c) => c.load_all_arrays(),
        }
    }

    pub(crate) fn load_wal_tombstones(&self) -> crate::Result<nodedb_wal::TombstoneSet> {
        match self {
            Self::ReadOnly(c) => c.load_wal_tombstones(),
            Self::Repaired(c) => c.load_wal_tombstones(),
        }
    }

    pub(crate) fn list_all_vector_index_params(
        &self,
    ) -> crate::Result<Vec<nodedb_types::StoredVectorIndexParams>> {
        match self {
            Self::ReadOnly(c) => c.list_all_vector_index_params(),
            Self::Repaired(c) => c.list_all_vector_index_params(),
        }
    }

    pub(crate) fn load_custom_crs_set(&self) -> crate::Result<nodedb_crs::CustomCrsSet> {
        match self {
            Self::ReadOnly(c) => c.load_custom_crs_set(),
            Self::Repaired(c) => Ok(nodedb_crs::CustomCrsSet::clone(&c.custom_crs_set())),
        }
    }
}

impl CollectionSource for CatalogForRead {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
        match self {
            Self::ReadOnly(c) => c.list_database_ids(),
            Self::Repaired(c) => c.list_database_ids(),
        }
    }

    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
        match self {
            Self::ReadOnly(c) => c.collections_in(database_id),
            Self::Repaired(c) => c.collections_in(database_id),
        }
    }

    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        match self {
            Self::ReadOnly(c) => c.collections_for_tenant(database_id, tenant_id),
            Self::Repaired(c) => c.collections_for_tenant(database_id, tenant_id),
        }
    }
}

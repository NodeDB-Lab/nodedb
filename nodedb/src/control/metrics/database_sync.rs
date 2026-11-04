// SPDX-License-Identifier: BUSL-1.1

//! Bind `DatabaseMetricsRegistry` entries to catalog databases.
//!
//! Entries are created when a database is created or restored, and at
//! startup for every listed database. Only the drop of a database removes
//! its entry. A scrape reads names and never adds or removes an entry, so a
//! stale catalog read cannot resurrect or prune a series.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use super::database::DatabaseMetricsRegistry;
use crate::control::security::catalog::SystemCatalog;

/// Ensure an entry for every database the catalog lists. Called at startup.
pub fn ensure_listed(
    registry: &DatabaseMetricsRegistry,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    for db in catalog.list_databases()? {
        registry.ensure_database(db.id);
    }
    Ok(())
}

/// The id-to-name map used to render database labels.
pub fn database_names(catalog: &SystemCatalog) -> crate::Result<HashMap<DatabaseId, String>> {
    Ok(catalog
        .list_databases()?
        .into_iter()
        .map(|db| (db.id, db.name))
        .collect())
}

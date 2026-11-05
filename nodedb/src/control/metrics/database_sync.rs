// SPDX-License-Identifier: BUSL-1.1

//! Bind `DatabaseMetricsRegistry` entries to catalog databases.
//!
//! The `PutDatabase` and `DeleteDatabase` post-apply hooks create and remove
//! entries on every node. Startup and a metadata image install reconcile the
//! registry with the catalog. A scrape reads names and never adds or removes
//! an entry, so a stale catalog read cannot resurrect or prune a series.

use std::collections::{HashMap, HashSet};

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

/// Make the registry hold exactly the databases the catalog lists.
/// Called after a metadata image replaced the catalog.
pub fn reconcile(registry: &DatabaseMetricsRegistry, catalog: &SystemCatalog) -> crate::Result<()> {
    let listed: HashSet<DatabaseId> = catalog
        .list_databases()?
        .into_iter()
        .map(|d| d.id)
        .collect();
    for id in &listed {
        registry.ensure_database(*id);
    }
    let mut w = registry.counters.write().unwrap_or_else(|p| p.into_inner());
    w.retain(|id, _| listed.contains(id));
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

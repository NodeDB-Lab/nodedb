// SPDX-License-Identifier: BUSL-1.1

//! Source-collection → materialized-sum bindings index, cached per schema
//! version.
//!
//! Bindings are declared on the TARGET collection's catalog row, so finding the
//! ones a SOURCE collection drives means reading every collection in the
//! tenant. Doing that per write would put a full catalog scan on the insert
//! path; instead the whole node's binding set is derived ONCE per schema
//! version (bumped by every DDL) and served from memory afterwards.
//!
//! The overwhelming majority of deployments declare no bindings at all. That
//! case costs one atomic load, one read-lock, and one hash probe that misses —
//! no catalog access, no allocation.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_physical::physical_plan::MaterializedSumBinding;

use crate::control::security::catalog::SystemCatalog;
use crate::types::{DatabaseId, TenantId};

/// Identifies one `(database, tenant)` namespace.
type Namespace = (u64, u64);

/// Source collection name → the bindings it drives, within one namespace.
type BySource = HashMap<String, Arc<Vec<MaterializedSumBinding>>>;

/// The binding set derived from one schema version of the catalog.
///
/// Keyed namespace-first so a probe costs two borrowed hash lookups and NO
/// allocation — the write path must not allocate a composite key just to
/// discover that a collection drives nothing.
#[derive(Default)]
struct Snapshot {
    /// Schema version this was derived from. `0` = never built.
    schema_version: u64,
    /// Only namespaces that declare at least one binding appear here.
    by_namespace: HashMap<Namespace, BySource>,
}

/// Node-wide cache of materialized-sum bindings keyed by source collection.
#[derive(Default)]
pub struct MaterializedSumIndex {
    snapshot: RwLock<Snapshot>,
}

impl MaterializedSumIndex {
    /// Bindings driven by `collection` as their SOURCE, rebuilding the cache
    /// first if the catalog has changed since it was derived.
    ///
    /// `collection` is the catalog name (no database prefix). Returns `None`
    /// when this collection drives no binding — the common case, and the one
    /// that must stay free of any further work.
    pub fn bindings_for_source(
        &self,
        catalog: &SystemCatalog,
        schema_version: u64,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<Option<Arc<Vec<MaterializedSumBinding>>>> {
        let namespace = (database_id.as_u64(), tenant_id.as_u64());

        {
            let snapshot = self.read_snapshot();
            if snapshot.schema_version == schema_version {
                return Ok(lookup(&snapshot, namespace, collection));
            }
        }

        // Stale (or never built): rebuild under the write lock. A concurrent
        // rebuilder may have finished while we waited, so re-check the version
        // before paying for the scan again.
        let by_namespace = Self::build(catalog)?;
        let mut snapshot = self.write_snapshot();
        if snapshot.schema_version != schema_version {
            snapshot.schema_version = schema_version;
            snapshot.by_namespace = by_namespace;
        }
        Ok(lookup(&snapshot, namespace, collection))
    }

    /// Drop the cached snapshot so the next read rebuilds it. Used by tests
    /// that mutate the catalog without bumping the schema version.
    pub fn invalidate(&self) {
        let mut snapshot = self.write_snapshot();
        snapshot.schema_version = 0;
        snapshot.by_namespace.clear();
    }

    fn read_snapshot(&self) -> std::sync::RwLockReadGuard<'_, Snapshot> {
        self.snapshot.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write_snapshot(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.snapshot.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Invert the catalog: every target collection's `materialized_sums` entry
    /// is filed under the SOURCE collection that drives it.
    fn build(catalog: &SystemCatalog) -> crate::Result<HashMap<Namespace, BySource>> {
        let mut raw: HashMap<Namespace, HashMap<String, Vec<MaterializedSumBinding>>> =
            HashMap::new();
        for target in catalog.load_all_collections_across_databases()? {
            for def in &target.materialized_sums {
                let namespace = (target.database_id.as_u64(), target.tenant_id);
                raw.entry(namespace)
                    .or_default()
                    .entry(def.source_collection.clone())
                    .or_default()
                    .push(MaterializedSumBinding {
                        target_collection: def.target_collection.clone(),
                        target_column: def.target_column.clone(),
                        join_column: def.join_column.clone(),
                        value_expr: def.value_expr.clone(),
                        // `target` is the collection the binding writes into,
                        // so its declared key names the target row.
                        declared_primary_key: target.declared_primary_key.clone(),
                    });
            }
        }
        Ok(raw
            .into_iter()
            .map(|(namespace, sources)| {
                let sources: BySource = sources
                    .into_iter()
                    .map(|(source, bindings)| (source, Arc::new(bindings)))
                    .collect();
                (namespace, sources)
            })
            .collect())
    }
}

/// Two borrowed hash probes, no allocation.
fn lookup(
    snapshot: &Snapshot,
    namespace: Namespace,
    collection: &str,
) -> Option<Arc<Vec<MaterializedSumBinding>>> {
    snapshot
        .by_namespace
        .get(&namespace)?
        .get(collection)
        .cloned()
}

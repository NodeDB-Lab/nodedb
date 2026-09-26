// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot capture of the in-memory engines: vectors and their index
//! config, KV tables, CRDT state and constraints, and timeseries memtables.
//!
//! Every section is all or nothing. A section that fails to export fails the
//! whole snapshot: a snapshot missing it would restore without it.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantDataSnapshot, TenantId};

/// The error for a section that failed to export.
fn capture_error(section: &str, key: &str, detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("snapshot: {section} '{key}' failed to export: {detail}"),
    }
}

/// `"{db}:{tid}:{collection}"`, the key every in-memory section carries.
fn scoped_key(database_id: DatabaseId, tenant: TenantId, collection: &str) -> String {
    format!(
        "{}:{}:{}",
        database_id.as_u64(),
        tenant.as_u64(),
        collection
    )
}

impl CoreLoop {
    /// Fill `snapshot`'s in-memory sections with `tenant`'s state.
    pub(super) fn capture_memory_engines(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        self.capture_vectors(tenant, snapshot)?;
        self.capture_kv_tables(tenant, snapshot)?;
        self.capture_crdt(database_id, tenant, snapshot)?;
        self.capture_timeseries_memtables(tenant, snapshot)
    }

    /// Raw vectors, HNSW params and index config per collection. The HNSW
    /// graph is rebuilt from the raw vectors on restore.
    fn capture_vectors(
        &self,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for (key, collection) in &self.vector_collections {
            if key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let vectors = collection
                .export_snapshot()
                .map_err(|e| capture_error("vector collection", &key_str, e))?;
            let bytes = zerompk::to_msgpack_vec(&vectors)
                .map_err(|e| capture_error("vector collection", &key_str, e))?;
            snapshot.vectors.push((key_str, bytes));
        }
        for (key, params) in &self.vector_params {
            if key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let bytes = zerompk::to_msgpack_vec(params)
                .map_err(|e| capture_error("vector params", &key_str, e))?;
            snapshot.vector_params.push((key_str, bytes));
        }
        for (key, cfg) in &self.index_configs {
            if key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let bytes = zerompk::to_msgpack_vec(cfg)
                .map_err(|e| capture_error("vector index config", &key_str, e))?;
            snapshot.index_configs.push((key_str, bytes));
        }
        Ok(())
    }

    /// Every live entry of every KV table of `tenant`, keyed by the table's
    /// stored collection name.
    fn capture_kv_tables(
        &self,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for (&hash, table) in &self.kv_engine.tables {
            let Some(&tid) = self.kv_engine.hash_to_tenant.get(&hash) else {
                continue;
            };
            if tid != tenant.as_u64() {
                continue;
            }
            let collection_name = self
                .kv_engine
                .hash_to_collection
                .get(&hash)
                .cloned()
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "snapshot: KV table {hash} of tenant {tid} has no collection name"
                    ),
                })?;
            let bytes = zerompk::to_msgpack_vec(&table.export_entries())
                .map_err(|e| capture_error("KV table", &collection_name, e))?;
            snapshot.kv_tables.push((collection_name, bytes));
        }
        Ok(())
    }

    /// One Loro export per CRDT collection, plus each collection's installed
    /// constraint set and version.
    fn capture_crdt(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        let Some(crdt) = self.crdt_engines.get(&(database_id, tenant)) else {
            return Ok(());
        };
        for (collection, bytes) in crdt.export_all_snapshots()? {
            snapshot
                .crdt_state
                .push((database_id.as_u64(), tenant.as_u64(), collection, bytes));
        }
        // A snapshot-installed follower with no constraint set fences every
        // peer delta on a constrained collection, so the set travels too.
        for collection in crdt.collections_with_constraints() {
            let version = crdt.installed_constraint_version(&collection);
            if version == 0 {
                continue;
            }
            let constraints = crdt
                .constraints_for_collection(&collection)
                .iter()
                .map(zerompk::to_msgpack_vec)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| capture_error("CRDT constraint set", &collection, e))?;
            if constraints.is_empty() {
                continue;
            }
            snapshot
                .crdt_constraints
                .push(crate::types::snapshot::CrdtConstraintEntry {
                    database_id: database_id.as_u64(),
                    tenant_id: tenant.as_u64(),
                    collection,
                    version,
                    constraints,
                });
        }
        Ok(())
    }

    /// The column data of every timeseries memtable of `tenant`.
    fn capture_timeseries_memtables(
        &self,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for ((d, t, coll), mt) in &self.columnar_memtables {
            if *t != tenant {
                continue;
            }
            let key_str = scoped_key(*d, *t, coll);
            let bytes = zerompk::to_msgpack_vec(&mt.export_snapshot())
                .map_err(|e| capture_error("timeseries memtable", &key_str, e))?;
            snapshot.timeseries.push((key_str, bytes));
        }
        Ok(())
    }
}

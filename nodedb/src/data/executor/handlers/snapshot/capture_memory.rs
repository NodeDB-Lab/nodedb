// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot capture of the in-memory engines: vectors with their index
//! config and history, sparse vector indexes, spatial R-trees, KV tables,
//! CRDT state and constraints, and timeseries memtables. Every index section
//! carries the index's system-time history, so a follower that installs the
//! snapshot answers a read at any retained cut as the source does.
//!
//! Every section is all or nothing. A section that fails to export fails the
//! whole snapshot: a snapshot missing it would restore without it.
//!
//! A snapshot covers one database. Every section keeps only the state of the
//! database the snapshot request names, so a backup that snapshots each of a
//! tenant's databases captures each row exactly once.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, FieldIndexImage, TenantDataSnapshot, TenantId, VectorIndexHistory};

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

/// Encode `image` into `section` under its section key.
fn push_image(
    section: &mut Vec<(String, Vec<u8>)>,
    what: &str,
    image: &FieldIndexImage,
) -> crate::Result<()> {
    let key = image.section_key();
    let bytes = zerompk::to_msgpack_vec(image)
        .map_err(|e| capture_error(what, &format!("{key}:{}", image.field), e))?;
    section.push((key, bytes));
    Ok(())
}

impl CoreLoop {
    /// Fill `snapshot`'s in-memory sections with `tenant`'s state in
    /// `database_id`.
    pub(super) fn capture_memory_engines(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        self.capture_vectors(database_id, tenant, snapshot)?;
        self.capture_sparse_vectors(database_id, tenant, snapshot)?;
        self.capture_spatial_indexes(database_id, tenant, snapshot)?;
        self.capture_kv_tables(database_id, tenant, snapshot)?;
        self.capture_crdt(database_id, tenant, snapshot)?;
        self.capture_timeseries_memtables(database_id, tenant, snapshot)
    }

    /// Live vectors, their history, HNSW params and index config per
    /// collection. The HNSW graph is rebuilt from the raw vectors on restore.
    fn capture_vectors(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for (key, collection) in &self.vector_collections {
            if key.0 != database_id || key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let vectors = collection
                .export_snapshot()
                .map_err(|e| capture_error("vector collection", &key_str, e))?;
            let bytes = zerompk::to_msgpack_vec(&vectors)
                .map_err(|e| capture_error("vector collection", &key_str, e))?;
            let history = VectorIndexHistory {
                live_starts: collection.live_node_starts(),
                ended: collection
                    .export_ended_nodes()
                    .map_err(|e| capture_error("vector history", &key_str, e))?,
            };
            if history != VectorIndexHistory::default() {
                let history_bytes = zerompk::to_msgpack_vec(&history)
                    .map_err(|e| capture_error("vector history", &key_str, e))?;
                snapshot
                    .vector_versions
                    .push((key_str.clone(), history_bytes));
            }
            let documents = collection.multi_vector_documents();
            if !documents.is_empty() {
                snapshot
                    .vector_multi_documents
                    .push((key_str.clone(), documents));
            }
            snapshot.vectors.push((key_str, bytes));
        }
        for (key, params) in &self.vector_params {
            if key.0 != database_id || key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let bytes = zerompk::to_msgpack_vec(params)
                .map_err(|e| capture_error("vector params", &key_str, e))?;
            snapshot.vector_params.push((key_str, bytes));
        }
        for (key, cfg) in &self.index_configs {
            if key.0 != database_id || key.1 != tenant {
                continue;
            }
            let key_str = scoped_key(key.0, key.1, &key.2);
            let bytes = zerompk::to_msgpack_vec(cfg)
                .map_err(|e| capture_error("vector index config", &key_str, e))?;
            snapshot.index_configs.push((key_str, bytes));
        }
        Ok(())
    }

    /// Every sparse vector index of `tenant` in `database_id`, history
    /// included.
    fn capture_sparse_vectors(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for ((db, tid, collection, field), index) in &self.sparse_vector_indexes {
            if *db != database_id || *tid != tenant {
                continue;
            }
            let image = FieldIndexImage {
                database_id: db.as_u64(),
                tenant_id: tid.as_u64(),
                collection: collection.clone(),
                field: field.clone(),
                index: index.checkpoint_to_bytes()?,
                doc_entries: Vec::new(),
            };
            push_image(&mut snapshot.sparse_vectors, "sparse vector index", &image)?;
        }
        Ok(())
    }

    /// Every spatial R-tree of `tenant` in `database_id`, history and entry
    /// map included.
    fn capture_spatial_indexes(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for ((db, tid, collection, field), rtree) in &self.spatial_indexes {
            if *db != database_id || *tid != tenant {
                continue;
            }
            let mut doc_entries: Vec<(u64, String)> = self
                .spatial_doc_map
                .iter()
                .filter(|((d, t, c, f, _), _)| d == db && t == tid && c == collection && f == field)
                .map(|((_, _, _, _, entry_id), doc_id)| (*entry_id, doc_id.clone()))
                .collect();
            doc_entries.sort_unstable();
            let image = FieldIndexImage {
                database_id: db.as_u64(),
                tenant_id: tid.as_u64(),
                collection: collection.clone(),
                field: field.clone(),
                index: rtree.checkpoint_to_bytes(None)?,
                doc_entries,
            };
            push_image(&mut snapshot.spatial_indexes, "spatial index", &image)?;
        }
        Ok(())
    }

    /// Every live entry of every KV table of `tenant` in `database_id`, keyed
    /// `"{db}:{tid}:{collection}"` with the table's stored, database-qualified
    /// collection name.
    fn capture_kv_tables(
        &self,
        database_id: DatabaseId,
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
            // The KV engine keys a table by its database-qualified name.
            if super::restore::database_id_from_qualified(&collection_name)? != database_id.as_u64()
            {
                continue;
            }
            let key_str = scoped_key(database_id, tenant, &collection_name);
            let bytes = zerompk::to_msgpack_vec(&table.export_snapshot())
                .map_err(|e| capture_error("KV table", &key_str, e))?;
            snapshot.kv_tables.push((key_str, bytes));
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

    /// The column data of every timeseries memtable of `tenant` in
    /// `database_id`, and every series catalog. Boot loads the catalog of
    /// every collection on disk, so memory holds them all.
    fn capture_timeseries_memtables(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        snapshot: &mut TenantDataSnapshot,
    ) -> crate::Result<()> {
        for ((d, t, coll), mt) in &self.columnar_memtables {
            if *d != database_id || *t != tenant {
                continue;
            }
            let key_str = scoped_key(*d, *t, coll);
            let bytes = zerompk::to_msgpack_vec(&mt.export_snapshot())
                .map_err(|e| capture_error("timeseries memtable", &key_str, e))?;
            snapshot.timeseries.push((key_str, bytes));
        }
        // Every catalog of the tenant, whether its rows sit in a memtable or
        // in flushed partitions: the rows of both carry its ids.
        for ((d, t, coll), catalog) in &self.ts_series_catalogs {
            if *d != database_id || *t != tenant {
                continue;
            }
            let bytes =
                crate::data::executor::timeseries_checkpoint::series_catalog::encode_ts_series_catalog(
                    catalog,
                )?;
            snapshot
                .ts_series_catalogs
                .push((scoped_key(*d, *t, coll), bytes));
        }
        Ok(())
    }
}

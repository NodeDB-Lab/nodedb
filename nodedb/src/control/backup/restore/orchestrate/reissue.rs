// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of columnar, timeseries, and vector rows drained from the
//! merged backup snapshot (see [`super::restore_tenant`]).

use std::sync::Arc;

use nodedb_types::surrogate::Surrogate;

use crate::Error;
use crate::control::state::SharedState;
use crate::engine::vector::index_config::IndexConfig;
use crate::types::TenantId;

use crate::control::backup::snapshot_keys::extract_db_scoped_collection;

/// Decode and durably re-issue every restored timeseries collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `memtables` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the captured `MemtableSnapshot` wire shape); `flushed` carries the flushed
/// partition blobs keyed by the same `"{db}:{tid}:{collection}"` key. The union
/// of the two key sets is re-issued once per collection (memtable + flushed rows
/// merged into a single ingest).
pub(super) async fn reissue_timeseries_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    memtables: Vec<(String, Vec<u8>)>,
    flushed: Vec<crate::types::TsFlushedCollectionBlob>,
) -> Result<usize, Error> {
    // Timeseries segment KEK == the WAL encryption key (segments are written via
    // the same key). Absent when at-rest encryption is not configured, in which
    // case segments are plaintext and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();
    let database_id = crate::types::DatabaseId::DEFAULT;

    // Index memtable bytes and flushed blobs by their `{db}:{tid}:{collection}`
    // key so each collection is decoded + re-issued exactly once.
    let mut memtable_by_key: std::collections::HashMap<String, Vec<u8>> =
        memtables.into_iter().collect();
    let mut keys_in_order: Vec<String> = Vec::new();
    let mut flushed_by_key: std::collections::HashMap<
        String,
        crate::types::TsFlushedCollectionBlob,
    > = std::collections::HashMap::new();
    for blob in flushed {
        keys_in_order.push(blob.collection_key.clone());
        flushed_by_key.insert(blob.collection_key.clone(), blob);
    }
    for key in memtable_by_key.keys() {
        if !flushed_by_key.contains_key(key) {
            keys_in_order.push(key.clone());
        }
    }

    let empty_flushed = crate::types::TsFlushedCollectionBlob::default();
    let mut reissued = 0usize;
    for key in keys_in_order {
        let Some(collection) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed timeseries snapshot key '{key}'"),
            });
        };
        let collection = collection.to_owned();

        let memtable_bytes = memtable_by_key.remove(&key);
        let flushed_blob = flushed_by_key.get(&key).unwrap_or(&empty_flushed);

        let rows = super::super::timeseries_reissue::decode_timeseries_live_rows(
            &collection,
            memtable_bytes.as_deref(),
            flushed_blob,
            kek.as_ref(),
        )?;
        if rows.is_empty() {
            continue;
        }

        let plan =
            super::super::timeseries_reissue::build_timeseries_ingest_plan(&collection, rows)?;
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

/// Decode and durably re-issue every restored plain-columnar collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `entries` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the `ColumnarEngineSnapshot` wire shape).
pub(super) async fn reissue_columnar_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    entries: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    // Columnar segment KEK == the WAL encryption key (segments are written via
    // `SegmentWriter::new(profile, memory).write_segment(..., kek)` with this
    // key). Absent when at-rest encryption is not configured, in which case
    // segments are plaintext NDBS and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();
    let database_id = crate::types::DatabaseId::DEFAULT;

    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let Some(collection) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed columnar snapshot key '{key}'"),
            });
        };
        let collection = collection.to_owned();

        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize ColumnarEngineSnapshot for '{collection}': {e}"
                ),
            })?;

        let decoded = super::super::columnar_reissue::decode_snapshot_live_rows(
            &collection,
            snap,
            kek.as_ref(),
        )?;
        if decoded.rows.is_empty() {
            continue;
        }

        let plan =
            super::super::columnar_reissue::build_columnar_insert_plan(&collection, decoded)?;
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

/// Decode and durably re-issue every restored vector as an individual
/// `VectorOp::Insert`.
///
/// Returns the number of vectors re-issued. Unlike columnar/timeseries,
/// `VectorOp::Insert` is a single-row op (there is no named-field-aware batch
/// variant), so — unlike the collection-level counts above — this counts
/// individual vectors, one re-issue per restored row.
///
/// `entries` are `("{db}:{tid}:{coll_key}", msgpack)` pairs where `coll_key`
/// is `collection` or `collection:field_name` (see
/// `CoreLoop::vector_index_key`) and the payload decodes to
/// `Vec<(u32, Vec<f32>, Option<Surrogate>)>` — the raw HNSW export shape
/// (`node_id`, vector data, surrogate) `VectorCollection::export_snapshot`
/// produces.
pub(super) async fn reissue_vector_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    entries: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let database_id = crate::types::DatabaseId::DEFAULT;

    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let Some(coll_key) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed vector snapshot key '{key}'"),
            });
        };
        let (collection, field_name) =
            super::super::vector_reissue::split_vector_coll_key(coll_key);
        let collection = collection.to_owned();
        let field_name = field_name.to_owned();

        let vectors: Vec<(u32, Vec<f32>, Option<Surrogate>)> = zerompk::from_msgpack(&bytes)
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize vector snapshot for '{collection}': {e}"
                ),
            })?;
        if vectors.is_empty() {
            continue;
        }

        for (_node_id, vector, surrogate) in vectors {
            let surrogate = surrogate.unwrap_or(Surrogate::ZERO);
            let plan = super::super::vector_reissue::build_vector_insert_plan(
                &collection,
                &field_name,
                vector,
                surrogate,
            );
            super::super::durable::reissue_plan_durably(
                state,
                TenantId::new(tenant_id),
                database_id,
                &collection,
                plan,
            )
            .await?;
            reissued += 1;
        }
    }
    Ok(reissued)
}

/// Decode and durably re-issue every restored vector-index (collection,
/// field) HNSW/PQ/IVF configuration as a `VectorOp::SetParams`.
///
/// MUST run before [`reissue_vector_snapshots`]: `get_or_create_vector_index`
/// (`handlers/vector.rs`) lazily creates the Data Plane HNSW index from
/// `self.vector_params` on the FIRST `VectorOp::Insert` it sees for a
/// (collection, field) — falling back to `HnswParams::default()` when no
/// `SetParams` has landed yet. Re-issuing params after inserts would be a
/// no-op for the already-created index.
///
/// `params` are `("{db}:{tid}:{coll_key}", msgpack)` pairs decoding to
/// `HnswParams` (the `TenantDataSnapshot::vector_params` wire shape);
/// `index_configs` are the same key shape decoding to `IndexConfig` (the
/// superset — HNSW params + index type + PQ/IVF params). A (collection,
/// field) present in both is re-issued once using the `IndexConfig` entry
/// (the superset); a (collection, field) present only in `params` is
/// re-issued with the rest of `IndexConfig` left at its default (matching
/// what `execute_set_vector_params` does for unspecified fields). Returns the
/// number of (collection, field) configs re-issued. Any failure is fatal — no
/// warn-and-continue.
pub(super) async fn reissue_vector_params(
    state: &Arc<SharedState>,
    tenant_id: u64,
    params: Vec<(String, Vec<u8>)>,
    index_configs: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let database_id = crate::types::DatabaseId::DEFAULT;

    let mut resolved: std::collections::HashMap<String, IndexConfig> =
        std::collections::HashMap::new();
    let mut keys_in_order: Vec<String> = Vec::new();

    for (key, bytes) in index_configs {
        let Some(coll_key) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed index_configs snapshot key '{key}'"),
            });
        };
        let coll_key = coll_key.to_owned();
        let cfg: IndexConfig = zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: deserialize IndexConfig for '{coll_key}': {e}"),
        })?;
        keys_in_order.push(coll_key.clone());
        resolved.insert(coll_key, cfg);
    }

    for (key, bytes) in params {
        let Some(coll_key) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed vector_params snapshot key '{key}'"),
            });
        };
        let coll_key = coll_key.to_owned();
        if resolved.contains_key(&coll_key) {
            // Superseded by a full IndexConfig entry for the same (collection,
            // field) — the two sections always describe the same DDL state,
            // so skip the narrower one.
            continue;
        }
        let hnsw: nodedb_types::hnsw::HnswParams =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("restore reissue: deserialize HnswParams for '{coll_key}': {e}"),
            })?;
        keys_in_order.push(coll_key.clone());
        resolved.insert(
            coll_key,
            IndexConfig {
                hnsw,
                ..IndexConfig::default()
            },
        );
    }

    let mut reissued = 0usize;
    for coll_key in keys_in_order {
        let Some(config) = resolved.remove(&coll_key) else {
            // Already re-issued (duplicate key across sections).
            continue;
        };
        let (collection, field_name) =
            super::super::vector_reissue::split_vector_coll_key(&coll_key);
        let collection = collection.to_owned();
        let field_name = field_name.to_owned();

        let plan = super::super::vector_reissue::build_vector_set_params_plan(
            &collection,
            &field_name,
            &config,
        );
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

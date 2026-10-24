// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of columnar, timeseries, and vector rows drained from one
//! database's merged backup snapshot (see [`super::restore_tenant`]).
//!
//! Every section key names a collection as the source Data Plane stored it.
//! Each re-issue resolves it through the [`DatabaseTarget`]: the plan names
//! the destination-qualified collection, and the write routes by the bare
//! name in the destination database.

use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp, VectorOp};
use nodedb_types::surrogate::Surrogate;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::engine::vector::index_config::IndexConfig;
use crate::types::{SurrogateBindEntry, TenantId};

use super::super::target::{DatabaseTarget, RestoredName};
use super::super::vector_reissue;

/// Durably clear a destination collection of an append-only engine before its
/// rows re-issue.
///
/// A columnar or timeseries ingest appends, so a second re-issue of the same
/// rows, after a failed restore or cutover, holds every row twice. The
/// re-issue replaces the collection's contents instead: it clears the
/// collection, then writes the captured rows. `truncate` is the engine's
/// whole-collection truncate of `name`.
async fn clear_before_append(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    name: &RestoredName,
    truncate: PhysicalPlan,
) -> Result<(), Error> {
    super::super::durable::reissue_plan_durably(
        state,
        TenantId::new(tenant_id),
        target,
        &name.bare,
        truncate,
    )
    .await
}

/// Decode and durably re-issue every restored timeseries collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `memtables` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the captured `MemtableSnapshot` wire shape); `flushed` carries the flushed
/// partition blobs keyed by the same `"{db}:{tid}:{collection}"` key. The union
/// of the two key sets is re-issued once per collection (memtable + flushed rows
/// merged into a single ingest).
pub(super) async fn reissue_timeseries_snapshots(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    memtables: Vec<(String, Vec<u8>)>,
    flushed: Vec<crate::types::TsFlushedCollectionBlob>,
) -> Result<usize, Error> {
    // Timeseries segment KEK == the WAL encryption key (segments are written via
    // the same key). Absent when at-rest encryption is not configured, in which
    // case segments are plaintext and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();

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
        let name = target.resolve_scoped(&key, tenant_id)?;

        let memtable_bytes = memtable_by_key.remove(&key);
        let flushed_blob = flushed_by_key.get(&key).unwrap_or(&empty_flushed);

        let rows = super::super::timeseries_reissue::decode_timeseries_live_rows(
            &name.bare,
            memtable_bytes.as_deref(),
            flushed_blob,
            kek.as_ref(),
        )?;
        if rows.is_empty() {
            continue;
        }

        let plan = super::super::timeseries_reissue::build_timeseries_ingest_plan(
            name.stored.as_str(),
            rows,
        )?;
        clear_before_append(
            state,
            tenant_id,
            target,
            &name,
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: name.stored.clone(),
                restart_identity: false,
            }),
        )
        .await?;
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            target,
            &name.bare,
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
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    entries: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    // Columnar segment KEK == the WAL encryption key (segments are written via
    // `SegmentWriter::new(profile, memory).write_segment(..., kek)` with this
    // key). Absent when at-rest encryption is not configured, in which case
    // segments are plaintext NDBS and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();

    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let name = target.resolve_scoped(&key, tenant_id)?;

        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize ColumnarEngineSnapshot for '{}': {e}",
                    name.bare
                ),
            })?;

        let decoded = super::super::columnar_reissue::decode_snapshot_live_rows(
            &name.bare,
            snap,
            kek.as_ref(),
        )?;
        if decoded.rows.is_empty() {
            continue;
        }

        let plan = super::super::columnar_reissue::build_columnar_insert_plan(
            name.stored.as_str(),
            decoded,
        )?;
        clear_before_append(
            state,
            tenant_id,
            target,
            &name,
            PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: name.stored.clone(),
                restart_identity: false,
                system_from: None,
                // The restore reinstalls every version under the system time
                // it carries, so the old history goes, bitemporal or not.
                erase_history: true,
            }),
        )
        .await?;
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            target,
            &name.bare,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

/// Decode and durably re-issue every restored vector: a single-vector row as
/// one `VectorOp::Insert`, a multi-vector document as one `MultiVectorDelete`
/// then one `MultiVectorInsert` of its full set. `multi_documents` names each
/// index's multi-vector documents, keyed like `entries`.
///
/// Returns the number of vectors re-issued, counted per vector, not per
/// collection.
///
/// `entries` are `("{db}:{tid}:{coll_key}", msgpack)` pairs where `coll_key`
/// is `collection` or `collection:field_name` (see
/// `CoreLoop::vector_index_key`) and the payload decodes to
/// `Vec<(u32, Vec<f32>, Option<Surrogate>)>` — the raw HNSW export shape
/// (`node_id`, vector data, surrogate) `VectorCollection::export_snapshot`
/// produces.
///
/// `binds` are the backup's surrogate binds. A single-vector row or a
/// multi-vector document whose surrogate the backup binds to a key re-issues
/// bound by that key.
pub(super) async fn reissue_vector_snapshots(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    entries: Vec<(String, Vec<u8>)>,
    multi_documents: Vec<(String, Vec<Surrogate>)>,
    binds: &[SurrogateBindEntry],
) -> Result<usize, Error> {
    let keys: std::collections::HashMap<(&str, u32), &[u8]> = binds
        .iter()
        .map(|bind| {
            (
                (bind.collection.as_str(), bind.surrogate),
                bind.pk.as_slice(),
            )
        })
        .collect();
    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let coll_key = target.scoped_rest(&key, tenant_id)?;
        let (collection, field_name) =
            super::super::vector_reissue::split_vector_coll_key(coll_key);
        let name = target.resolve(collection)?;
        let field_name = field_name.to_owned();

        let vectors: Vec<(u32, Vec<f32>, Option<Surrogate>)> = zerompk::from_msgpack(&bytes)
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize vector snapshot for '{}': {e}",
                    name.bare
                ),
            })?;
        if vectors.is_empty() {
            continue;
        }

        let members: std::collections::HashSet<Surrogate> = multi_documents
            .iter()
            .filter(|(members_key, _)| *members_key == key)
            .flat_map(|(_, documents)| documents.iter().copied())
            .collect();
        let grouped = vector_reissue::group_restored_vectors(vectors, &members)?;
        let tenant = TenantId::new(tenant_id);
        let stored = name.stored.as_str();
        let mut plans = Vec::new();
        for (surrogate, vector) in grouped.single {
            let pk_bytes = keys
                .get(&(collection, surrogate.as_u32()))
                .map(|pk| pk.to_vec());
            plans.push(vector_reissue::build_vector_insert_plan(
                stored,
                &field_name,
                vector,
                surrogate,
                pk_bytes,
            ));
        }
        for (surrogate, group) in grouped.multi {
            // A multi-vector document, whatever its vector count: clear it,
            // then insert its full set, so a repeated re-issue neither drops
            // nor doubles one.
            reissued += group.len();
            plans.push(vector_reissue::build_multi_vector_delete_plan(
                stored,
                &field_name,
                surrogate,
            ));
            let pk_bytes = keys
                .get(&(collection, surrogate.as_u32()))
                .map(|pk| pk.to_vec());
            plans.push(vector_reissue::build_multi_vector_insert_plan(
                stored,
                &field_name,
                surrogate,
                pk_bytes,
                group,
            ));
        }
        for plan in plans {
            if matches!(plan, PhysicalPlan::Vector(VectorOp::Insert { .. })) {
                reissued += 1;
            }
            super::super::durable::reissue_plan_durably(state, tenant, target, &name.bare, plan)
                .await?;
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
/// `SetParams` has landed yet. Re-issuing params after inserts is a
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
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    params: Vec<(String, Vec<u8>)>,
    index_configs: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let mut resolved: std::collections::HashMap<String, IndexConfig> =
        std::collections::HashMap::new();
    let mut keys_in_order: Vec<String> = Vec::new();

    for (key, bytes) in index_configs {
        let coll_key = target.scoped_rest(&key, tenant_id)?.to_owned();
        let cfg: IndexConfig = zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: deserialize IndexConfig for '{coll_key}': {e}"),
        })?;
        keys_in_order.push(coll_key.clone());
        resolved.insert(coll_key, cfg);
    }

    for (key, bytes) in params {
        let coll_key = target.scoped_rest(&key, tenant_id)?.to_owned();
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
        let name = target.resolve(collection)?;

        let plan = super::super::vector_reissue::build_vector_set_params_plan(
            name.stored.as_str(),
            field_name,
            &config,
        );
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            target,
            &name.bare,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

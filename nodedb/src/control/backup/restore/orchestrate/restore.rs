// SPDX-License-Identifier: BUSL-1.1

//! `restore_tenant`: validates a backup envelope, merges all sections into
//! a single `TenantDataSnapshot`, and re-issues every section as durable,
//! replicated writes.

use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, EnvelopeError, parse_encrypted as parse_envelope_encrypted,
};

use crate::Error;
use crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored;
use crate::control::state::SharedState;

use super::super::sections::{apply_metadata_sections, merge_sections};
use super::rebind;
use super::reissue;
use super::stats::RestoreStats;

/// Restore a tenant from a fully-buffered backup envelope.
pub async fn restore_tenant(
    state: &Arc<SharedState>,
    tenant_id: u64,
    envelope_bytes: &[u8],
    dry_run: bool,
    force: bool,
) -> Result<RestoreStats, Error> {
    let env = match &state.backup_kek {
        Some(kek) => parse_envelope_encrypted(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES, kek)?,
        None => {
            return Err(Error::Internal {
                detail: "restore: envelope is encrypted but no backup KEK is configured; \
                         set [backup_encryption] in the server config"
                    .into(),
            });
        }
    };
    if env.meta.tenant_id != tenant_id {
        return Err(EnvelopeError::TenantMismatch {
            expected: tenant_id,
            actual: env.meta.tenant_id,
        }
        .into());
    }

    // Every group the restore reads or writes has a reachable majority, or
    // the restore fails here, before it proposes anything.
    if !dry_run {
        super::super::quorum::require_quorum(state)?;
    }

    let newest = if !dry_run && env.meta.snapshot_watermark != 0 {
        super::super::guard::newest_committed_write(state, tenant_id).await?
    } else {
        None
    };
    if let Some(mark) = newest {
        let current_high_water = mark.hlc;
        if env.meta.snapshot_watermark < current_high_water {
            if force {
                tracing::warn!(
                    tenant_id,
                    envelope_watermark = env.meta.snapshot_watermark,
                    current_high_water,
                    newest_write_site = mark.site.as_str(),
                    newest_write_collection = mark.collection.as_deref().unwrap_or(""),
                    "restore staleness protection explicitly overridden via FORCE: \
                     envelope watermark is older than the destination cluster's last \
                     observed write-HLC for this tenant — newer writes will be overwritten"
                );
            } else {
                return Err(Error::Internal {
                    detail: format!(
                        "restore refused: envelope watermark {} is older than the \
                         destination cluster's last observed write-HLC {} for tenant \
                         {} (newest write: {} on collection '{}') — newer writes would \
                         be silently overwritten",
                        env.meta.snapshot_watermark,
                        current_high_water,
                        tenant_id,
                        mark.site,
                        mark.collection.as_deref().unwrap_or("<none>"),
                    ),
                });
            }
        }
    }

    let mut stats = RestoreStats {
        tenant_id,
        dry_run,
        sections: env.sections.len() as u16,
        source_vshard_count: env.meta.source_vshard_count,
        ..Default::default()
    };

    if !dry_run {
        let restored_collections = apply_metadata_sections(state, tenant_id, &env)?;
        // Every restored collection's declaration reaches this node's Data
        // Plane before any of its rows do. The catalog row alone leaves
        // `doc_configs` empty for the collection, and the re-issue below
        // would then ingest a timeseries collection's rows into an inferred
        // shape: the declared time key becomes an integer field and the row
        // is stamped with the restore-time clock. This is the same
        // registration a committed DDL and the boot rehydration dispatch,
        // and it replaces any registration already present, so a cluster
        // applier's own register hook and a later boot seed are both
        // idempotent with it. A registration failure fails the restore.
        for coll in &restored_collections {
            dispatch_register_from_stored(state, coll)
                .await
                .map_err(|e| Error::Internal {
                    detail: format!(
                        "restore: Data Plane registration of collection '{}' failed: {e}",
                        coll.name
                    ),
                })?;
        }
    }

    let mut merged = merge_sections(&env.sections)?;
    stats.documents = merged.documents.len() + merged.documents_versioned.len();
    stats.indexes = merged.indexes.len() + merged.indexes_versioned.len();
    stats.edges = merged.edges.len();
    stats.vectors = merged.vectors.len();
    stats.kv_tables = merged.kv_tables.len();
    // CRDT state is one entry per (tenant, collection).
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();
    stats.flushed_ts_segments = merged.flushed_ts_segments.len();
    stats.surrogate_pk = merged.surrogate_pk.len();

    rebind::warn_on_tombstoned_restores(state, tenant_id, &merged, env.meta.snapshot_watermark);

    if dry_run {
        stats.columnar_engines = merged.columnar_engines.len();
        return Ok(stats);
    }

    // Every section re-issues as durable writes: Raft-replicated to every
    // replica of its group in cluster mode, WAL-appended then installed on a
    // single node. None is installed straight into a Data-Plane map, which
    // would hold it on one node only and lose it on restart.
    let columnar_snapshots = std::mem::take(&mut merged.columnar_engines);
    let timeseries_memtables = std::mem::take(&mut merged.timeseries);
    let flushed_ts_segments = std::mem::take(&mut merged.flushed_ts_segments);
    let crdt_state = std::mem::take(&mut merged.crdt_state);
    let kv_tables = std::mem::take(&mut merged.kv_tables);
    let vector_snapshots = std::mem::take(&mut merged.vectors);
    // Vector-index config re-issues as `VectorOp::SetParams` before the first
    // vector `Insert`: the Data Plane creates a (collection, field) HNSW index
    // on its first `Insert`, from whatever params it holds by then.
    let vector_params_snapshots = std::mem::take(&mut merged.vector_params);
    let index_config_snapshots = std::mem::take(&mut merged.index_configs);

    // The PK→surrogate identity map. It is bound on this node before any
    // re-issue, so a re-issued row keeps the surrogate the backup stored it
    // under unless this node already binds its key.
    let surrogate_binds = std::mem::take(&mut merged.surrogate_pk);
    rebind::rebind_surrogates(state, &surrogate_binds)?;

    // Document rows, their versions and graph edges re-issue as committed
    // redo records through each collection's apply log: every replica binds
    // the rows' identities, appends the record to its WAL, installs the rows
    // and derives their secondary index entries. The backup's own index
    // entries are therefore not installed.
    let documents = std::mem::take(&mut merged.documents);
    let documents_versioned = std::mem::take(&mut merged.documents_versioned);
    let edges = std::mem::take(&mut merged.edges);
    let redo = super::super::redo_reissue::reissue_rows_and_edges(
        state,
        tenant_id,
        super::super::redo_reissue::RestoredRows {
            documents,
            documents_versioned,
            edges,
            binds: &surrogate_binds,
        },
    )
    .await?;
    stats.documents_reissued = redo.documents;
    stats.edges_reissued = redo.edges;
    stats.redo_records = redo.records;

    // Durable re-issue of plain-columnar rows. Each restored collection's live
    // rows are decoded from the snapshot and replayed as a durable
    // `ColumnarOp::Insert` (Raft-replicated in cluster mode; WAL-appended then
    // installed in single-node mode). Collections that decode to zero live rows
    // are skipped. Any failure is fatal — no warn-and-continue.
    stats.columnar_engines =
        reissue::reissue_columnar_snapshots(state, tenant_id, columnar_snapshots).await?;

    // Durable re-issue of timeseries rows. Each restored collection's memtable
    // rows plus every flushed partition's rows are decoded from the snapshot and
    // replayed as a durable `TimeseriesOp::Ingest` (Raft-replicated in cluster
    // mode; WAL-appended then installed in single-node mode). Collections that
    // decode to zero live rows are skipped. Any failure is fatal — no
    // warn-and-continue.
    stats.timeseries_reissued = reissue::reissue_timeseries_snapshots(
        state,
        tenant_id,
        timeseries_memtables,
        flushed_ts_segments,
    )
    .await?;

    // Durable re-issue of CRDT state. Each collection's Loro snapshot is
    // proposed through Raft to the data group owning that collection's vshard
    // (Raft-replicated in cluster mode; WAL-appended then installed in
    // single-node mode). Every replica applies the same idempotent Loro merge
    // and converges deterministically. Any failure is fatal — no
    // warn-and-continue.
    stats.crdt_reissued =
        super::super::crdt_reissue::reissue_crdt_snapshots(state, crdt_state).await?;

    // Durable re-issue of KV rows, one `KvOp::Put` per live row. Any failure
    // is fatal — no warn-and-continue.
    stats.kv_reissued =
        super::super::kv_reissue::reissue_kv_tables(state, tenant_id, kv_tables).await?;

    // Durable re-issue of vector-index configuration. Each restored
    // (collection, field) HNSW/PQ/IVF config is replayed as a
    // `VectorOp::SetParams` (Raft-replicated in cluster mode; WAL-appended
    // then installed in single-node mode). MUST run before the vector-insert
    // re-issue below — see the `vector_params_snapshots` drain comment
    // above. Any failure is fatal — no warn-and-continue.
    stats.vector_params_reissued = reissue::reissue_vector_params(
        state,
        tenant_id,
        vector_params_snapshots,
        index_config_snapshots,
    )
    .await?;

    // Durable re-issue of vector rows. Each restored vector is replayed as an
    // individual `VectorOp::Insert` (Raft-replicated in cluster mode;
    // WAL-appended then installed in single-node mode). Collections that
    // decode to zero vectors are skipped. Any failure is fatal — no
    // warn-and-continue.
    stats.vectors_reissued =
        reissue::reissue_vector_snapshots(state, tenant_id, vector_snapshots).await?;

    Ok(stats)
}

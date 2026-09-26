// SPDX-License-Identifier: BUSL-1.1

//! Catalog-section and data-section helpers for RESTORE TENANT.

use nodedb_types::DatabaseId;
use std::sync::Arc;

use crate::Error;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot};

pub(super) fn merge_sections(
    sections: &[nodedb_types::backup_envelope::Section],
) -> Result<TenantDataSnapshot, Error> {
    use nodedb_types::backup_envelope::{SECTION_ORIGIN_SURROGATE_PK, SurrogateBindBlob};

    let mut merged = TenantDataSnapshot::default();
    for section in sections {
        // The surrogate-pk metadata section carries the PK→surrogate identity
        // map (not a tenant snapshot); decode it into `merged.surrogate_pk` so
        // the restore orchestrator can rebind it after the data install.
        if section.origin_node_id == SECTION_ORIGIN_SURROGATE_PK {
            let binds: Vec<SurrogateBindBlob> =
                zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                    detail: "invalid backup format: surrogate-pk section payload is not decodable"
                        .into(),
                })?;
            merged
                .surrogate_pk
                .extend(binds.into_iter().map(|b| SurrogateBindEntry {
                    tenant_id: b.tenant_id,
                    collection: b.collection,
                    pk: b.pk,
                    surrogate: b.surrogate,
                }));
            continue;
        }
        if is_metadata_section(section) {
            continue;
        }
        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                detail: "invalid backup format: section payload is not a tenant snapshot".into(),
            })?;
        merged.documents.extend(snap.documents);
        merged.indexes.extend(snap.indexes);
        merged.documents_versioned.extend(snap.documents_versioned);
        merged.indexes_versioned.extend(snap.indexes_versioned);
        merged.edges.extend(snap.edges);
        merged.vectors.extend(snap.vectors);
        merged.vector_params.extend(snap.vector_params);
        merged.index_configs.extend(snap.index_configs);
        merged.kv_tables.extend(snap.kv_tables);
        // CRDT state is per-collection and tenant-explicit:
        // `(tenant_id, collection, loro_bytes)`. Loro import is a monotonic
        // merge so concatenating section contributions is safe.
        merged.crdt_state.extend(snap.crdt_state);
        merged.timeseries.extend(snap.timeseries);
        merged.flushed_ts_segments.extend(snap.flushed_ts_segments);
        merged.columnar_engines.extend(snap.columnar_engines);
        // surrogate_pk on a per-node data section (Raft snapshots carry it
        // there); merge it too so both transports converge here.
        merged.surrogate_pk.extend(snap.surrogate_pk);
    }
    Ok(merged)
}

pub(super) fn is_metadata_section(section: &nodedb_types::backup_envelope::Section) -> bool {
    matches!(
        section.origin_node_id,
        nodedb_types::backup_envelope::SECTION_ORIGIN_CATALOG_ROWS
            | nodedb_types::backup_envelope::SECTION_ORIGIN_SOURCE_TOMBSTONES
            | nodedb_types::backup_envelope::SECTION_ORIGIN_SURROGATE_PK
    )
}

/// Apply catalog-row and source-tombstone sections to the destination catalog.
/// Runs BEFORE the data-section restore.
///
/// Catalog rows are proposed cluster-wide through the metadata Raft
/// group (group 0) — exactly like CREATE COLLECTION — so every node's
/// catalog learns the restored collection and can serve it. A
/// catalog-propose failure on this path is FATAL: returning the data
/// restored but unqueryable on non-coordinator nodes is the
/// silent-partial-success anti-pattern this codebase forbids.
///
/// Returns every collection written to the catalog, in section order. The
/// caller registers each one with this node's Data Plane before any restored
/// row is installed or reissued: a catalog row alone leaves `doc_configs`
/// without the collection's declaration, and a reissued timeseries row would
/// then be ingested into an inferred shape.
pub(super) fn apply_metadata_sections(
    state: &Arc<SharedState>,
    tenant_id: u64,
    env: &nodedb_types::backup_envelope::Envelope,
) -> Result<Vec<StoredCollection>, Error> {
    use nodedb_types::backup_envelope::{
        SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_SOURCE_TOMBSTONES, SourceTombstoneEntry,
        StoredCollectionBlob,
    };
    let catalog = state.credentials.catalog();
    let mut restored: Vec<StoredCollection> = Vec::new();

    for section in &env.sections {
        match section.origin_node_id {
            SECTION_ORIGIN_CATALOG_ROWS => {
                let blobs = zerompk::from_msgpack::<Vec<StoredCollectionBlob>>(&section.body)
                    .map_err(|_| Error::Internal {
                        detail: "invalid backup format: catalog-rows section is not decodable"
                            .into(),
                    })?;
                for blob in blobs {
                    let coll =
                        zerompk::from_msgpack::<StoredCollection>(&blob.bytes).map_err(|_| {
                            Error::Internal {
                                detail: format!(
                                    "invalid backup format: catalog row of '{}' is not decodable",
                                    blob.name
                                ),
                            }
                        })?;
                    // Propose the collection through the metadata Raft
                    // group so every node's applier (`catalog_entry::
                    // apply::collection::put`) writes the row — mirroring
                    // CREATE COLLECTION and DROP COLLECTION. The proposer
                    // blocks on its local applied-index watcher, so on the
                    // cluster path it has already applied the put via the
                    // same applier — we must NOT also put locally (double-put).
                    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(
                        Box::new(coll.clone()),
                    );
                    let outcome =
                        crate::control::metadata_proposer::propose_catalog_entry(state, &entry)?;
                    if outcome.needs_local_apply() {
                        // Single-node / no-cluster fallback: apply the
                        // catalog mutation directly, matching what the
                        // applier would have done on a clustered deployment.
                        // A failure here is FATAL — the collection would be
                        // unqueryable otherwise.
                        catalog.put_collection(DatabaseId::DEFAULT, &coll)?;
                    }
                    restored.push(coll);
                }
            }
            SECTION_ORIGIN_SOURCE_TOMBSTONES => {
                let tombs = zerompk::from_msgpack::<Vec<SourceTombstoneEntry>>(&section.body)
                    .map_err(|_| Error::Internal {
                        detail: "invalid backup format: source-tombstones section is not \
                                 decodable"
                            .into(),
                    })?;
                for t in tombs {
                    // Replicate via the metadata Raft group so every node's boot WAL
                    // replay barrier matches — a coordinator-local tombstone lets purged
                    // writes resurrect on follower restart.
                    let entry = crate::control::catalog_entry::CatalogEntry::RecordWalTombstone {
                        database_id: DatabaseId::DEFAULT.as_u64(),
                        tenant_id,
                        collection: t.collection,
                        purge_lsn: t.purge_lsn,
                    };
                    let outcome =
                        crate::control::metadata_proposer::propose_catalog_entry(state, &entry)?;
                    if outcome.needs_local_apply() {
                        // Single-node / no-cluster fallback: apply directly,
                        // matching the applier. A failure here is FATAL — a
                        // silently-skipped tombstone means purged writes resurrect
                        // on restart, which is the bug this change fixes.
                        if let crate::control::catalog_entry::CatalogEntry::RecordWalTombstone {
                            collection,
                            purge_lsn,
                            ..
                        } = entry
                        {
                            catalog.record_wal_tombstone(
                                DatabaseId::DEFAULT.as_u64(),
                                tenant_id,
                                &collection,
                                purge_lsn,
                            )?;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(restored)
}

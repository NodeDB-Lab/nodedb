// SPDX-License-Identifier: BUSL-1.1

//! Catalog-section and data-section helpers for RESTORE TENANT.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_types::backup_envelope::{
    DatabaseDataSection, Envelope, SECTION_ORIGIN_ARRAY_CATALOG, SECTION_ORIGIN_CATALOG_ROWS,
    SECTION_ORIGIN_DATABASES, SECTION_ORIGIN_SOURCE_TOMBSTONES, SECTION_ORIGIN_SURROGATE_PK,
    SECTION_ORIGIN_VERIFICATION, Section, SourceTombstoneEntry, StoredCollectionBlob,
    SurrogateBindBlob,
};

use crate::Error;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot};

use super::databases::DatabaseMap;

/// Merge every data section and the surrogate-pk section into one
/// `TenantDataSnapshot` per source database, keyed by source database id.
pub(super) fn merge_sections(
    sections: &[Section],
) -> Result<BTreeMap<u64, TenantDataSnapshot>, Error> {
    let mut merged: BTreeMap<u64, TenantDataSnapshot> = BTreeMap::new();
    for section in sections {
        // The surrogate-pk metadata section carries the PK→surrogate identity
        // map (not a tenant snapshot). Each bind goes to its database's
        // snapshot, so the restore rebinds it in that database.
        if section.origin_node_id == SECTION_ORIGIN_SURROGATE_PK {
            let binds: Vec<SurrogateBindBlob> =
                zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                    detail: "invalid backup format: surrogate-pk section payload is not decodable"
                        .into(),
                })?;
            for b in binds {
                merged
                    .entry(b.database_id)
                    .or_default()
                    .surrogate_pk
                    .push(SurrogateBindEntry {
                        database_id: b.database_id,
                        tenant_id: b.tenant_id,
                        collection: b.collection,
                        pk: b.pk,
                        surrogate: b.surrogate,
                    });
            }
            continue;
        }
        if is_metadata_section(section) {
            continue;
        }
        let data: DatabaseDataSection =
            zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                detail: "invalid backup format: section payload is not a database data section"
                    .into(),
            })?;
        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&data.snapshot).map_err(|_| Error::Internal {
                detail: "invalid backup format: section payload is not a tenant snapshot".into(),
            })?;
        append_snapshot(merged.entry(data.database_id).or_default(), snap);
    }
    Ok(merged)
}

/// Concatenate every section of `snap` onto `into`. Each source node holds
/// disjoint vShards, so the sections never overlap.
pub(in crate::control::backup) fn append_snapshot(
    into: &mut TenantDataSnapshot,
    snap: TenantDataSnapshot,
) {
    // Destructure exhaustively so a new field fails to compile here rather
    // than being dropped from the restore.
    let TenantDataSnapshot {
        documents,
        indexes,
        edges,
        vectors,
        kv_tables,
        crdt_state,
        crdt_constraints,
        timeseries,
        flushed_ts_segments,
        columnar_engines,
        vector_params,
        index_configs,
        surrogate_pk,
        tenant_edges,
        group_write_marks,
        group_proposal_keys,
        group_proposal_keys_complete_from,
        documents_versioned,
        indexes_versioned,
        vector_multi_documents,
        arrays,
        metadata_floor,
        // A backup carries no cut: it serves Raft installs only.
        group_cut_index: _,
        // A backup carries no lane state: the destination fires and
        // delivers its own writes.
        group_event_lane: _,
        // A backup carries no Calvin cut: the restore re-applies its rows
        // as new writes on the destination.
        group_calvin: _,
        // A backup carries no redo stream: its rows restore as new writes.
        group_redo_streams: _,
        // A backup carries visible edge versions only. The restore re-applies
        // them at its own ordinal, so the source's cuts, hidden versions and
        // applied ordinals never reach the destination.
        edge_hidden: _,
        edge_cuts: _,
        edge_applied: _,
        tenant_edge_cuts: _,
        tenant_edge_applied: _,
    } = snap;
    into.documents.extend(documents);
    // Several nodes can list one index's members; the re-issue reads the
    // union.
    into.vector_multi_documents.extend(vector_multi_documents);
    into.indexes.extend(indexes);
    into.documents_versioned.extend(documents_versioned);
    into.indexes_versioned.extend(indexes_versioned);
    into.edges.extend(edges);
    into.vectors.extend(vectors);
    into.vector_params.extend(vector_params);
    into.index_configs.extend(index_configs);
    into.kv_tables.extend(kv_tables);
    // CRDT state is per-collection and tenant-explicit. Loro import is a
    // monotonic merge so concatenating section contributions is safe.
    into.crdt_state.extend(crdt_state);
    into.crdt_constraints.extend(crdt_constraints);
    into.timeseries.extend(timeseries);
    into.flushed_ts_segments.extend(flushed_ts_segments);
    into.columnar_engines.extend(columnar_engines);
    into.surrogate_pk.extend(surrogate_pk);
    into.arrays.extend(arrays);
    into.tenant_edges.extend(tenant_edges);
    // A backup carries no marks: the guard reads the destination's marks.
    into.group_write_marks.extend(group_write_marks);
    // A backup carries no proposal keys: they serve Raft installs only.
    into.group_proposal_keys.extend(group_proposal_keys);
    into.group_proposal_keys_complete_from = into
        .group_proposal_keys_complete_from
        .max(group_proposal_keys_complete_from);
    // A backup carries no floor: it serves Raft installs only.
    into.metadata_floor = into.metadata_floor.max(metadata_floor);
}

pub(super) fn is_metadata_section(section: &Section) -> bool {
    matches!(
        section.origin_node_id,
        SECTION_ORIGIN_CATALOG_ROWS
            | SECTION_ORIGIN_ARRAY_CATALOG
            | SECTION_ORIGIN_SOURCE_TOMBSTONES
            | SECTION_ORIGIN_SURROGATE_PK
            | SECTION_ORIGIN_DATABASES
            | SECTION_ORIGIN_VERIFICATION
    )
}

/// Apply catalog-row and source-tombstone sections to the destination catalog.
/// Runs BEFORE the data-section restore, after `databases` maps every source
/// database to its destination.
///
/// Each catalog row moves to its destination database. Catalog rows are
/// proposed cluster-wide through the metadata Raft group (group 0) — exactly
/// like CREATE COLLECTION — so every node's catalog learns the restored
/// collection and can serve it. A catalog-propose failure on this path is
/// FATAL: returning the data restored but unqueryable on non-coordinator nodes
/// is the silent-partial-success anti-pattern this codebase forbids.
///
/// Returns every collection written to the catalog, in section order. The
/// caller registers each one with this node's Data Plane before any restored
/// row is installed or reissued: a catalog row alone leaves `doc_configs`
/// without the collection's declaration, and a reissued timeseries row
/// is then ingested into an inferred shape.
pub(super) async fn apply_metadata_sections(
    state: &Arc<SharedState>,
    tenant_id: u64,
    env: &Envelope,
    databases: &DatabaseMap,
) -> Result<Vec<StoredCollection>, Error> {
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
                    let mut coll =
                        zerompk::from_msgpack::<StoredCollection>(&blob.bytes).map_err(|_| {
                            Error::Internal {
                                detail: format!(
                                    "invalid backup format: catalog row of '{}' is not decodable",
                                    blob.name
                                ),
                            }
                        })?;
                    coll.database_id = databases.target(blob.database_id)?.dest;
                    // The source cluster's incarnation names nothing here: an
                    // existing collection keeps its own, a new one gets one.
                    coll.incarnation = nodedb_types::Hlc::ZERO;
                    // Propose the collection so every node's applier
                    // (`catalog_entry::apply::collection::put`) writes the
                    // row — mirroring CREATE COLLECTION and DROP COLLECTION.
                    // The proposer returns once this node applied it.
                    let entry = CatalogEntry::PutCollection(Box::new(coll.clone()));
                    propose_catalog_entry_async(state, &entry).await?;
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
                    let database_id = databases.target(t.database_id)?.dest.as_u64();
                    // Replicate via the metadata Raft group so every node's boot WAL
                    // replay barrier matches — a coordinator-local tombstone lets purged
                    // writes resurrect on follower restart.
                    let entry = CatalogEntry::RecordWalTombstone {
                        database_id,
                        tenant_id,
                        collection: t.collection.clone(),
                        purge_lsn: t.purge_lsn,
                    };
                    propose_catalog_entry_async(state, &entry).await?;
                }
            }
            _ => {}
        }
    }
    Ok(restored)
}

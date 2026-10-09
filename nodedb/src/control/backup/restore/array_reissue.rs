// SPDX-License-Identifier: BUSL-1.1

//! Restore of arrays: each array's catalog row, then every cell version.
//!
//! The catalog row goes through `PutArray` like `CREATE ARRAY`, so every node
//! opens the array. A destination array of the same name, schema and routing
//! keeps its incarnation and takes the backup's settings: retention,
//! `audit_retain_ms`, and every other field of the row. One of another schema
//! or routing refuses the envelope before any write.
//!
//! Each cell version re-issues as a durable array write, in system-time
//! order, with its own system time and valid time: a restored array keeps
//! its history. A live cell takes the destination's surrogate for its
//! coordinate. Each write goes to the vShard its cells route to.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::backup_envelope::{
    ArrayCatalogBlob, DatabaseBlob, Envelope, SECTION_ORIGIN_ARRAY_CATALOG,
};
use nodedb_types::{CollectionKey, Hlc};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::ddl::propose_array_entries;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::engine::array::export::ArrayCellVersion;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::event::EventSource;
use crate::types::{ArrayCellsBlob, DatabaseId, TenantDataSnapshot, TenantId, VShardId};

use super::databases::DatabaseMap;
use super::target::DatabaseTarget;

/// Cells one re-issued write carries at most.
const CELLS_PER_WRITE: usize = 1024;

/// Whether the destination's `existing` array takes the backup's `row` in
/// place: the same schema, and cells that route the same way.
fn compatible(existing: &ArrayCatalogEntry, row: &ArrayCatalogEntry) -> bool {
    existing.schema_hash == row.schema_hash && existing.prefix_bits == row.prefix_bits
}

fn another_schema(array: &str, database: &str) -> Error {
    Error::BadRequest {
        detail: format!(
            "array '{array}' exists in database '{database}' with another schema or routing; \
             drop it, then retry the restore"
        ),
    }
}

fn malformed(detail: String) -> Error {
    Error::Internal {
        detail: format!("invalid backup format: {detail}"),
    }
}

fn codec(array: &str, e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore: array '{array}': {e}"),
    }
}

/// Every array catalog row of `env`, with its source database id.
fn array_rows(env: &Envelope) -> Result<Vec<(u64, ArrayCatalogEntry)>, Error> {
    let mut rows = Vec::new();
    for section in env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_ARRAY_CATALOG)
    {
        let blobs: Vec<ArrayCatalogBlob> = zerompk::from_msgpack(&section.body)
            .map_err(|_| malformed("array catalog section is not decodable".into()))?;
        for blob in blobs {
            let entry: ArrayCatalogEntry = zerompk::from_msgpack(&blob.bytes).map_err(|_| {
                malformed(format!(
                    "catalog row of array '{}' is not decodable",
                    blob.name
                ))
            })?;
            rows.push((blob.database_id, entry));
        }
    }
    Ok(rows)
}

/// Every array row names a listed database, and every array a data section
/// carries cells of has a row. A destination array of the same name must
/// have the same schema.
pub(super) fn validate_array_rows(
    catalog: &SystemCatalog,
    tenant: TenantId,
    env: &Envelope,
    databases: &[DatabaseBlob],
    merged: &BTreeMap<u64, TenantDataSnapshot>,
) -> Result<(), Error> {
    let names: BTreeMap<u64, &str> = databases
        .iter()
        .map(|blob| (blob.database_id, blob.name.as_str()))
        .collect();
    let mut rowed: BTreeSet<(u64, String)> = BTreeSet::new();
    for (source, entry) in array_rows(env)? {
        let Some(database) = names.get(&source) else {
            return Err(malformed(format!(
                "the row of array '{}' names database {source}, which the backup's database \
                 section does not list",
                entry.name
            )));
        };
        if let Some(dest) = catalog.get_database_id_by_name(database)?
            && let Some(existing) = catalog.get_array_in_database(tenant, dest, &entry.name)?
            && !compatible(&existing, &entry)
        {
            return Err(another_schema(&entry.name, database));
        }
        rowed.insert((source, entry.name));
    }
    for (source, snap) in merged {
        if let Some(blob) = snap
            .arrays
            .iter()
            .find(|blob| !rowed.contains(&(*source, blob.array.clone())))
        {
            return Err(malformed(format!(
                "cells of array '{}' in database {source} have no catalog row",
                blob.array
            )));
        }
    }
    Ok(())
}

/// Create each backed-up array in its destination database, or apply the
/// backup's row to the destination's compatible array of the same name.
/// Returns the rows restored.
pub(super) async fn restore_array_rows(
    state: &SharedState,
    tenant_id: u64,
    env: &Envelope,
    databases: &DatabaseMap,
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let rows = array_rows(env)?;
    for (source, row) in &rows {
        let dest = databases.target(*source)?.dest;
        let entry = ArrayCatalogEntry {
            array_id: ArrayId::in_database(tenant, dest, &row.name),
            // A new array takes a fresh incarnation from the proposer's stamp.
            modification_hlc: Hlc::ZERO,
            incarnation: Hlc::ZERO,
            ..row.clone()
        };
        propose_array_entries(state, |catalog| {
            let entry = match catalog.get_array_in_database(tenant, dest, &entry.name)? {
                // The array keeps its incarnation, so its in-flight cell
                // writes still route to it.
                Some(existing) if compatible(&existing, &entry) => ArrayCatalogEntry {
                    incarnation: existing.incarnation,
                    ..entry.clone()
                },
                Some(_) => {
                    return Err(another_schema(&entry.name, &dest.as_u64().to_string()));
                }
                None => entry.clone(),
            };
            Ok(vec![CatalogEntry::PutArray(Box::new(entry))])
        })
        .await?;
    }
    Ok(rows.len())
}

/// Re-issue every cell version of `blobs` into `target.dest`. Returns the
/// versions re-issued.
pub(in crate::control::backup::restore) async fn reissue_array_cells(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    blobs: Vec<ArrayCellsBlob>,
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for blob in blobs {
        let mut versions: Vec<ArrayCellVersion> =
            zerompk::from_msgpack(&blob.cells).map_err(|e| codec(&blob.array, e))?;
        // Every version of one coordinate is in this blob: its cells route to
        // one vShard. System-time order rebuilds each coordinate's history.
        versions.sort_by_key(|version| version.system_from_ms);
        super::durable::log_reissue_step(
            state,
            "array",
            &blob.array,
            VShardId::new(blob.vshard),
            versions.len(),
        );
        let write = CellWrite {
            tenant,
            target,
            array: &blob.array,
            vshard: blob.vshard,
        };
        let mut run: Vec<ArrayCellVersion> = Vec::new();
        for version in versions {
            let kind_changes = run
                .last()
                .is_some_and(|last| last.payload.is_some() != version.payload.is_some());
            if kind_changes || run.len() == CELLS_PER_WRITE {
                reissued += run.len();
                write.run(state, std::mem::take(&mut run)).await?;
            }
            run.push(version);
        }
        reissued += run.len();
        write.run(state, run).await?;
    }
    Ok(reissued)
}

/// Where one blob's cells re-issue.
struct CellWrite<'a> {
    tenant: TenantId,
    target: DatabaseTarget,
    array: &'a str,
    vshard: u32,
}

impl CellWrite<'_> {
    /// Re-issue `run`: all puts or all deletes.
    async fn run(&self, state: &SharedState, run: Vec<ArrayCellVersion>) -> Result<(), Error> {
        let Some(first) = run.first() else {
            return Ok(());
        };
        let puts = first.payload.is_some();
        let array_id = ArrayId::in_database(self.tenant, self.target.dest, self.array);
        let key = CollectionKey::from_bare(self.target.dest, self.array);
        let op = if puts {
            let mut staged = Vec::with_capacity(run.len());
            for version in run {
                let Some(payload) = version.payload else {
                    continue;
                };
                let pk =
                    zerompk::to_msgpack_vec(&version.coord).map_err(|e| codec(self.array, e))?;
                staged.push((pk, version.coord, version.system_from_ms, payload));
            }
            // Every cell's surrogate in one batch at the array's home.
            let pks: Vec<&[u8]> = staged.iter().map(|(pk, ..)| pk.as_slice()).collect();
            let surrogates = crate::control::server::surrogate_exchange::assign_surrogates_routed(
                state,
                key,
                self.tenant,
                &pks,
                crate::types::TraceId::ZERO,
            )
            .await?;
            let cells: Vec<ArrayPutCell> = staged
                .into_iter()
                .zip(surrogates)
                .map(
                    |((_, coord, system_from_ms, payload), surrogate)| ArrayPutCell {
                        surrogate,
                        coord,
                        attrs: payload.attrs,
                        system_from_ms,
                        valid_from_ms: payload.valid_from_ms,
                        valid_until_ms: payload.valid_until_ms,
                    },
                )
                .collect();
            ArrayOp::Put {
                array_id,
                cells_msgpack: zerompk::to_msgpack_vec(&cells).map_err(|e| codec(self.array, e))?,
                wal_lsn: 0,
                provenance: None,
                vshard_id: self.vshard,
            }
        } else {
            let cells: Vec<ArrayDeleteCell> = run
                .into_iter()
                .map(|version| ArrayDeleteCell {
                    coord: version.coord,
                    system_from_ms: version.system_from_ms,
                    erasure: version.erased,
                })
                .collect();
            ArrayOp::Delete {
                array_id,
                coords_msgpack: zerompk::to_msgpack_vec(&cells)
                    .map_err(|e| codec(self.array, e))?,
                wal_lsn: 0,
                provenance: None,
                vshard_id: self.vshard,
            }
        };
        self.write(state, PhysicalPlan::Array(op)).await
    }

    /// Write `plan` durably: propose it to the group of the vShard its cells
    /// route to.
    async fn write(&self, state: &SharedState, plan: PhysicalPlan) -> Result<(), Error> {
        let dest: DatabaseId = self.target.dest;
        let proposer = state.async_raft_proposer()?;
        let entry = crate::control::wal_replication::to_replicated_entry(
            self.tenant,
            dest,
            VShardId::new(self.vshard),
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
        )?
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "restore reissue: the cells restored into array '{}' did not map to a \
                 replicated write",
                self.array
            ),
        })?
        .with_event_source(EventSource::Restore)
        .with_restore_id(self.target.restore_id);
        crate::control::wal_replication::propose_replicated_entry(
            state,
            proposer,
            entry,
            crate::control::wal_replication::statement_propose_deadline(state),
        )
        .await?;
        Ok(())
    }
}

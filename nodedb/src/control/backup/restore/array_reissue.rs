// SPDX-License-Identifier: BUSL-1.1

//! Restore of arrays: each array's catalog row, then every cell version.
//!
//! The catalog row goes through `PutArray` like `CREATE ARRAY`, so every node
//! opens the array. A destination array of the same name, schema and routing
//! keeps its incarnation and takes the backup's settings: retention,
//! `audit_retain_ms`, and every other field of the row. One of another schema
//! or routing refuses the envelope before any write.
//!
//! The cell versions re-issue as Calvin transactions
//! (`redo_reissue::reissue_array_cells`).

use std::collections::{BTreeMap, BTreeSet};

use nodedb_array::types::ArrayId;
use nodedb_types::Hlc;
use nodedb_types::backup_envelope::{
    ArrayCatalogBlob, DatabaseBlob, Envelope, SECTION_ORIGIN_ARRAY_CATALOG,
};

use crate::Error;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::ddl::propose_array_entries;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::types::{TenantDataSnapshot, TenantId};

use super::databases::DatabaseMap;

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
        // A restored row mints its array name in the destination.
        crate::control::relation_name::check_relation_name("array", &entry.name)?;
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

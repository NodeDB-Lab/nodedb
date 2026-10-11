// SPDX-License-Identifier: BUSL-1.1

//! Raft snapshot install of the array engine.
//!
//! The group snapshot carries every cell version of its vShards. On this
//! core each array store drops its versions of those vShards and takes the
//! snapshot's, so the install replaces the group's array state the way it
//! replaces collection rows. Versions of every other vShard stay.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_tile_prefix;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::array_vshard_for_tile;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::array::export::ArrayCellVersion;
use crate::types::ArrayCellsBlob;

fn install_error(detail: String) -> crate::Error {
    crate::Error::Internal {
        detail: format!("array snapshot install: {detail}"),
    }
}

/// `(database, tenant, array)`.
type ArrayKey = (u64, u64, String);

impl CoreLoop {
    /// Replace the array cells of `vshards` on this core with `blobs`.
    /// Returns the versions installed.
    pub(in crate::data::executor) fn install_group_arrays(
        &mut self,
        vshards: &[u32],
        blobs: &[ArrayCellsBlob],
    ) -> crate::Result<u64> {
        if vshards.is_empty() {
            return match blobs.first() {
                None => Ok(0),
                Some(blob) => Err(install_error(format!(
                    "cells of array '{}' arrived with no vShard to replace",
                    blob.array
                ))),
            };
        }
        let group: HashSet<u32> = vshards.iter().copied().collect();
        let mut incoming: HashMap<ArrayKey, Vec<ArrayCellVersion>> = HashMap::new();
        for blob in blobs {
            let versions: Vec<ArrayCellVersion> = zerompk::from_msgpack(&blob.cells)
                .map_err(|e| install_error(format!("array '{}': {e}", blob.array)))?;
            incoming
                .entry((blob.database_id, blob.tenant_id, blob.array.clone()))
                .or_default()
                .extend(versions);
        }

        let entries = self
            .array_catalog
            .read()
            .map_err(|_| install_error("array catalog lock poisoned".into()))?
            .all_entries();
        // A snapshot array this node's catalog lacks cannot open. The metadata
        // group applies it first, and the install retries.
        for (database_id, tenant_id, array) in incoming.keys() {
            let known = entries.iter().any(|entry| {
                entry.array_id.database_id.as_u64() == *database_id
                    && entry.array_id.tenant_id.as_u64() == *tenant_id
                    && entry.name == *array
            });
            if !known {
                return Err(install_error(format!(
                    "array '{array}' of tenant {tenant_id} in database {database_id} is not in \
                     this node's catalog yet"
                )));
            }
        }

        let stamp = self.floors.applied_prefix.stamp()?;
        let mut installed = 0u64;
        for entry in entries {
            let key = (
                entry.array_id.database_id.as_u64(),
                entry.array_id.tenant_id.as_u64(),
                entry.name.clone(),
            );
            let versions = incoming.remove(&key).unwrap_or_default();
            installed += versions.len() as u64;
            let schema: ArraySchema = zerompk::from_msgpack(&entry.schema_msgpack)
                .map_err(|e| install_error(format!("array '{}' schema: {e}", entry.name)))?;
            let schema = Arc::new(schema);
            let routes = Arc::clone(&schema);
            let prefix_bits = entry.prefix_bits;
            let replaced = |coord: &[CoordValue]| {
                cell_tile_prefix(&routes, coord)
                    .ok()
                    .and_then(|prefix| array_vshard_for_tile(prefix, prefix_bits).ok())
                    .is_some_and(|vshard| group.contains(&vshard))
            };
            self.array_engine
                .replace_cells(
                    &entry.array_id,
                    schema,
                    entry.schema_hash,
                    replaced,
                    versions,
                    stamp.clone(),
                )
                .map_err(|e| install_error(format!("array '{}': {e}", entry.name)))?;
        }
        Ok(installed)
    }
}

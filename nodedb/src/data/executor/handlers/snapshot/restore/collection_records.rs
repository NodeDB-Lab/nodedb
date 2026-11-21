// SPDX-License-Identifier: BUSL-1.1

//! The per-collection state a clear-then-install replaces before any row
//! installs: each cleared collection's rows, its conversion record and its
//! maintained index set, then the records the snapshot carries.

use nodedb_physical::physical_plan::SnapshotClearTarget;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::clear_versioned_sparse::{CollectionClear, MaintainedSetFate};
use crate::types::TenantDataSnapshot;

impl CoreLoop {
    /// Drop the stale state of every listed collection, keeping each
    /// collection's definition.
    ///
    /// Clear-then-install replaces row data from the snapshot, but the
    /// snapshot does not carry the schema, so the reinstalled rows land in
    /// the still-defined collection. Keys deleted before the snapshot and
    /// dropped collections do not linger on a lagging follower. A clear
    /// error aborts the restore. Rows that survive a clear stay as un-owned
    /// data on this follower.
    pub(super) fn clear_install_targets(
        &mut self,
        targets: &[SnapshotClearTarget],
        group_install: bool,
    ) -> Result<(), ErrorCode> {
        for target in targets {
            self.clear_collection_all_engines(
                nodedb_types::DatabaseId::new(target.database_id),
                crate::types::TenantId::new(target.tenant_id),
                &target.collection,
                // The cleared index entries' maintained set goes with them,
                // in the rows' own transaction. The snapshot carries the set
                // of the entries that replace them.
                CollectionClear::KeepDefinition(MaintainedSetFate::Forget),
                target.reclaim_l1_files,
                !group_install,
            )
            .map_err(|e| ErrorCode::Internal {
                detail: format!(
                    "clear-then-install purge failed for '{}': {e}",
                    target.collection
                ),
            })?;
            // The cleared rows' conversion record goes with them. The
            // snapshot carries the record of the rows that replace them.
            self.sparse
                .delete_storage_conversion(target.database_id, target.tenant_id, &target.collection)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!(
                        "clear-then-install conversion record of '{}' failed: {e}",
                        target.collection
                    ),
                })?;
        }
        Ok(())
    }

    /// Install the conversion records and the maintained index sets `snap`
    /// carries, before any row: the rows install in the encoding each record
    /// names, and each collection maintains the index paths whose entries
    /// the snapshot holds.
    pub(super) fn install_collection_records(
        &mut self,
        snap: &TenantDataSnapshot,
    ) -> Result<(), ErrorCode> {
        for (key, bytes) in &snap.storage_conversions {
            self.sparse
                .put_storage_conversion_raw(key, bytes)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("restore: conversion record of {key} failed: {e}"),
                })?;
        }
        for (key, bytes) in &snap.maintained_indexes {
            self.install_maintained_index_set(key, bytes)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("restore: maintained index set of {key} failed: {e}"),
                })?;
        }
        Ok(())
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Resolve the restored rows of one timeseries collection.
//!
//! A timeseries row has no key: the collection is the unit a RESTORE orders
//! against (`TimeseriesRestoreRedo`). The resolve reads the destination's
//! partition ends at the transaction's turn. A partition ended above the
//! RESTORE's cut names a TRUNCATE after the RESTORE began, which ends every
//! row the RESTORE wrote: nothing installs. Otherwise the rows resolve to the
//! `ts-resolved` ingest sub-record a live ingest writes. Each row keeps its
//! backed-up `_ts_system` (`carry_source_ordinals`), and a row the
//! destination already holds is dropped (`restore_timeseries_held`), so a
//! RESTORE is idempotent. An ingest after the cut appends its own rows
//! beside them, and a TRUNCATE after the batch ends them.
//!
//! The rows are the backup's current rows only. The RESTORE never ingests a
//! row of a partition a TRUNCATE ended in the backup.

use std::collections::HashSet;

use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_types::value::Value;
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::wal_dispatch::{
    TimeseriesIngestRecord, encode_timeseries_ingest_payload,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::ended_install::ENDED_INSTALL_PREFIX;
use crate::data::executor::handlers::timeseries::{
    StampedIngest, TsResolveInput, carry_source_ordinals,
};
use crate::engine::timeseries::resolved_ingest::{
    CLONE_COPY_INGEST_FORMAT, RESOLVED_INGEST_FORMAT, TsDriftPolicy,
};
use crate::types::TenantId;
use crate::wal::{RedoRowKind, RedoSubRecord, TimeseriesEndedPartitionRedo, TimeseriesRestoreRedo};

use super::restore_changes::RestoreChanges;
use super::restore_reassert::RestoreScope;
use super::restore_timeseries_held::{drop_held, restored_identity};

/// Whether a TRUNCATE whose newest partition end is `newest_end` followed a
/// RESTORE whose cut is `cut`.
pub(super) fn truncated_after_cut(newest_end: Option<i64>, cut: i64) -> bool {
    newest_end.is_some_and(|end| end > cut)
}

/// Whether `held` is the partition `restored` names: the same time range,
/// end, newest ordinal and row count.
pub(super) fn same_ended_partition(
    held: &nodedb_types::timeseries::PartitionMeta,
    restored: &nodedb_types::timeseries::PartitionMeta,
) -> bool {
    restored.ended_at.is_some()
        && held.ended_at == restored.ended_at
        && held.min_ts == restored.min_ts
        && held.max_ts == restored.max_ts
        && held.max_system_ts == restored.max_system_ts
        && held.row_count == restored.row_count
}

impl CoreLoop {
    /// Whether the destination holds the ended partition `ended` names: a
    /// RESTORE into the collection it was backed up from, or a second
    /// RESTORE, installs no copy of it.
    pub(super) fn holds_ended_partition(
        &self,
        scope: RestoreScope,
        ended: &TimeseriesEndedPartitionRedo,
    ) -> bool {
        let key = (
            scope.database_id,
            TenantId::new(scope.tid),
            ended.collection.clone(),
        );
        self.ts_registries.get(&key).is_some_and(|registry| {
            registry
                .iter()
                .any(|(_, entry)| same_ended_partition(&entry.meta, &ended.meta))
        })
    }

    /// The newest ordinal a TRUNCATE of this destination ended a partition
    /// of `collection` at. A partition a RESTORE installed carries the end
    /// the backup's source stamped, which is no TRUNCATE here.
    fn newest_ts_partition_end(&self, scope: RestoreScope, collection: &str) -> Option<i64> {
        let key = (
            scope.database_id,
            TenantId::new(scope.tid),
            collection.to_string(),
        );
        self.ts_registries.get(&key).and_then(|registry| {
            registry
                .iter()
                .filter(|(partition, _)| !partition.dir_name.starts_with(ENDED_INSTALL_PREFIX))
                .filter_map(|(_, entry)| entry.meta.ended_at)
                .max()
        })
    }

    /// The ingest sub-record of the restored rows `redo`, or `None` when a
    /// TRUNCATE after the RESTORE's cut ends them.
    ///
    /// `Err` when the rows do not resolve against the collection.
    pub(super) fn resolve_restored_timeseries(
        &self,
        scope: RestoreScope,
        redo: TimeseriesRestoreRedo,
        changes: &mut RestoreChanges,
    ) -> crate::Result<Option<RedoSubRecord>> {
        if truncated_after_cut(
            self.newest_ts_partition_end(scope, &redo.collection),
            scope.cut,
        ) {
            return Ok(None);
        }
        let tid = TenantId::new(scope.tid);
        let collection = redo.collection.as_str();
        let rows = match nodedb_types::value_from_msgpack(&redo.rows) {
            Ok(Value::Array(rows)) => rows,
            Ok(_) => {
                return Err(crate::Error::Internal {
                    detail: format!("restore resolve: the rows of '{collection}' are no array"),
                });
            }
            Err(e) => {
                return Err(crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("restore resolve: the rows of '{collection}': {e}"),
                });
            }
        };
        // Every restored row the destination already holds is dropped.
        let mut ordinals = HashSet::new();
        for row in &rows {
            if let (_, Some(system)) = restored_identity(collection, row)? {
                ordinals.insert(system);
            }
        }
        let held = self.held_ts_rows(scope, collection, &ordinals)?;
        let rows = drop_held(collection, rows, held)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let payload = nodedb_types::value_to_msgpack(&Value::Array(rows)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("restore resolve: encode the rows of '{collection}': {e}"),
            }
        })?;
        // Every backed-up row carries its time key, so the default instant
        // never stamps one. It is the transaction's millisecond.
        let now_ms = nodedb_types::ordinal_to_ms(scope.applied);
        // Each row carries its backed-up `_ts_system`, as a clone copy's row
        // carries its source ordinal.
        let lines = self
            .stamped_ingest_lines(StampedIngest {
                database_id: scope.database_id,
                tid,
                collection,
                payload: &payload,
                format: CLONE_COPY_INGEST_FORMAT,
                now_ms,
            })
            .map_err(crate::Error::DataPlane)?;
        let Some(vshard) =
            nodedb_types::CollectionKey::from_qualified_str(scope.database_id, collection)
                .ok()
                .map(|key| key.vshard())
        else {
            return Err(crate::Error::Internal {
                detail: format!("restore resolve: '{collection}' names no collection key"),
            });
        };
        let task = Self::replay_task(
            tid,
            scope.database_id,
            vshard,
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                payload: Vec::new(),
                format: RESOLVED_INGEST_FORMAT.into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            None,
        );
        let batch = self.resolve_ts_batch(
            &task,
            TsResolveInput {
                tid,
                collection,
                lines: &lines,
                now_ms,
                drift: TsDriftPolicy::ApplyByName,
                needs_images: false,
                base: None,
            },
        )?;
        let joined = lines.join("\n");
        let parsed = crate::engine::timeseries::ilp::parse_batch(&joined).map_err(|e| {
            crate::Error::Internal {
                detail: format!("restore resolve: the lines of '{collection}' do not parse: {e}"),
            }
        })?;
        // The install keeps every row's carried ordinal. A restore is no
        // clone copy: its rows publish the events a live ingest publishes.
        let emits_events = batch.emits_events;
        let mut batch = carry_source_ordinals(batch, parsed.lines())?;
        batch.emits_events = emits_events;
        let payload = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection,
            payload: &batch.to_bytes()?,
            provenance: None,
            format: RESOLVED_INGEST_FORMAT,
            default_timestamp_ms: batch.now_ms,
        })?;
        if !batch.rows.is_empty() {
            changes.note_grouped(collection, RedoRowKind::Insert);
        }
        Ok(Some(RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A TRUNCATE at or below the cut preceded the RESTORE: its rows
    /// install. One above it followed the RESTORE and ends them: nothing
    /// installs.
    #[test]
    fn a_truncate_after_the_cut_ends_the_restored_rows() {
        assert!(!truncated_after_cut(None, 3_000));
        assert!(!truncated_after_cut(Some(2_000), 3_000));
        assert!(!truncated_after_cut(Some(3_000), 3_000));
        assert!(truncated_after_cut(Some(3_001), 3_000));
    }
}

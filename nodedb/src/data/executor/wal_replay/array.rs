// SPDX-License-Identifier: BUSL-1.1

//! Array engine WAL replay: rebuilds tile state after crash.
//!
//! ## The durable watermark
//!
//! Each array's manifest carries a `durable_lsn` — the highest LSN whose cells
//! are already inside a flushed, on-disk segment (`Manifest::add_segment`
//! raises it). Replay skips every record at or below it, which is what makes
//! replaying the same retained tail twice a no-op and, more importantly, what
//! stops a cell version that a bitemporal audit purge physically removed from a
//! segment being re-materialised out of a still-retained `ArrayPut`. Without
//! the gate the purge is silently undone on the next boot.
//!
//! Records ABOVE the watermark are the tail the segments have not absorbed and
//! must be re-applied into the memtable.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use std::sync::Arc;

/// Outcome of preparing an array's store for replay.
enum ArrayOpen {
    /// The store is open; replay may proceed against it.
    Ready,
    /// The array has no catalog entry on this node. The catalog is a separate
    /// durable store from the WAL, so a WAL tail can legitimately outlive the
    /// catalog row it names — a DROP that committed in the catalog and had its
    /// WAL tombstone truncated, or a node that never received the DDL. There is
    /// no array to apply the cells to and no state to corrupt by not applying
    /// them, so this is a skip rather than an abort.
    NoCatalogEntry,
}

impl CoreLoop {
    fn ensure_array_open_for_replay(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
    ) -> crate::Result<ArrayOpen> {
        let entry = {
            let cat = self
                .array_catalog
                .read()
                .map_err(|_| crate::Error::Internal {
                    detail: "array catalog lock poisoned during WAL replay".into(),
                })?;
            cat.lookup_by_id(array_id)
                .map(|entry| (entry.schema_msgpack.clone(), entry.schema_hash))
        };
        let Some((schema_msgpack, schema_hash)) = entry else {
            return Ok(ArrayOpen::NoCatalogEntry);
        };
        let schema = zerompk::from_msgpack::<nodedb_array::schema::ArraySchema>(&schema_msgpack)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array schema decode during WAL replay: {e}"),
            })?;
        self.array_engine
            .open_array(array_id.clone(), Arc::new(schema), schema_hash)
            .map_err(|e| crate::Error::Internal {
                detail: format!("array open during WAL replay: {e}"),
            })?;
        Ok(ArrayOpen::Ready)
    }

    /// In the install pass of a committed-redo apply, record the memtable
    /// tiles a cell write touches and the sync high-water mark it advances.
    /// Returns `false` when the tiles cannot be read; the error is kept on
    /// the apply.
    fn record_array_tiles_undo<'a>(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
        cells: impl IntoIterator<Item = (&'a [nodedb_array::types::coord::value::CoordValue], i64)>,
        provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    ) -> bool {
        let captured = self
            .array_engine
            .snapshot_tiles(array_id, cells)
            .map_err(|e| crate::Error::Internal {
                detail: format!(
                    "reading the memtable tiles of array '{}': {e}",
                    array_id.name
                ),
            })
            .map(|snapshot| {
                std::iter::once(UndoEntry::ArrayTiles {
                    array_id: array_id.clone(),
                    snapshot,
                })
                .chain(self.capture_sync_hwm_undo(provenance))
            });
        self.record_redo_capture(captured)
    }

    /// The LSN this array's flushed segments are already durable through.
    ///
    /// `0` when the store is not open, which gates nothing — the safe
    /// direction, matching every other engine's unset replay floor.
    pub(in crate::data::executor) fn array_durable_lsn(
        &self,
        array_id: &nodedb_array::types::ArrayId,
    ) -> u64 {
        self.array_engine
            .store(array_id)
            .map_or(0, |store| store.manifest().durable_lsn)
    }

    pub fn replay_array_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use crate::engine::array::wal::{decode_delete_with_version, decode_put_with_version};
        use nodedb_wal::record::RecordType;

        let mut puts = 0usize;
        let mut deletes = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);
            let is_put = record_type == Some(RecordType::ArrayPut);
            let is_delete = record_type == Some(RecordType::ArrayDelete);
            if !is_put && !is_delete {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;

            if is_put {
                let payload = match decode_put_with_version(&record.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        self.replay_record_unapplied(
                            "array",
                            "decode_put",
                            record_lsn,
                            &format!("ArrayPut payload could not be decoded: {e}"),
                        );
                        skipped += 1;
                        continue;
                    }
                };
                if tombstones.is_tombstoned(
                    record.header.database_id,
                    tenant_id,
                    &payload.array_id.name,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }
                match self.ensure_array_open_for_replay(&payload.array_id) {
                    Ok(ArrayOpen::Ready) => {}
                    Ok(ArrayOpen::NoCatalogEntry) => {
                        self.replay_record_rejected(
                            "array",
                            record_lsn,
                            None,
                            &format!(
                                "array '{}' has no catalog entry; its retained cells are skipped",
                                payload.array_id.name
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    Err(e) => {
                        self.replay_record_unapplied(
                            "array",
                            "open",
                            record_lsn,
                            &format!("array '{}' could not be opened: {e}", payload.array_id.name),
                        );
                        skipped += 1;
                        continue;
                    }
                }
                if self
                    .replay_watermark_skips(record_lsn <= self.array_durable_lsn(&payload.array_id))
                {
                    skipped += 1;
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                let installing = self.recording_redo_undo();
                if installing
                    && !self.record_array_tiles_undo(
                        &payload.array_id,
                        payload
                            .cells
                            .iter()
                            .map(|c| (c.coord.as_slice(), c.system_from_ms)),
                        payload.provenance.as_ref(),
                    )
                {
                    skipped += 1;
                    continue;
                }
                let cell_count = payload.cells.len();
                let prov = payload.provenance.clone();
                // An install flushes once the whole record landed, so a
                // rollback finds its cells in the memtable.
                let stamped = if installing {
                    self.array_engine.put_cells_unflushed(
                        &payload.array_id,
                        payload.cells,
                        record_lsn,
                    )
                } else {
                    self.array_engine
                        .put_cells(&payload.array_id, payload.cells, record_lsn)
                };
                if let Err(e) = stamped {
                    self.replay_record_unapplied(
                        "array",
                        "put_cells",
                        record_lsn,
                        &format!("committed cells could not be re-applied: {e}"),
                    );
                    skipped += 1;
                    continue;
                }
                puts += cell_count;
                self.note_redo_array_written(&payload.array_id);
                // Rebuild the per-core HWM frontier from the WAL record's
                // provenance. No fence check here — replay records are already
                // durable and ordered; just advance the frontier.
                if let Some(p) = &prov {
                    self.sync_commit(p);
                }
                continue;
            }

            let payload = match decode_delete_with_version(&record.payload) {
                Ok(p) => p,
                Err(e) => {
                    self.replay_record_unapplied(
                        "array",
                        "decode_delete",
                        record_lsn,
                        &format!("ArrayDelete payload could not be decoded: {e}"),
                    );
                    skipped += 1;
                    continue;
                }
            };
            if tombstones.is_tombstoned(
                record.header.database_id,
                tenant_id,
                &payload.array_id.name,
                record_lsn,
            ) {
                skipped += 1;
                continue;
            }
            match self.ensure_array_open_for_replay(&payload.array_id) {
                Ok(ArrayOpen::Ready) => {}
                Ok(ArrayOpen::NoCatalogEntry) => {
                    self.replay_record_rejected(
                        "array",
                        record_lsn,
                        None,
                        &format!(
                            "array '{}' has no catalog entry; its retained tombstones are skipped",
                            payload.array_id.name
                        ),
                    );
                    skipped += 1;
                    continue;
                }
                Err(e) => {
                    self.replay_record_unapplied(
                        "array",
                        "open",
                        record_lsn,
                        &format!("array '{}' could not be opened: {e}", payload.array_id.name),
                    );
                    skipped += 1;
                    continue;
                }
            }
            if self.replay_watermark_skips(record_lsn <= self.array_durable_lsn(&payload.array_id))
            {
                skipped += 1;
                continue;
            }
            if self.claim_for_validation() {
                continue;
            }
            let installing = self.recording_redo_undo();
            if installing
                && !self.record_array_tiles_undo(
                    &payload.array_id,
                    payload
                        .cells
                        .iter()
                        .map(|c| (c.coord.as_slice(), c.system_from_ms)),
                    payload.provenance.as_ref(),
                )
            {
                skipped += 1;
                continue;
            }
            let cell_count = payload.cells.len();
            let prov = payload.provenance.clone();
            let stamped = if installing {
                self.array_engine.delete_cells_unflushed(
                    &payload.array_id,
                    payload.cells,
                    record_lsn,
                )
            } else {
                self.array_engine
                    .delete_cells(&payload.array_id, payload.cells, record_lsn)
            };
            if let Err(e) = stamped {
                self.replay_record_unapplied(
                    "array",
                    "delete_cells",
                    record_lsn,
                    &format!("committed tombstones could not be re-applied: {e}"),
                );
                skipped += 1;
                continue;
            }
            deletes += cell_count;
            self.note_redo_array_written(&payload.array_id);
            if let Some(p) = &prov {
                self.sync_commit(p);
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                skipped,
                "WAL array replay complete"
            );
        }
    }
}

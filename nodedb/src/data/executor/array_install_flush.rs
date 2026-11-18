// SPDX-License-Identifier: BUSL-1.1

//! A batched install's array flushes.
//!
//! A batched install holds one batch of its record at a time, and its array
//! memtables stay under their cell threshold: after each array batch, the
//! driver flushes every array of the record whose memtable reached it. The
//! record is not noted applied yet, so the flush's manifest names every
//! record the core applied and, in part, the install's record: its ops
//! below the batch's end. Restart replay then applies the record's other
//! ops, each once (`array_partial_replay_skips`).
//!
//! The undo of such a flush (`ArrayInstallFlushUndo`) withdraws its segment
//! and leaves the memtable to the install's tile snapshots. A tile snapshot
//! holds only the tiles the install wrote, so the segment must hold no
//! older cell of another tile. So before the array arm's first batch, the
//! install flushes every array whose memtable holds cells and that the
//! record's cells, counted by the stream validate, can bring to the
//! threshold. That flush names only records applied before, and no undo
//! takes it back. An array the record cannot fill never flushes during the
//! install.

use std::collections::HashMap;

use nodedb_array::types::ArrayId;
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::array_install_flush::ArrayInstallFlushUndo;
use crate::engine::array::wal::{decode_delete_with_version, decode_put_with_version};
use crate::types::partial_record::{PartialRecord, PartialRecords};
use crate::wal::{ArrayRestoreRedo, RedoSubRecord};

/// The cells one streamed record writes into each array of this core.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct ArrayStreamLoads {
    cells: HashMap<ArrayId, usize>,
}

impl ArrayStreamLoads {
    /// The cells the record writes into each array.
    pub(in crate::data::executor) fn arrays(&self) -> impl Iterator<Item = (&ArrayId, usize)> {
        self.cells.iter().map(|(id, cells)| (id, *cells))
    }

    /// An estimate of the RAM the loads hold: each entry and its array name.
    pub(in crate::data::executor) fn held_bytes(&self) -> usize {
        self.cells
            .keys()
            .map(|id| std::mem::size_of::<(ArrayId, usize)>() + id.name.len())
            .sum()
    }
}

fn flush_error(id: &ArrayId, action: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "array".to_string(),
        detail: format!("{action} of array '{}' in a batched install: {e}", id.name),
    }
}

impl CoreLoop {
    /// Fold the op `sub` of a streamed record on `vshard_id` into `loads`.
    /// Only an array write this core owns adds cells. An op that does not
    /// decode adds none: its install fails, which reports it.
    pub(in crate::data::executor) fn add_array_stream_op(
        &self,
        loads: &mut ArrayStreamLoads,
        sub: &RedoSubRecord,
        vshard_id: u32,
    ) {
        let record_type = RecordType::from_raw(sub.record_type);
        let is_put = record_type == Some(RecordType::ArrayPut);
        if !is_put && record_type != Some(RecordType::ArrayDelete) {
            return;
        }
        let num_cores = self.redo_apply.num_cores;
        let target_core = if num_cores > 0 {
            vshard_id as usize % num_cores
        } else {
            0
        };
        if target_core != self.core_id {
            return;
        }
        let written = if is_put {
            match ArrayRestoreRedo::decode(&sub.payload) {
                Some(restored) => Some((
                    restored.array_id,
                    restored.puts.len() + restored.deletes.len(),
                )),
                None => decode_put_with_version(&sub.payload)
                    .ok()
                    .map(|put| (put.array_id, put.cells.len())),
            }
        } else {
            decode_delete_with_version(&sub.payload)
                .ok()
                .map(|delete| (delete.array_id, delete.cells.len()))
        };
        if let Some((id, cells)) = written {
            let held = loads.cells.entry(id).or_insert(0);
            *held = held.saturating_add(cells);
        }
    }

    /// Before the array arm's first batch: flush every array in `loads`
    /// whose memtable holds cells and that the record's cells can fill. An
    /// array whose store is not open holds no memtable cell.
    pub(in crate::data::executor) fn flush_arrays_for_stream(
        &mut self,
        loads: &ArrayStreamLoads,
    ) -> crate::Result<()> {
        for (id, cells) in loads.arrays() {
            // `may_fill` fails only for an array this core never opened.
            if self.array_engine.may_fill(id, cells).unwrap_or(false) {
                self.flush_array(id)?;
            }
        }
        Ok(())
    }

    /// After an array batch of a batched install: flush every array in
    /// `loads` whose memtable reached its threshold, naming the install's
    /// record through `partial`, and record each flush's undo. Returns the
    /// arrays it flushed.
    pub(in crate::data::executor) fn flush_array_install_batch(
        &mut self,
        loads: &ArrayStreamLoads,
        partial: PartialRecord,
    ) -> crate::Result<usize> {
        let mut count = 0;
        for (id, _) in loads.arrays() {
            // `needs_flush` fails only for an array this core never opened,
            // which holds no memtable cell.
            if !self.array_engine.needs_flush(id).unwrap_or(false) {
                continue;
            }
            let stamp = self.floors.applied_prefix.stamp()?;
            let named = PartialRecords::naming(partial).pruned(&stamp);
            let store = self
                .array_engine
                .store(id)
                .map_err(|e| flush_error(id, "read the manifest", e))?;
            let manifest = store.manifest();
            let replay_before = manifest.replay.clone();
            let partial_before = manifest.partial.clone();
            let flushed = self
                .array_engine
                .flush_with(id, stamp, named)
                .map_err(|e| flush_error(id, "flush", e))?;
            // A failed flush leaves no manifest naming its segment, so it
            // leaves nothing to take back.
            if let Some(segment) = flushed {
                self.record_redo_undo([UndoEntry::ArrayInstallFlush(Box::new(
                    ArrayInstallFlushUndo {
                        array_id: id.clone(),
                        segment: segment.id,
                        replay_before,
                        partial_before,
                    },
                ))]);
                count += 1;
            }
        }
        Ok(count)
    }
}

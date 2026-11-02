// SPDX-License-Identifier: BUSL-1.1

//! WAL replay → memtable rehydration.
//!
//! Recovery is driven by the engine on `open`: the caller streams
//! decoded WAL records (filtered to array record types) into
//! [`Recovery::apply_record`]. A record the manifest's replay stamp names is
//! skipped — a segment already holds it. Every other record is re-applied
//! to the live memtable.
//!
//! The recovery layer is intentionally pure with respect to WAL I/O: it
//! takes already-decoded payloads. The engine open path reads the
//! segmented WAL and feeds records in here.

use crate::engine::array::store::ArrayStore;
use crate::engine::array::wal::{ArrayDeletePayload, ArrayPutCell, ArrayPutPayload};
use crate::engine::array::write::{stamp_delete_cells, stamp_put_cells};
use nodedb_array::ArrayError;

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Array(#[from] ArrayError),
    #[error(transparent)]
    Engine(#[from] crate::engine::array::engine::ArrayEngineError),
    #[error("recovery: unknown array {array}")]
    UnknownArray { array: String },
}

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub puts_applied: usize,
    pub puts_skipped: usize,
    pub deletes_applied: usize,
    pub deletes_skipped: usize,
}

/// One WAL record presented to the engine during recovery. The Origin
/// caller decodes the raw [`nodedb_wal::WalRecord`] payload via zerompk
/// and dispatches to the appropriate variant.
///
/// The caller stamps every cell with the system-time ordinal of its apply
/// before it hands the record over: a record carries no version stamp.
pub enum RecoveryRecord {
    Put {
        lsn: u64,
        payload: ArrayPutPayload,
    },
    Delete {
        lsn: u64,
        payload: ArrayDeletePayload,
    },
    /// A flush record. The segment it wrote is already mmap'd at startup,
    /// and the manifest's stamp already names the records that segment
    /// holds, so the record changes nothing here.
    Flush {
        lsn: u64,
        array: nodedb_array::types::ArrayId,
    },
}

pub struct Recovery<'a> {
    store: &'a mut ArrayStore,
    pub stats: RecoveryStats,
}

impl<'a> Recovery<'a> {
    pub fn new(store: &'a mut ArrayStore) -> Self {
        Self {
            store,
            stats: RecoveryStats::default(),
        }
    }

    pub fn apply_record(&mut self, rec: RecoveryRecord) -> Result<(), RecoveryError> {
        match rec {
            RecoveryRecord::Put { lsn, payload } => {
                if self.store.manifest().replay.skips(lsn) {
                    self.stats.puts_skipped += 1;
                    return Ok(());
                }
                // Every cell carries the tile-version stamp the caller
                // decided for this apply. Recovery writes it as given.
                let cells: Vec<ArrayPutCell> = payload.cells;
                stamp_put_cells(self.store, payload.vshard_id, cells, lsn)?;
                self.stats.puts_applied += 1;
            }
            RecoveryRecord::Delete { lsn, payload } => {
                if self.store.manifest().replay.skips(lsn) {
                    self.stats.deletes_skipped += 1;
                    return Ok(());
                }
                // Every tombstone carries the stamp the caller decided.
                stamp_delete_cells(self.store, payload.vshard_id, payload.cells, lsn)?;
                self.stats.deletes_applied += 1;
            }
            RecoveryRecord::Flush { .. } => {}
        }
        Ok(())
    }
}

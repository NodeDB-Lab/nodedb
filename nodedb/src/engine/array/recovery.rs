//! WAL replay → memtable rehydration.
//!
//! Recovery is driven by the engine on `open`: the caller streams
//! decoded WAL records (filtered to array record types) into
//! [`Recovery::apply_record`]. Records with LSN <= the manifest's
//! `durable_lsn` are skipped — the segment they belong to is already on
//! disk. Records with LSN greater than the durable watermark are
//! re-applied to the live memtable.
//!
//! The recovery layer is intentionally pure with respect to WAL I/O: it
//! takes already-decoded payloads. The Origin glue (Tier 5) reads the
//! segmented WAL and feeds records in here.

use crate::engine::array::store::ArrayStore;
use crate::engine::array::wal::{ArrayDeletePayload, ArrayPutPayload};
use nodedb_array::ArrayError;

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Array(#[from] ArrayError),
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
pub enum RecoveryRecord {
    Put {
        lsn: u64,
        payload: ArrayPutPayload,
    },
    Delete {
        lsn: u64,
        payload: ArrayDeletePayload,
    },
    /// Flush watermarks update the durable_lsn on the matching store.
    /// The segment itself is already mmap'd at startup time; this
    /// record simply tells us "WAL records up to this LSN are durable".
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
        let durable = self.store.manifest().durable_lsn;
        match rec {
            RecoveryRecord::Put { lsn, payload } => {
                if lsn <= durable {
                    self.stats.puts_skipped += 1;
                    return Ok(());
                }
                let schema = self.store.schema().clone();
                for cell in payload.cells {
                    self.store
                        .memtable
                        .put_cell(&schema, cell.coord, cell.attrs, lsn)?;
                }
                self.stats.puts_applied += 1;
            }
            RecoveryRecord::Delete { lsn, payload } => {
                if lsn <= durable {
                    self.stats.deletes_skipped += 1;
                    return Ok(());
                }
                let schema = self.store.schema().clone();
                for coord in payload.coords {
                    self.store.memtable.delete_cell(&schema, coord, lsn)?;
                }
                self.stats.deletes_applied += 1;
            }
            RecoveryRecord::Flush { lsn, .. } => {
                let m = self.store.manifest_mut();
                m.durable_lsn = m.durable_lsn.max(lsn);
            }
        }
        Ok(())
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The sync idempotency-gate checkpoint write path: export both gate maps and
//! publish them with one atomic write.

use tracing::info;

use super::format::{SYNC_HWM_CKPT_FORMAT_VERSION, SyncHwmCheckpointFile};
use super::paths::{SYNC_HWM_CKPT_STATE, sync_hwm_ckpt_dir, sync_hwm_ckpt_state_path};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush this core's sync gate to disk and return the LSN it is now durable
    /// through.
    ///
    /// Returns `Ok(watermark)` only once the state file has landed and been
    /// fsynced. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN the gate was known durable
    /// through, so a failed flush costs WAL growth instead of a gate that comes
    /// back empty and re-applies frames it had already acknowledged.
    ///
    /// The single write is the commit point: before it the previous state file
    /// is intact and live, after it the new one is. There is no window in which
    /// half a gate is published.
    ///
    /// Stamping with the core watermark rests on this: the checkpoint runs
    /// on the core's own thread between tasks, and `sync_commit` advances
    /// the HWM only after the frame's `SyncSeqAdvance` record is durable, so
    /// every advance the core has admitted is already in the maps exported here.
    pub(in crate::data::executor) fn checkpoint_sync_hwm(&self) -> crate::Result<Lsn> {
        let durable_through = self.watermark;

        // Sorted so identical gate state always encodes to identical bytes.
        let mut hwm: Vec<(u64, u64, u64)> = self
            .sync_hwm
            .iter()
            .map(|(&(producer_id, stream_id), &seq)| (producer_id, stream_id, seq))
            .collect();
        hwm.sort_unstable();
        let mut epoch_floor: Vec<(u64, u64)> = self
            .producer_epoch_floor
            .iter()
            .map(|(&producer_id, &epoch)| (producer_id, epoch))
            .collect();
        epoch_floor.sort_unstable();

        let file = SyncHwmCheckpointFile {
            format_version: SYNC_HWM_CKPT_FORMAT_VERSION,
            durable_through_lsn: durable_through.as_u64(),
            hwm,
            epoch_floor,
        };
        let bytes = zerompk::to_msgpack_vec(&file).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!("sync HWM checkpoint encode failed: {e}"),
        })?;

        let ckpt_dir = sync_hwm_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;
        let path = sync_hwm_ckpt_state_path(&ckpt_dir);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, SYNC_HWM_CKPT_STATE, &bytes)
            .map_err(|e| storage_err(&path, "publish state", &e))?;

        info!(
            core = self.core_id,
            streams = file.hwm.len(),
            producers = file.epoch_floor.len(),
            durable_through_lsn = durable_through.as_u64(),
            "sync HWM checkpoint published"
        );
        Ok(durable_through)
    }
}

/// Wrap a filesystem failure as the sync gate's typed storage error.
fn storage_err(path: &std::path::Path, action: &str, e: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "sync_hwm".to_string(),
        detail: format!(
            "sync HWM checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant-engine checkpoint writes for `CoreLoop`.
//!
//! Split out of `snapshot.rs`, which owns the checkpoint ORCHESTRATION — the
//! fold over each engine's durable LSN — and had grown a per-engine writer
//! inside it.

use tracing::info;

use crate::data::executor::checkpoint_outcome::CheckpointOutcome;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Flush every CRDT tenant engine to disk and report the LSN they are now
    /// durable through, plus the number of checkpoint files published.
    ///
    /// Each tenant's Loro state is exported per collection and written to
    /// `{data_dir}/crdt-ckpt/core-{core_id}/tenant-{tid}-coll-{hex(collection)}.ckpt`
    /// with atomic temp+rename. The per-core subdir is required because `data_dir` is
    /// shared across all cores and a tenant's CRDT state is fragmented across
    /// cores by collection — without the subdir, cores would race-overwrite
    /// the same file and persist only a partial fragment.
    ///
    /// Called from both `snapshot.rs` (explicit checkpoint command) and
    /// `compact.rs` (periodic maintenance via `maybe_run_maintenance`).
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// A `TenantCrdtEngine` is a set of in-memory `LoroDoc`s with no store
    /// behind them. `load_crdt_checkpoints` reads these files back at boot and
    /// WAL replay re-imports the deltas above them; there is no third source.
    /// So a flush that failed while the core still reported its watermark would
    /// authorise deleting the delta records that are the only remaining copy of
    /// the state this flush did not write — the documents come back at whatever
    /// version the last SUCCESSFUL checkpoint captured, with every edit since
    /// silently gone and no error at read time to show for it.
    ///
    /// Any tenant that cannot be exported or published returns `Err`, and the
    /// caller clamps the reported checkpoint LSN to the last LSN the CRDT
    /// engines were known durable through.
    ///
    /// Stamping with the core watermark mirrors `checkpoint_kv_engines`: this
    /// runs on the core's own thread between tasks, and a delta apply raises the
    /// watermark only after the `LoroDoc` has already imported it.
    pub(in crate::data::executor) fn checkpoint_crdt_engines(
        &self,
    ) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.watermark;
        if self.crdt_engines.is_empty() {
            return Ok(CheckpointOutcome {
                durable_lsn,
                files_written: 0,
            });
        }

        let ckpt_dir =
            crate::data::executor::crdt_checkpoint::crdt_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        let mut files_written = 0;
        for ((database_id, tenant_id), engine) in &self.crdt_engines {
            let database_id = database_id.as_u64();
            let tid = tenant_id.as_u64();
            // One checkpoint file per (tenant, collection) — each collection
            // owns its own LoroDoc. Filenames are
            // `tenant-{id}-coll-{hex(collection)}.ckpt`, matching the loader's
            // parse and the cluster-restore writer.
            let snapshots = engine
                .export_all_snapshots()
                .map_err(|e| crate::Error::Storage {
                    engine: "crdt".to_string(),
                    detail: format!("CRDT checkpoint export failed for tenant {tid}: {e}"),
                })?;
            for (collection, snapshot) in snapshots {
                // An empty snapshot is a collection with no state to lose, so it
                // writes no file and cannot overstate the LSN.
                if snapshot.is_empty() {
                    continue;
                }
                let fname = crate::data::executor::crdt_checkpoint::crdt_ckpt_filename(
                    database_id,
                    tid,
                    &collection,
                );
                let ckpt_path = ckpt_dir.join(&fname);
                let tmp_path = ckpt_dir.join(format!("{fname}.tmp"));
                nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &snapshot)
                    .map_err(|e| storage_err(&ckpt_path, "publish snapshot", &e))?;
                files_written += 1;
            }
        }

        if files_written > 0 {
            info!(
                core = self.core_id,
                files_written,
                total = self.crdt_engines.len(),
                durable_through_lsn = durable_lsn.as_u64(),
                "CRDT engines checkpointed"
            );
        }
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }
}

/// Wrap a filesystem failure as the CRDT engine's typed storage error.
fn storage_err(path: &std::path::Path, action: &str, e: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "crdt".to_string(),
        detail: format!(
            "CRDT checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}

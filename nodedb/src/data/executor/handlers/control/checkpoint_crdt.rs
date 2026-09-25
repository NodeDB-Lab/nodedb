// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant-engine checkpoint writes for `CoreLoop`.
//!
//! Split out of `snapshot.rs`, which owns the checkpoint ORCHESTRATION — the
//! fold over each engine's durable LSN — and had grown a per-engine writer
//! inside it. The naming, the manifest, and the load path live in
//! `data/executor/crdt_checkpoint/`.

use tracing::{info, warn};

use crate::data::executor::checkpoint_outcome::CheckpointOutcome;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::crdt_checkpoint::{
    crdt_ckpt_dir, crdt_ckpt_filename, crdt_ckpt_gen_dir, publish_crdt_generation,
    read_crdt_manifest_at, storage_err,
};

impl CoreLoop {
    /// Flush every CRDT tenant engine to disk and report the LSN they are now
    /// durable through, plus the number of checkpoint files published.
    ///
    /// Each tenant's Loro state is exported per collection into a fresh
    /// `gen-{n}/` directory, and the whole set is published with ONE atomic
    /// manifest write. That single write is the commit point: a collection the
    /// generation does not name does not exist, which is what stops a dropped
    /// collection's file from outliving it and reloading at every subsequent
    /// boot.
    ///
    /// Called from both `snapshot.rs` (explicit checkpoint command) and
    /// `compact.rs` (periodic maintenance via `maybe_run_maintenance`).
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// A `TenantCrdtEngine` is a set of in-memory `LoroDoc`s with no store
    /// behind them. `load_crdt_checkpoints` reads these files back at boot and
    /// WAL replay re-imports the deltas above them; there is no third source.
    /// So a flush that failed while the core still reported its floor would
    /// authorise deleting the delta records that are the only remaining copy of
    /// the state this flush did not write — the documents come back at whatever
    /// version the last SUCCESSFUL checkpoint captured, with every edit since
    /// silently gone and no error at read time to show for it.
    ///
    /// Any tenant that cannot be exported or published returns `Err`, and the
    /// caller clamps the reported checkpoint LSN to the last LSN the CRDT
    /// engines were known durable through.
    ///
    /// The reported LSN is the checkpoint floor (`checkpoint_floor`): the
    /// checkpoint runs on the core's own thread between tasks, so every record
    /// at or below that outcome floor that this core applied is in the export
    /// below.
    pub(in crate::data::executor) fn checkpoint_crdt_engines(
        &self,
    ) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.checkpoint_floor();

        let ckpt_dir = crdt_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        // Never reuse a generation number: a reader holding the old manifest
        // must keep seeing an intact old generation until the new one is
        // published, so the new files cannot be written over the live ones.
        let live = read_crdt_manifest_at(&ckpt_dir)?;
        let generation = live.as_ref().map_or(0, |m| m.generation.wrapping_add(1));
        let gen_dir = crdt_ckpt_gen_dir(&ckpt_dir, generation);
        // A directory already at this exact generation can only be debris from a
        // cycle that failed before publishing (its manifest was never written),
        // so clearing it discards nothing reachable.
        if gen_dir.exists() {
            std::fs::remove_dir_all(&gen_dir)
                .map_err(|e| storage_err(&gen_dir, "clear stale generation dir", &e))?;
        }
        std::fs::create_dir_all(&gen_dir)
            .map_err(|e| storage_err(&gen_dir, "create generation dir", &e))?;

        let files_written = self.write_crdt_generation(&gen_dir)?;
        publish_crdt_generation(&ckpt_dir, generation, durable_lsn)?;

        // The previous generation is now unreachable. Removing it reclaims disk
        // but is NOT required for correctness — the manifest alone decides what
        // is live — so a failure here is logged, never propagated: it must not
        // clamp an LSN whose data is already safely published.
        if let Some(old) = live {
            let old_dir = crdt_ckpt_gen_dir(&ckpt_dir, old.generation);
            if old_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&old_dir)
            {
                warn!(
                    core = self.core_id,
                    dir = %old_dir.display(),
                    error = %e,
                    "failed to remove superseded CRDT checkpoint generation; it is \
                     unreachable and will be retried next cycle"
                );
            }
        }

        info!(
            core = self.core_id,
            generation,
            files_written,
            tenants = self.crdt_engines.len(),
            durable_through_lsn = durable_lsn.as_u64(),
            "CRDT checkpoint published"
        );
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }

    /// Write one file per live `(database, tenant, collection)` into `gen_dir`.
    /// Returns the count.
    fn write_crdt_generation(&self, gen_dir: &std::path::Path) -> crate::Result<usize> {
        let mut files_written = 0usize;
        for ((database_id, tenant_id), engine) in &self.crdt_engines {
            let database_id = database_id.as_u64();
            let tid = tenant_id.as_u64();
            let snapshots = engine
                .export_all_snapshots()
                .map_err(|e| crate::Error::Storage {
                    engine: "crdt".to_string(),
                    detail: format!("CRDT checkpoint export failed for tenant {tid}: {e}"),
                })?;
            for (collection, snapshot) in snapshots {
                // Written even when the collection holds no rows: the file
                // records that the collection is durably EMPTY. A collection
                // absent from the engine writes nothing, and that is sound only
                // because `gen_dir` is FRESH — the manifest swing retires the
                // previous generation whole.
                let fname = crdt_ckpt_filename(database_id, tid, &collection);
                // Raw bytes, not the checkpoint frame: a Loro snapshot carries
                // its own version and checksum, and the load path reads it back
                // the same way.
                nodedb_wal::segment::atomic_write_fsync(gen_dir, &fname, &snapshot)
                    .map_err(|e| storage_err(&gen_dir.join(&fname), "publish snapshot", &e))?;
                files_written += 1;
            }
        }
        Ok(files_written)
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_types::DatabaseId;

    use crate::data::executor::crdt_checkpoint::open_core_at;
    use crate::types::TenantId;

    /// State that has left the core between two cycles must not reload at boot.
    /// The flush reports the core's floor either way, so the WAL records that
    /// removed it can be deleted. The previous generation's file must not stay
    /// reachable, or the collection comes back at every boot.
    #[test]
    fn state_dropped_between_cycles_does_not_survive_the_next_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        let db = DatabaseId::DEFAULT;
        let tid = TenantId::new(7);

        core.get_crdt_engine(db, tid)
            .expect("create CRDT engine")
            .doc_upsert("orders", "row-1", &[("qty", LoroValue::I64(2))])
            .expect("write a CRDT row");
        let first = core
            .checkpoint_crdt_engines()
            .expect("first flush must publish");
        assert_eq!(
            first.files_written, 1,
            "the collection holding state must be written"
        );

        // The tenant's engine is evicted from this core — a tenant purge, or a
        // vShard that moved away.
        core.crdt_engines.remove(&(db, tid));
        let second = core
            .checkpoint_crdt_engines()
            .expect("second flush must publish");
        assert_eq!(
            second.files_written, 0,
            "state absent from the core must contribute no file to the new generation"
        );
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored.load_crdt_checkpoints().expect("load must succeed");
        assert!(
            restored.crdt_engines.is_empty(),
            "a collection absent from the published generation must not reload"
        );
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The CRDT checkpoint load path: read the published generation whole and
//! import every collection's Loro snapshot.

use tracing::info;

use super::manifest::{read_crdt_manifest_at, storage_err};
use super::paths::{crdt_ckpt_dir, crdt_ckpt_gen_dir, parse_crdt_ckpt_stem};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

/// One collection's restored snapshot, read before anything is imported.
struct DecodedCrdtCollection {
    database_id: crate::types::DatabaseId,
    tenant_id: crate::types::TenantId,
    collection: String,
    snapshot: Vec<u8>,
}

impl CoreLoop {
    /// Load CRDT checkpoints from disk on startup, before WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/crdt-ckpt/core-{core_id}/`), and within it only the
    /// generation the manifest names. A collection dropped since the last cycle
    /// has no file in the live generation, so it does not come back — under the
    /// previous flat layout its file survived every subsequent cycle and
    /// reloaded at boot forever.
    ///
    /// Each file is a full Loro snapshot; importing it is the same idempotent
    /// `state.import` used by delta apply, so a subsequent WAL replay that
    /// re-imports deltas already folded into the checkpoint is a safe no-op.
    ///
    /// # Fail-stop on corruption
    ///
    /// The CRDT checkpoint contributes a durable LSN that gates WAL truncation,
    /// so once truncation has passed it a corrupt checkpoint is unrecoverable:
    /// a read failure, an unparseable filename, a failed CRDT engine create, or
    /// a rejected Loro import all propagate as `Err` and the boot sequence
    /// refuses to bring the core up, instead of silently serving truncated
    /// state. An absent checkpoint directory — or one with no manifest — is not
    /// an error: WAL replay reconstructs everything.
    pub fn load_crdt_checkpoints(&mut self) -> crate::Result<()> {
        let crs = self.crs_context();
        let ckpt_dir = crdt_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = read_crdt_manifest_at(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = crdt_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Read the WHOLE generation before importing any of it, so a file that
        // cannot be read aborts boot rather than leaving half the tenants at
        // this generation's version and half at nothing.
        let decoded = self.read_crdt_generation(&gen_dir)?;

        let loaded = decoded.len();
        for entry in decoded {
            let engine = self.get_crdt_engine(entry.database_id, entry.tenant_id)?;
            engine.import_snapshot_bytes(&entry.collection, &entry.snapshot, &crs)?;
        }

        self.floors.crdt_durable_lsn = Lsn::new(manifest.durable_through_lsn);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            loaded,
            durable_through_lsn = manifest.durable_through_lsn,
            "CRDT checkpoint restored"
        );
        Ok(())
    }

    /// Read every collection file in a generation without importing any of it.
    fn read_crdt_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> crate::Result<Vec<DecodedCrdtCollection>> {
        let entries = std::fs::read_dir(gen_dir)
            .map_err(|e| storage_err(gen_dir, "read live generation dir", &e))?;

        let mut decoded = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // This directory is engine-private, so a `.ckpt` whose stem does not
            // parse is a corrupted real checkpoint, not a foreign file to skip.
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let (database_id, tenant_id, collection) =
                parse_crdt_ckpt_stem(stem).ok_or_else(|| {
                    CheckpointDecodeError::UnparseableFilename {
                        stem: stem.to_string(),
                    }
                })?;

            let snapshot = nodedb_wal::segment::read_checkpoint_dontneed(&path)?;
            decoded.push(DecodedCrdtCollection {
                database_id: crate::types::DatabaseId::new(database_id),
                tenant_id: crate::types::TenantId::new(tenant_id),
                collection,
                snapshot,
            });
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::test_manifest_bytes;
    use super::super::paths::{CRDT_CKPT_MANIFEST, crdt_ckpt_filename};
    use super::super::test_support::open_core_at;
    use super::*;

    /// Publish `write_files`' output as generation 0 and swing the manifest.
    fn publish(ckpt_dir: &std::path::Path, write_files: impl FnOnce(&std::path::Path)) {
        let gen_dir = crdt_ckpt_gen_dir(ckpt_dir, 0);
        std::fs::create_dir_all(&gen_dir).expect("create gen dir");
        write_files(&gen_dir);
        let bytes = test_manifest_bytes(0);
        nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, CRDT_CKPT_MANIFEST, &bytes)
            .expect("write manifest");
    }

    /// An absent checkpoint directory is not corruption — a fresh data
    /// directory must load cleanly with nothing restored.
    #[test]
    fn absent_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_crdt_checkpoints()
            .expect("an absent checkpoint dir must not be treated as corruption");
    }

    /// A file with a parseable stem but bytes that are not a real Loro snapshot
    /// must fail the load: once the WAL below this generation's LSN is
    /// truncated, the checkpoint is the only durable copy of the CRDT state.
    #[test]
    fn corrupt_crdt_checkpoint_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = crdt_ckpt_dir(&core.data_dir, core.core_id);
        publish(&ckpt_dir, |gen_dir| {
            std::fs::write(
                gen_dir.join(crdt_ckpt_filename(3, 7, "orders")),
                b"not a valid Loro snapshot",
            )
            .expect("write garbage checkpoint");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_crdt_checkpoints()
            .expect_err("a corrupt CRDT checkpoint must fail the load, not silently skip it");
    }

    /// A `.ckpt` inside the live generation whose stem does not parse is
    /// corruption of a real checkpoint: this directory only ever holds files
    /// this module wrote, so guessing a key would import the state under a
    /// tenant nothing ever reads.
    #[test]
    fn unparseable_stem_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = crdt_ckpt_dir(&core.data_dir, core.core_id);
        publish(&ckpt_dir, |gen_dir| {
            std::fs::write(gen_dir.join("tenant-5.ckpt"), b"whatever bytes")
                .expect("write badly named checkpoint");
        });
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_crdt_checkpoints()
            .expect_err("an unparseable filename inside a live generation must fail the load");
    }

    /// A corrupt manifest must fail the load, not read as "nothing published".
    #[test]
    fn corrupt_manifest_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = crdt_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        std::fs::write(ckpt_dir.join(CRDT_CKPT_MANIFEST), b"not a frame")
            .expect("write garbage manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_crdt_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant checkpoint load operations for [`CoreLoop`].
//!
//! The matching write path lives in `handlers/control/checkpoint_crdt.rs`
//! (`checkpoint_crdt_engines`). Checkpoints are written per-core to
//! `{data_dir}/crdt-ckpt/core-{core_id}/db-{dbid}-tenant-{tid}-coll-{hex(collection)}.ckpt` because
//! `data_dir` is shared across cores and each core only owns the CRDT
//! fragments routed to its vShards.

use super::core_loop::CoreLoop;

/// Canonical path for a core's CRDT checkpoint directory.
///
/// Used by the write path (`checkpoint_crdt_engines`), the load path
/// (`load_crdt_checkpoints`), and the restore path
/// (`restore_crdt_checkpoints`) so all three stay in sync if the scheme
/// changes. The previous bug was exactly a path divergence between writer
/// and reader — centralising here prevents recurrence.
pub(crate) fn crdt_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("crdt-ckpt").join(format!("core-{core_id}"))
}

/// Per-collection checkpoint filename:
/// `db-{dbid}-tenant-{tid}-coll-{hex(collection)}.ckpt`.
///
/// The collection is hex-encoded so the filename is filesystem-safe (collection
/// names may contain `/`, `:` or `-`) and unambiguously parseable: hex contains
/// only `[0-9a-f]`, so the `-coll-` separator never collides with the encoded
/// name and the numeric tenant id never collides with the encoding.
pub(crate) fn crdt_ckpt_filename(database_id: u64, tenant_id: u64, collection: &str) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(collection.len() * 2);
    for b in collection.as_bytes() {
        // infallible: writing to a String never returns Err
        let _ = write!(hex, "{b:02x}");
    }
    format!("db-{database_id}-tenant-{tenant_id}-coll-{hex}.ckpt")
}

/// Parse a per-collection checkpoint file stem (no extension) back into
/// `(tenant_id, collection)`. Returns `None` for the pre-per-collection
/// `tenant-{tid}` scheme or any unparseable stem.
fn parse_crdt_ckpt_stem(stem: &str) -> Option<(u64, u64, String)> {
    let rest = stem.strip_prefix("db-")?;
    let (database_str, rest) = rest.split_once("-tenant-")?;
    let database_id = database_str.parse::<u64>().ok()?;
    let (tid_str, hex) = rest.split_once("-coll-")?;
    let tenant_id = tid_str.parse::<u64>().ok()?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let raw = hex.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    let collection = String::from_utf8(bytes).ok()?;
    Some((database_id, tenant_id, collection))
}

impl CoreLoop {
    /// Load CRDT tenant checkpoints from disk on startup, before WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/crdt-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own fragments.
    ///
    /// Each `tenant-{tid}.ckpt` is a full Loro snapshot; importing it is the
    /// same idempotent `state.import` used by delta apply, so a subsequent WAL
    /// replay that re-imports deltas already folded into the checkpoint is a
    /// safe no-op.
    ///
    /// # Fail-stop on corruption
    ///
    /// The CRDT checkpoint contributes a durable LSN that gates WAL truncation,
    /// so once truncation has passed it, a corrupt checkpoint is unrecoverable:
    /// a read failure, a failed CRDT engine create, or a rejected Loro import
    /// all propagate as `Err` and the boot sequence refuses to bring the core
    /// up, instead of silently serving truncated state. An absent checkpoint
    /// directory is not an error — WAL replay reconstructs everything. A
    /// pre-per-collection legacy filename that fails to parse is a known-benign
    /// skip (see `parse_crdt_ckpt_stem`), not corruption.
    pub fn load_crdt_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = crdt_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(&ckpt_dir)
            .map_err(|e| storage_err(&ckpt_dir, "read CRDT checkpoint dir", &e))?;

        let mut loaded = 0;
        let mut skipped_legacy = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames are
            // `"db-{dbid}-tenant-{tid}-coll-{hex(collection)}.ckpt"`.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let Some((database_id, tid, collection)) = parse_crdt_ckpt_stem(&stem) else {
                // Pre-per-collection `tenant-{tid}.ckpt` (or otherwise
                // unparseable). No released data to preserve; WAL replay
                // rebuilds. Count and skip.
                skipped_legacy += 1;
                continue;
            };
            let database_id = crate::types::DatabaseId::new(database_id);
            let tid = crate::types::TenantId::new(tid);

            let bytes = nodedb_wal::segment::read_checkpoint_dontneed(&path)?;

            let engine = self.get_crdt_engine(database_id, tid)?;
            engine.import_snapshot_bytes(&collection, &bytes)?;
            loaded += 1;
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "CRDT checkpoints loaded");
        }
        if skipped_legacy > 0 {
            tracing::info!(
                core = self.core_id,
                skipped_legacy,
                "skipped pre-per-collection CRDT checkpoint files; WAL replay rebuilds"
            );
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stem_roundtrips_through_parse() {
        let stem = crdt_ckpt_filename(3, 7, "orders");
        let stem = stem.strip_suffix(".ckpt").expect("has .ckpt suffix");
        let (database_id, tid, collection) =
            parse_crdt_ckpt_stem(stem).expect("must parse own filename");
        assert_eq!(database_id, 3);
        assert_eq!(tid, 7);
        assert_eq!(collection, "orders");
    }

    #[test]
    fn legacy_stem_is_unparseable() {
        // Pre-per-collection `tenant-{tid}.ckpt` scheme: no `-coll-` marker.
        assert!(parse_crdt_ckpt_stem("tenant-5").is_none());
    }

    /// A core rooted at `dir`, so a corrupt or legacy checkpoint file can be
    /// planted on disk and then read back through the real boot-time load
    /// path.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        use std::sync::Arc;

        use nodedb_bridge::buffer::RingBuffer;
        use nodedb_types::OrdinalClock;

        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in this test
        CoreLoop::open(0, req_rx, resp_tx, dir, hlc).expect("CoreLoop::open")
    }

    /// An absent checkpoint directory is not corruption — a fresh data
    /// directory (or one that has never checkpointed CRDT tenants) must load
    /// cleanly with nothing restored.
    #[test]
    fn absent_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_crdt_checkpoints()
            .expect("an absent checkpoint dir must not be treated as corruption");
    }

    /// A `.ckpt` file with a valid, parseable `db-{dbid}-tenant-{tid}-coll-{hex}` stem
    /// but bytes that are not a real Loro snapshot must fail the load, not be
    /// silently skipped: once the WAL below this checkpoint's LSN is
    /// truncated, the checkpoint is the only durable copy of the CRDT state,
    /// and Loro's own snapshot format self-checksums so any corruption here
    /// is a genuine, real fault rather than a foreign file to ignore.
    #[test]
    fn corrupt_crdt_checkpoint_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = crdt_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let fname = crdt_ckpt_filename(3, 7, "orders");
        std::fs::write(ckpt_dir.join(&fname), b"not a valid Loro snapshot")
            .expect("write garbage checkpoint");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_crdt_checkpoints()
            .expect_err("a corrupt CRDT checkpoint must fail the load, not silently skip it");
    }

    /// A `.ckpt` file with an unparseable (pre-per-collection legacy) stem
    /// must be a counted, non-fatal skip: it is a documented legacy naming
    /// scheme with no data to preserve, not corruption of a current
    /// checkpoint — WAL replay rebuilds whatever it held.
    #[test]
    fn legacy_stem_checkpoint_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = crdt_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        std::fs::write(
            ckpt_dir.join("tenant-5.ckpt"),
            b"legacy format, whatever bytes",
        )
        .expect("write legacy-named checkpoint");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_crdt_checkpoints()
            .expect("an unparseable legacy stem must be a skip, not fail the load");
    }
}

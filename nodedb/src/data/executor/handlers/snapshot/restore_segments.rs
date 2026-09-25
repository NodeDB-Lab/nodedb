// SPDX-License-Identifier: BUSL-1.1

//! Segment-level restore helpers: flushed timeseries partitions and
//! plain-columnar engines.
//!
//! Split from `restore.rs` to keep each file under the 500-line limit.
//! All methods are `pub(super)` — called only from `restore.rs`.

use std::path::Path;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::{TS_STAMP_FILE, write_ts_stamp};
use crate::types::TsFlushedCollectionBlob;

/// The partition commit marker. A partition directory without it is treated as
/// absent by the boot registry, so it must be the LAST file written.
const PARTITION_META: &str = "partition.meta";

/// Write one restored partition file durably inside `dir`.
///
/// Routed through the shared WAL helper so the `write → sync_data → rename →
/// fsync_dir` ordering matches the partition writer's own and cannot drift.
/// That helper rejects a `filename` that is not one plain path component.
fn durable_write_into(dir: &Path, filename: &str, bytes: &[u8]) -> crate::Result<()> {
    nodedb_wal::segment::atomic_write_fsync(dir, filename, bytes).map_err(|e| {
        crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "restore: durable write {}: {e}",
                dir.join(filename).display()
            ),
        }
    })
}

fn fsync_dir(dir: &Path) -> crate::Result<()> {
    nodedb_wal::segment::fsync_directory(dir).map_err(|e| crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("restore: fsync {}: {e}", dir.display()),
    })
}

impl CoreLoop {
    /// Restore flushed on-disk timeseries segment directories from snapshot blobs.
    ///
    /// For each captured partition:
    /// - Collision handling is fail-closed (no silent overwrites):
    ///   - If the partition dir already exists AND the registry already tracks a
    ///     partition at the same `min_ts`, we compare `row_count` and
    ///     `last_flushed_wal_lsn` to determine idempotency:
    ///     - Identical metadata → skip (idempotent re-apply).
    ///     - Different metadata → return `Storage` error (would clobber live data).
    ///   - Otherwise: create the directory, write all files, register in
    ///     `ts_registries` mirroring `flush_ts_collection`'s exact registration.
    ///
    /// `replace_mode` (Raft InstallSnapshot apply) SKIPS the fail-closed
    /// collision checks and OVERWRITES the partition directory and registry
    /// entry with the snapshot's version. `!replace_mode` (user RESTORE) keeps
    /// the fail-closed behavior described above.
    pub(super) fn restore_flushed_ts_segments(
        &mut self,
        blobs: &[TsFlushedCollectionBlob],
        replace_mode: bool,
    ) -> crate::Result<()> {
        for coll_blob in blobs {
            let (database_id, tenant_id, collection) =
                super::restore::parse_timeseries_snapshot_key(&coll_blob.collection_key);

            // The collection name is parsed out of a snapshot-supplied key and
            // becomes the last component of the segment directory.
            if !nodedb_types::is_plain_path_component(&collection) {
                return Err(crate::Error::Storage {
                    engine: "timeseries".into(),
                    detail: format!(
                        "restore: snapshot key '{}' yields collection name '{collection}', \
                         which is not a plain path component",
                        coll_blob.collection_key,
                    ),
                });
            }

            let segment_dir = super::super::timeseries::paths::ts_collection_dir(
                &self.data_dir,
                database_id,
                tenant_id,
                &collection,
            );

            let reg_key = (
                nodedb_types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tenant_id),
                collection.clone(),
            );

            for part_blob in &coll_blob.partitions {
                // Deserialize PartitionMeta from the embedded msgpack bytes.
                let meta: nodedb_types::timeseries::PartitionMeta =
                    zerompk::from_msgpack(&part_blob.meta_bytes).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!(
                                "restore: deserialize PartitionMeta for {}/{}: {e}",
                                collection, part_blob.dir_name
                            ),
                        }
                    })?;

                // `dir_name` and the file names below arrive inside the
                // snapshot blob — a Raft InstallSnapshot payload or a user
                // RESTORE. They are joined onto `segment_dir` and one of them
                // is handed to `remove_dir_all`, so a name that is not a single
                // plain component would read, write, and delete outside the
                // data directory.
                if !nodedb_types::is_plain_path_component(&part_blob.dir_name) {
                    return Err(crate::Error::Storage {
                        engine: "timeseries".into(),
                        detail: format!(
                            "restore: snapshot for collection '{collection}' carries partition \
                             directory name '{}', which is not a plain path component",
                            part_blob.dir_name,
                        ),
                    });
                }
                for (filename, _) in &part_blob.files {
                    if !nodedb_types::is_plain_path_component(filename) {
                        return Err(crate::Error::Storage {
                            engine: "timeseries".into(),
                            detail: format!(
                                "restore: snapshot partition '{}' of collection \
                                 '{collection}' carries file name '{filename}', which is not a \
                                 plain path component",
                                part_blob.dir_name,
                            ),
                        });
                    }
                }

                let partition_dir = segment_dir.join(&part_blob.dir_name);

                // Collision check: registry already knows this min_ts key.
                // Skipped under `replace_mode` (Raft install): the snapshot's
                // partition OVERWRITES the local one — `registry.import` replaces
                // the entry keyed by min_ts and the directory is wiped + rewritten
                // below.
                if !replace_mode
                    && let Some(registry) = self.ts_registries.get(&reg_key)
                    && let Some(existing) = registry.get(meta.min_ts)
                {
                    let is_identical = existing.meta.row_count == meta.row_count
                        && existing.meta.last_flushed_wal_lsn == meta.last_flushed_wal_lsn;
                    if is_identical {
                        // Idempotent: same partition already present, skip.
                        continue;
                    }
                    return Err(crate::Error::Storage {
                        engine: "timeseries".into(),
                        detail: format!(
                            "restore: partition collision for collection '{}' min_ts={}: \
                                 existing (rows={}, lsn={}) differs from snapshot \
                                 (rows={}, lsn={}); refusing to overwrite live data",
                            collection,
                            meta.min_ts,
                            existing.meta.row_count,
                            existing.meta.last_flushed_wal_lsn,
                            meta.row_count,
                            meta.last_flushed_wal_lsn,
                        ),
                    });
                }

                // Also check the filesystem: if the directory already exists
                // and is non-empty, treat it as a collision. Skipped under
                // `replace_mode` — the directory is removed and rewritten below.
                if !replace_mode && partition_dir.exists() {
                    let is_empty = std::fs::read_dir(&partition_dir)
                        .map_err(crate::Error::Io)
                        .map(|mut d| d.next().is_none())?;
                    if !is_empty {
                        return Err(crate::Error::Storage {
                            engine: "timeseries".into(),
                            detail: format!(
                                "restore: partition directory '{}' already exists for \
                                 collection '{}'; refusing to overwrite live data",
                                part_blob.dir_name, collection,
                            ),
                        });
                    }
                }

                // Stage the whole partition beside its final name, then swap it
                // in with a rename. The live directory is never destroyed
                // before the replacement is durable, so a crash at any point
                // leaves one COMPLETE partition rather than neither.
                //
                // Neither scratch name starts with `ts-`, which is the prefix
                // the boot registry scan and the orphan sweeper key on — a
                // half-finished restore is therefore invisible to both rather
                // than being adopted as a partition.
                let staging_name = format!("restore-stage-{}", part_blob.dir_name);
                let backup_name = format!("restore-backup-{}", part_blob.dir_name);
                let staging_dir = segment_dir.join(&staging_name);
                let backup_dir = segment_dir.join(&backup_name);
                for scratch in [&staging_dir, &backup_dir] {
                    if scratch.exists() {
                        // Remains of a restore that crashed mid-swap.
                        std::fs::remove_dir_all(scratch)?;
                    }
                }
                std::fs::create_dir_all(&staging_dir)?;

                // Segment files first; `partition.meta` is the commit point and
                // must not become visible before the columns it describes.
                for (filename, bytes) in &part_blob.files {
                    if filename.as_str() == PARTITION_META || filename.as_str() == TS_STAMP_FILE {
                        continue;
                    }
                    durable_write_into(&staging_dir, filename, bytes)?;
                }
                // A stamp names records of the core that wrote it, so the
                // snapshot's never applies here. The restored rows come from
                // no local record: the partition carries the collection's
                // local stamp, which names nothing new.
                let local_stamp = self
                    .ts_replay_stamps
                    .get(&reg_key)
                    .cloned()
                    .unwrap_or_default();
                write_ts_stamp(&staging_dir, &local_stamp)?;
                if let Some((_, bytes)) = part_blob
                    .files
                    .iter()
                    .find(|(filename, _)| filename.as_str() == PARTITION_META)
                {
                    durable_write_into(&staging_dir, PARTITION_META, bytes)?;
                }
                fsync_dir(&staging_dir)?;

                if partition_dir.exists() {
                    nodedb_wal::segment::atomic_swap_dirs_fsync(
                        &segment_dir,
                        &part_blob.dir_name,
                        &backup_name,
                        &staging_name,
                    )
                    .map_err(|e| crate::Error::Storage {
                        engine: "timeseries".into(),
                        detail: format!("restore: swap partition {}: {e}", partition_dir.display()),
                    })?;
                    // The new partition is durably named; the old one is now
                    // pure garbage.
                    std::fs::remove_dir_all(&backup_dir)?;
                } else {
                    std::fs::rename(&staging_dir, &partition_dir)?;
                    fsync_dir(&segment_dir)?;
                }

                // Register the restored partition in ts_registries, mirroring
                // exactly the registration step in flush_ts_collection.
                let registry = self
                    .ts_registries
                    .entry(reg_key.clone())
                    .or_insert_with(|| {
                        crate::engine::timeseries::partition_registry::PartitionRegistry::new(
                            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                        )
                    });
                let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                    meta,
                    dir_name: part_blob.dir_name.clone(),
                };
                // `insert_partition`, not `import`: partitions sharing a min_ts
                // must both stay reachable. Re-registering the same directory
                // is idempotent, which is what a repeated restore does.
                registry.insert_partition(pe);
            }
        }
        Ok(())
    }

    /// Restore plain-columnar (and spatial) engine state from snapshot entries.
    ///
    /// For each `(collection_key, msgpack_bytes)` entry:
    /// - Deserialises the `ColumnarEngineSnapshot` from `msgpack_bytes`.
    /// - Reconstructs the `MutationEngine` via `MutationEngine::from_snapshot`.
    /// - Inserts the engine into `columnar_engines` and any returned flushed
    ///   segment blobs into `columnar_flushed_segments`.
    ///
    /// **Collision handling depends on `replace_mode`:**
    /// - `!replace_mode` (user RESTORE): fail-closed — if either
    ///   `columnar_engines` or `columnar_flushed_segments` already contains an
    ///   entry for the key, return `Error::Storage` rather than silently
    ///   overwriting live data.
    /// - `replace_mode` (Raft InstallSnapshot apply): SKIP the guards and
    ///   OVERWRITE — the engine entry is replaced, and the flushed segments /
    ///   surrogates maps are SET to the snapshot's (the stale entries are removed
    ///   when the snapshot carries none) so they are never appended to stale
    ///   state.
    pub(super) fn restore_columnar_engines(
        &mut self,
        entries: &[(String, Vec<u8>)],
        replace_mode: bool,
    ) -> crate::Result<()> {
        for (collection_key, bytes) in entries {
            let (database_id, tenant_id, collection) =
                super::restore::parse_timeseries_snapshot_key(collection_key);

            let engine_key = (
                nodedb_types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tenant_id),
                collection.clone(),
            );

            // Fail-closed (user RESTORE only): refuse to overwrite any live
            // engine or flushed segment state that was present before this
            // restore call. Skipped under `replace_mode` (Raft install).
            if !replace_mode {
                if self.columnar_engines.contains_key(&engine_key) {
                    return Err(crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!(
                            "restore: columnar engine already exists for collection '{collection}' \
                             (db={database_id}, tenant={tenant_id}); refusing to overwrite live data"
                        ),
                    });
                }
                if self.columnar_flushed_segments.contains_key(&engine_key) {
                    return Err(crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!(
                            "restore: flushed segment state already exists for collection \
                             '{collection}' (db={database_id}, tenant={tenant_id}); \
                             refusing to overwrite live data"
                        ),
                    });
                }
            }

            let snap: nodedb_columnar::ColumnarEngineSnapshot = zerompk::from_msgpack(bytes)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "restore: deserialize ColumnarEngineSnapshot for '{collection}': {e}"
                    ),
                })?;

            let (engine, flushed, flushed_surrogates) =
                nodedb_columnar::MutationEngine::from_snapshot(snap).map_err(|e| {
                    crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!(
                            "restore: from_snapshot for collection '{collection}': {e}"
                        ),
                    }
                })?;

            self.columnar_engines.insert(engine_key.clone(), engine);
            if !flushed.is_empty() {
                self.columnar_flushed_segments
                    .insert(engine_key.clone(), flushed);
            } else if replace_mode {
                // Replace: the snapshot carries no flushed segments, so any stale
                // local entry must be dropped (not left behind to mismatch the
                // overwritten engine).
                self.columnar_flushed_segments.remove(&engine_key);
            }
            // Re-attach the cross-engine surrogate sidecar under the SAME key so
            // prefiltered scans see flushed rows post-restore. Old snapshots
            // carry empty surrogates (non-empty segments): we skip populating
            // the sidecar, so those rows read as `None`-surrogate and are
            // conservatively excluded under an active prefilter — the correct
            // backward-compat behavior. Order is preserved so segment_id ==
            // index + 1 holds for the sidecar too.
            if !flushed_surrogates.is_empty() {
                self.columnar_flushed_surrogates
                    .insert(engine_key, flushed_surrogates);
            } else if replace_mode {
                // Replace: drop any stale surrogate sidecar when the snapshot
                // carries none, keeping the sidecar consistent with the
                // overwritten engine + flushed segments.
                self.columnar_flushed_surrogates.remove(&engine_key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::types::TsFlushedPartitionBlob;
    use nodedb_bridge::buffer::RingBuffer;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let hlc = Arc::new(nodedb_types::OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx);
        drop(resp_rx);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    fn partition_meta(min_ts: i64) -> nodedb_types::timeseries::PartitionMeta {
        nodedb_types::timeseries::PartitionMeta {
            min_ts,
            max_ts: min_ts + 1,
            row_count: 1,
            size_bytes: 1,
            schema_version: 1,
            state: nodedb_types::timeseries::PartitionState::Sealed,
            interval_ms: 0,
            last_flushed_wal_lsn: 7,
            column_stats: std::collections::HashMap::new(),
            max_system_ts: 0,
        }
    }

    fn blob(dir_name: &str, min_ts: i64) -> TsFlushedCollectionBlob {
        let meta = partition_meta(min_ts);
        let meta_bytes = zerompk::to_msgpack_vec(&meta).expect("encode meta");
        TsFlushedCollectionBlob {
            collection_key: "0:0:metrics".to_string(),
            partitions: vec![TsFlushedPartitionBlob {
                dir_name: dir_name.to_string(),
                meta_bytes,
                // Deliberately meta-first so an in-order write would publish
                // the commit marker before the column it describes.
                files: vec![
                    (PARTITION_META.to_string(), b"new-meta".to_vec()),
                    ("value.col".to_string(), b"new-col".to_vec()),
                ],
            }],
        }
    }

    fn ts_dir(root: &std::path::Path) -> std::path::PathBuf {
        super::super::super::timeseries::paths::ts_collection_dir(root, 0, 0, "metrics")
    }

    fn names(dir: &std::path::Path) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .expect("read_dir")
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .collect();
        out.sort();
        out
    }

    /// The restore must land as a whole-directory swap: the previous
    /// partition's files are gone, the snapshot's files are all present, and no
    /// staging/backup scratch survives to be mistaken for a partition.
    #[test]
    fn replace_swaps_partition_atomically_and_leaves_no_scratch() {
        let root = TempDir::new().expect("tempdir");
        let mut core = open_core(root.path());

        let segment_dir = ts_dir(root.path());
        let partition_dir = segment_dir.join("ts-1_2");
        std::fs::create_dir_all(&partition_dir).expect("mkdir");
        std::fs::write(partition_dir.join("stale.col"), b"old").expect("write stale");
        std::fs::write(partition_dir.join(PARTITION_META), b"old-meta").expect("write old meta");

        // Remains of an earlier restore that died mid-swap.
        let leftover = segment_dir.join("restore-stage-ts-1_2");
        std::fs::create_dir_all(&leftover).expect("mkdir leftover");
        std::fs::write(leftover.join("junk"), b"junk").expect("write junk");

        core.restore_flushed_ts_segments(&[blob("ts-1_2", 1)], true)
            .expect("restore");

        assert_eq!(
            names(&partition_dir),
            vec![
                PARTITION_META.to_string(),
                TS_STAMP_FILE.to_string(),
                "value.col".to_string()
            ],
            "the swapped-in partition must hold exactly the snapshot's files and \
             the local stamp"
        );
        assert_eq!(
            std::fs::read(partition_dir.join("value.col")).expect("read col"),
            b"new-col"
        );
        assert_eq!(
            std::fs::read(partition_dir.join(PARTITION_META)).expect("read meta"),
            b"new-meta"
        );
        assert_eq!(
            names(&segment_dir),
            vec!["ts-1_2".to_string()],
            "no staging or backup directory may survive a successful restore"
        );
    }

    /// A restore into a collection with no existing partition still commits
    /// through staging + rename, and the boot-visible directory only appears
    /// once every file inside it is written.
    #[test]
    fn fresh_restore_publishes_a_complete_partition() {
        let root = TempDir::new().expect("tempdir");
        let mut core = open_core(root.path());

        core.restore_flushed_ts_segments(&[blob("ts-5_6", 5)], false)
            .expect("restore");

        let partition_dir = ts_dir(root.path()).join("ts-5_6");
        assert_eq!(
            names(&partition_dir),
            vec![
                PARTITION_META.to_string(),
                TS_STAMP_FILE.to_string(),
                "value.col".to_string()
            ]
        );
        // No staging file may be left inside the published dir.
        assert!(
            !names(&partition_dir).iter().any(|n| n.ends_with(".tmp")),
            "atomic write tmp files must be renamed away"
        );

        let key = (
            nodedb_types::DatabaseId::new(0),
            crate::types::TenantId::new(0),
            "metrics".to_string(),
        );
        let registry = core.ts_registries.get(&key).expect("registry registered");
        assert_eq!(registry.get(5).expect("partition entry").dir_name, "ts-5_6");
    }
}

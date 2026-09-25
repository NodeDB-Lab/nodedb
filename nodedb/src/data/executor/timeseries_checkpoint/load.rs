// SPDX-License-Identifier: BUSL-1.1

//! Rebuilding `ts_registries` from the partition directories on disk.
//!
//! The partitions ARE the timeseries checkpoint, so this is its loader: it makes
//! the flushed rows reachable again, and it installs the per-collection replay
//! stamp (`stamp.rs`) that stops `replay_timeseries_wal` re-appending records
//! those partitions already contain.
//!
//! A partition directory is committed by its `partition.meta`, which
//! `ColumnarSegmentWriter::write_partition` writes last and atomically. A
//! directory without one is an incomplete write — the crash-time remains of a
//! flush that never reached its commit point — and is ignored here, exactly as
//! `PartitionRegistry::cleanup_orphans` would remove it. A meta that is present
//! but cannot be read or decoded is NOT ignored: that is corruption of a
//! committed partition, and silently dropping it would under-restore the
//! collection while its stamp says its records need no replay. The same holds
//! for a stamp file that is present but does not decode.

use tracing::info;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::partition_registry::{PartitionEntry, PartitionRegistry};
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Rebuild every timeseries collection's partition registry from disk.
    ///
    /// Called once at boot, BEFORE `replay_all_wal`. Replay's skip gate reads
    /// the collection stamps this loads. Loaded lazily instead, at the first
    /// scan, they would be absent at replay: every retained `TimeseriesBatch`
    /// would append again on top of the partition that holds it, and the scan
    /// reads partitions and memtable together, so nothing masks the duplicate.
    ///
    /// A collection whose registry cannot be rebuilt is fail-stop: a
    /// `partition.meta` or stamp that exists but will not decode is corruption
    /// of committed state this core is about to claim is durable, and the WAL
    /// behind it may already be gone.
    pub fn load_ts_registries(&mut self) -> crate::Result<()> {
        let ts_root = self.data_dir.join("ts");
        if !ts_root.exists() {
            return Ok(());
        }
        // An aside directory is the remains of a truncate whose WAL record
        // precedes it; replay re-applies the truncate, so the aside rows are
        // never wanted.
        crate::data::executor::handlers::timeseries::remove_truncating_leftovers(&ts_root)?;

        let mut loaded = 0usize;
        let mut partitions = 0usize;
        for (database_id, tenant_id, collection) in enumerate_ts_collections(&ts_root) {
            self.ensure_ts_registry(tenant_id, database_id, &collection)?;
            let key = (database_id, tenant_id, collection);
            if let Some(reg) = self.ts_registries.get(&key) {
                loaded += 1;
                partitions += reg.partition_count();
            }
        }

        if loaded > 0 {
            info!(
                core = self.core_id,
                collections = loaded,
                partitions,
                "timeseries partition registries restored"
            );
        }
        Ok(())
    }

    /// Ensure the partition registry and the replay stamp are loaded for one
    /// timeseries collection.
    ///
    /// A no-op once the collection's registry is present, so the boot load above
    /// and the lazy first-scan path can both call it.
    pub(in crate::data::executor) fn ensure_ts_registry(
        &mut self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
    ) -> crate::Result<()> {
        let key = (database_id, tid, collection.to_string());
        if self.ts_registries.contains_key(&key) {
            return Ok(());
        }
        let ts_dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        if !ts_dir.exists() {
            return Ok(());
        }

        let registry = read_registry(&ts_dir, self.segment_keks.ts_segment_kek.as_ref())?;
        let stamp = super::stamp::read_collection_ts_stamp(&ts_dir, &registry)?;
        let merged = match self.ts_replay_stamps.get(&key) {
            Some(held) => held.union(&stamp),
            None => stamp,
        };
        self.ts_replay_stamps.insert(key.clone(), merged);
        if registry.partition_count() > 0 {
            info!(
                collection,
                partitions = registry.partition_count(),
                "loaded partition registry from disk"
            );
        }
        self.ts_registries.insert(key, registry);
        Ok(())
    }
}

/// Walk `{data_dir}/ts/{database_id}/{tenant_id}/{collection}` and yield every
/// collection that has a directory.
///
/// Unparseable directory names are skipped: the layout is written only by
/// `ts_collection_dir`, so a name that is not a `u64` at the database or tenant
/// level was not put there by this engine.
fn enumerate_ts_collections(ts_root: &std::path::Path) -> Vec<(DatabaseId, TenantId, String)> {
    let mut out = Vec::new();
    let Ok(db_dirs) = std::fs::read_dir(ts_root) else {
        return out;
    };
    for db_dir in db_dirs.flatten() {
        let Some(database_id) = db_dir
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u64>().ok())
        else {
            continue;
        };
        let Ok(tenant_dirs) = std::fs::read_dir(db_dir.path()) else {
            continue;
        };
        for tenant_dir in tenant_dirs.flatten() {
            let Some(tenant_id) = tenant_dir
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u64>().ok())
            else {
                continue;
            };
            let Ok(coll_dirs) = std::fs::read_dir(tenant_dir.path()) else {
                continue;
            };
            for coll_dir in coll_dirs.flatten() {
                if !coll_dir.path().is_dir() {
                    continue;
                }
                let Some(collection) = coll_dir.file_name().to_str().map(|s| s.to_string()) else {
                    continue;
                };
                // Never a collection: a truncate's aside directory.
                if crate::data::executor::handlers::timeseries::is_truncating_leftover(&collection)
                {
                    continue;
                }
                out.push((
                    DatabaseId::new(database_id),
                    TenantId::new(tenant_id),
                    collection,
                ));
            }
        }
    }
    out
}

/// Build a registry from every committed partition directory under `ts_dir`.
fn read_registry(
    ts_dir: &std::path::Path,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<PartitionRegistry> {
    let mut registry =
        PartitionRegistry::new(nodedb_types::timeseries::TieredPartitionConfig::origin_defaults());

    let entries = std::fs::read_dir(ts_dir).map_err(|e| crate::Error::Storage {
        engine: "timeseries".to_string(),
        detail: format!("read partition dir {}: {e}", ts_dir.display()),
    })?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(dir_name) = name.to_str() else {
            continue;
        };
        if !dir_name.starts_with("ts-") || !entry.path().is_dir() {
            continue;
        }
        let meta_path = entry.path().join("partition.meta");
        if !meta_path.exists() {
            // Not committed: the meta write is the commit point, so this
            // directory is the remains of an interrupted flush.
            continue;
        }
        let meta = read_partition_meta(&meta_path, kek)?;
        // `insert_partition`, not `import`: two committed partitions can share a
        // min_ts, and filing both under it would make one unreachable to every
        // query even though its rows are on disk.
        registry.insert_partition(PartitionEntry {
            meta,
            dir_name: dir_name.to_string(),
        });
    }
    Ok(registry)
}

/// Read and decode one committed `partition.meta`.
///
/// Reads via `read_checkpoint_dontneed`: the metas are consumed once at boot and
/// then superseded by the in-memory registry, so leaving them pinned in the page
/// cache costs memory the hot workload needs.
///
/// Decrypts when the file carries the `SEGT` envelope. `write_partition` wraps
/// EVERY file it writes — the meta included — when a segment KEK is installed,
/// so a raw parse would find nothing but ciphertext and hand back an empty
/// registry: partitions unreachable, records un-gated. Nothing installs the
/// timeseries segment KEK today (`set_ts_segment_kek` has no caller outside
/// tests), so that is a hole the first caller would fall into rather than a live
/// one — which is why it is closed here, in the reader, alongside the writer
/// that already encrypts.
fn read_partition_meta(
    path: &std::path::Path,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<nodedb_types::timeseries::PartitionMeta> {
    let raw =
        nodedb_wal::segment::read_checkpoint_dontneed(path).map_err(|e| crate::Error::Storage {
            engine: "timeseries".to_string(),
            detail: format!("read {}: {e}", path.display()),
        })?;

    let encrypted = crate::engine::timeseries::columnar_segment::encrypt::is_encrypted(&raw)
        .map_err(|e| crate::Error::Storage {
            engine: "timeseries".to_string(),
            detail: format!("sniff {}: {e}", path.display()),
        })?;
    let bytes = if encrypted {
        let key = kek.ok_or_else(|| crate::Error::Storage {
            engine: "timeseries".to_string(),
            detail: format!(
                "{} is SEGT-encrypted but no timeseries segment key is installed",
                path.display()
            ),
        })?;
        crate::engine::timeseries::columnar_segment::encrypt::decrypt_file(key, &raw).map_err(
            |e| crate::Error::Storage {
                engine: "timeseries".to_string(),
                detail: format!("decrypt {}: {e}", path.display()),
            },
        )?
    } else {
        raw
    };

    sonic_rs::from_slice(&bytes).map_err(|e| crate::Error::Serialization {
        format: "json".to_string(),
        detail: format!("parse {}: {e}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumerate_walks_database_tenant_collection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ts_root = dir.path().join("ts");
        std::fs::create_dir_all(ts_root.join("0").join("1").join("metrics")).expect("mkdir");
        std::fs::create_dir_all(ts_root.join("0").join("2").join("events")).expect("mkdir");
        // A non-numeric level is not this engine's layout and must be skipped
        // rather than mis-attributed to some tenant.
        std::fs::create_dir_all(ts_root.join("not-a-db").join("1").join("x")).expect("mkdir");

        let mut found = enumerate_ts_collections(&ts_root);
        found.sort_by_key(|entry| (entry.1.as_u64(), entry.2.clone()));
        assert_eq!(
            found,
            vec![
                (DatabaseId::new(0), TenantId::new(1), "metrics".to_string()),
                (DatabaseId::new(0), TenantId::new(2), "events".to_string()),
            ]
        );
    }

    /// A partition directory with no `partition.meta` never reached its commit
    /// point. Restoring it would register a partition whose columns may be
    /// missing or half-written.
    #[test]
    fn uncommitted_partition_directory_is_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("ts-1_2")).expect("mkdir");
        let registry = read_registry(dir.path(), None).expect("read registry");
        assert_eq!(registry.partition_count(), 0);
    }

    /// A committed partition whose meta cannot be decoded is corruption of state
    /// this core is about to claim is durable. Skipping it quietly would leave
    /// the collection short of rows AND leave its records un-gated.
    #[test]
    fn corrupt_partition_meta_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part = dir.path().join("ts-1_2");
        std::fs::create_dir_all(&part).expect("mkdir");
        std::fs::write(part.join("partition.meta"), b"{ not json").expect("write meta");
        assert!(
            read_registry(dir.path(), None).is_err(),
            "an undecodable committed meta must surface, not be skipped"
        );
    }

    /// The boot-time entry point must surface the same corruption, not just
    /// the inner `read_registry` helper: a committed `partition.meta` this
    /// core is about to claim is durable, but cannot decode, must abort the
    /// whole boot rather than silently leave the collection un-registered
    /// (and so un-gated against replay duplicating its rows).
    #[test]
    fn load_ts_registries_is_fail_stop_on_a_corrupt_partition_meta() {
        use std::sync::Arc;

        use nodedb_bridge::buffer::RingBuffer;

        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let dir = tempfile::tempdir().expect("tempdir");
        let part = dir
            .path()
            .join("ts")
            .join("0")
            .join("1")
            .join("metrics")
            .join("ts-1_2");
        std::fs::create_dir_all(&part).expect("mkdir");
        std::fs::write(part.join("partition.meta"), b"{ not json").expect("write meta");

        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in this test
        let mut core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open");

        assert!(
            core.load_ts_registries().is_err(),
            "a committed but undecodable partition.meta must fail the boot, not \
             be logged and skipped"
        );
    }
}

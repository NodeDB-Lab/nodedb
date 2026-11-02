// SPDX-License-Identifier: BUSL-1.1

//! Installing an ended timeseries partition a RESTORE carries.
//!
//! A backup keeps every partition a TRUNCATE ended, so a read at a cut
//! before the end sees its rows after a restore as it did before. The
//! install files the partition beside the live ones:
//!
//! - Its directory name is its identity: `min_ts`, `max_ts`, `ended_at`,
//!   `max_system_ts`, `row_count` and a SHA-256 of its files. An install of
//!   a partition already filed is a no-op, so a replay of the sub-record
//!   after a restart installs nothing twice.
//! - The registry keys partitions by `min_ts` and directory name, so a live
//!   partition that shares the `min_ts` stays.
//! - The files arrive plaintext and are stored under the core's segment
//!   key, so no ciphertext crosses keys.
//! - The files are staged under a name the partition scan passes over, the
//!   meta is written last as the commit point, and one rename publishes the
//!   directory.
//! - The merge never takes an installed partition, so its identity lasts.
//! - `ended_at` is set, so `query_partitions` reads the rows at a cut below
//!   the end, and `current_partitions` never lists them: no aggregate folds
//!   them and no last-value cache reads them.
//!
//! Inside a transaction the install pushes an undo entry, and a rollback
//! removes the directory and the registry entry.

use sha2::{Digest, Sha256};

use nodedb_types::timeseries::PartitionMeta;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::{TimeseriesEndedInstallUndo, UndoEntry};
use crate::data::executor::timeseries_checkpoint::stamp::{TS_STAMP_FILE, TsCollectionKey};
use crate::engine::timeseries::columnar_segment::encrypt::encrypt_file;
use crate::engine::timeseries::columnar_segment::write_partition_meta;
use crate::engine::timeseries::partition_registry::{
    PartitionEntry, PartitionKey, PartitionRegistry,
};

use super::truncate::remove_dir_tree;

/// The file a partition's commit point is.
const PARTITION_META: &str = "partition.meta";

/// Prefix of an installed ended partition's directory. The merge never
/// takes such a partition, so its directory, and the identity its name
/// carries, lasts as long as the partition does.
pub(crate) const ENDED_INSTALL_PREFIX: &str = "ts-ended-";

/// Prefix of a staged install. It does not start with `ts-`, so the
/// partition scan and the orphan sweep pass over it.
const STAGE_PREFIX: &str = "ended-stage-";

/// One ended partition to install.
#[derive(Debug, Clone)]
pub(in crate::data::executor) struct EndedPartitionInstall {
    /// The partition's meta. `ended_at` must be set.
    pub meta: PartitionMeta,
    /// Every segment file of the partition, plaintext: `(file name, bytes)`.
    /// The install stores each under this core's segment key, and the
    /// partition's identity names the plaintext. A name is one path
    /// component. `partition.meta` and `replay.stamp` are refused:
    /// the install writes the meta, and a source stamp names records of
    /// another WAL.
    pub files: Vec<(String, Vec<u8>)>,
}

/// What an install did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) enum EndedPartitionInstalled {
    /// The partition was filed under this key.
    Installed(PartitionKey),
    /// A partition of the same identity was filed already, under this key.
    AlreadyPresent(PartitionKey),
}

fn refused(collection: &str, detail: String) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!("ended partition install into '{collection}': {detail}"),
    }
}

fn storage(action: &str, path: &std::path::Path, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("ended partition install: {action} {}: {e}", path.display()),
    }
}

/// Check every file name, and return the files sorted by name.
fn checked_files<'a>(
    collection: &str,
    files: &'a [(String, Vec<u8>)],
) -> crate::Result<Vec<&'a (String, Vec<u8>)>> {
    if files.is_empty() {
        return Err(refused(collection, "the partition carries no file".into()));
    }
    let mut sorted: Vec<&(String, Vec<u8>)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in sorted.windows(2) {
        if let [a, b] = pair
            && a.0 == b.0
        {
            return Err(refused(collection, format!("file '{}' appears twice", a.0)));
        }
    }
    for (name, _) in &sorted {
        let single =
            !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\u{0}']);
        if !single {
            return Err(refused(
                collection,
                format!("file name '{name}' is not one path component"),
            ));
        }
        if name == PARTITION_META || name == TS_STAMP_FILE {
            return Err(refused(
                collection,
                format!("file '{name}' is written by the install, never carried"),
            ));
        }
    }
    Ok(sorted)
}

/// The directory name an ended partition's identity gives it.
fn identity_dir(meta: &PartitionMeta, ended_at: i64, files: &[&(String, Vec<u8>)]) -> String {
    let mut hasher = Sha256::new();
    for (name, bytes) in files {
        hasher.update(name.as_bytes());
        hasher.update([0u8]);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    let digest = hasher.finalize();
    let hex: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{ENDED_INSTALL_PREFIX}{}_{}_e{}_s{}_n{}_{hex}",
        meta.min_ts, meta.max_ts, ended_at, meta.max_system_ts, meta.row_count
    )
}

fn fsync_dir(dir: &std::path::Path) -> crate::Result<()> {
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|e| storage("fsync", dir, e))
}

impl CoreLoop {
    /// Install the ended partition `partition` into collection `key`.
    ///
    /// Idempotent by the partition's identity: a second install of the same
    /// partition files nothing and answers `AlreadyPresent`. With
    /// `undo_log`, an install that filed the partition pushes the undo that
    /// removes it.
    pub(in crate::data::executor) fn install_ts_ended_partition(
        &mut self,
        key: &TsCollectionKey,
        partition: EndedPartitionInstall,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<EndedPartitionInstalled> {
        let (database_id, tid, collection) = (key.0, key.1, key.2.as_str());
        let Some(ended_at) = partition.meta.ended_at else {
            return Err(refused(collection, "the meta carries no ended_at".into()));
        };
        let files = checked_files(collection, &partition.files)?;
        let dir_name = identity_dir(&partition.meta, ended_at, &files);

        self.ensure_ts_registry(tid, database_id, collection)?;
        if let Some(existing) = self
            .ts_registries
            .get(key)
            .and_then(|registry| registry.key_of_dir(&dir_name))
        {
            return Ok(EndedPartitionInstalled::AlreadyPresent(existing));
        }

        let ts_dir = self.ts_dir_of(key);
        std::fs::create_dir_all(&ts_dir).map_err(|e| storage("create", &ts_dir, e))?;
        let final_dir = ts_dir.join(&dir_name);
        let mut meta = partition.meta.clone();
        meta.state = nodedb_types::timeseries::PartitionState::Sealed;
        if !final_dir.exists() {
            let stage = ts_dir.join(format!("{STAGE_PREFIX}{dir_name}"));
            remove_dir_tree(&stage)?;
            std::fs::create_dir_all(&stage).map_err(|e| storage("create", &stage, e))?;
            // The files arrive plaintext. Each is stored under this core's
            // segment key, as a flush stores its own.
            let kek = self.segment_keks.ts_segment_kek.as_ref();
            for (name, bytes) in &files {
                let stored = match kek {
                    Some(kek) => encrypt_file(kek, bytes)
                        .map_err(|e| storage("encrypt", &stage.join(name), e))?,
                    None => bytes.clone(),
                };
                nodedb_wal::segment::atomic_write_fsync(&stage, name, &stored)
                    .map_err(|e| storage("write", &stage.join(name), e))?;
            }
            // The meta is the commit point: a directory without it is no
            // partition.
            write_partition_meta(&stage, &meta, self.segment_keks.ts_segment_kek.as_ref())
                .map_err(|e| storage("write the meta of", &stage, e))?;
            fsync_dir(&stage)?;
            std::fs::rename(&stage, &final_dir).map_err(|e| storage("publish", &final_dir, e))?;
            fsync_dir(&ts_dir)?;
        }
        // A directory a crash left after its rename holds the same files:
        // its name is their digest.
        let registry = self.ts_registries.entry(key.clone()).or_insert_with(|| {
            PartitionRegistry::new(
                nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
            )
        });
        let partition_key = registry.insert_partition(PartitionEntry {
            meta,
            dir_name: dir_name.clone(),
        });
        if let Some(log) = undo_log {
            log.push(UndoEntry::TimeseriesEndedInstall(
                TimeseriesEndedInstallUndo {
                    collection_key: key.clone(),
                    partition_key: partition_key.clone(),
                },
            ));
        }
        tracing::debug!(
            core = self.core_id,
            collection,
            partition = %dir_name,
            ended_at,
            "installed ended timeseries partition"
        );
        Ok(EndedPartitionInstalled::Installed(partition_key))
    }

    /// Remove the ended partition filed under `partition_key`: its
    /// directory, then its registry entry.
    pub(in crate::data::executor) fn remove_ts_ended_partition(
        &mut self,
        key: &TsCollectionKey,
        partition_key: &PartitionKey,
    ) -> crate::Result<()> {
        let ts_dir = self.ts_dir_of(key);
        remove_dir_tree(&ts_dir.join(&partition_key.dir_name))?;
        if ts_dir.exists() {
            fsync_dir(&ts_dir)?;
        }
        if let Some(registry) = self.ts_registries.get_mut(key) {
            registry.remove(partition_key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::cagg_test_support::{
        COLLECTION, ROWS_A, TENANT, flush, ingest,
    };
    use crate::types::{DatabaseId, TenantId};
    use nodedb_columnar::VersionCut;
    use nodedb_types::timeseries::TimeRange;

    fn key() -> TsCollectionKey {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// The ended copy of the flushed partition of `core`: its files and its
    /// meta with `ended_at` set.
    fn ended_copy(core: &CoreLoop, ended_at: i64) -> EndedPartitionInstall {
        let registry = core.ts_registries.get(&key()).expect("registry");
        let (_, entry) = registry.iter().next().expect("a partition");
        let dir = core.ts_dir_of(&key()).join(&entry.dir_name);
        let mut files = Vec::new();
        for file in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let name = file.file_name().to_string_lossy().into_owned();
            if name == PARTITION_META || name == TS_STAMP_FILE {
                continue;
            }
            files.push((name, std::fs::read(file.path()).expect("read file")));
        }
        let mut meta = entry.meta.clone();
        meta.ended_at = Some(ended_at);
        EndedPartitionInstall { meta, files }
    }

    fn visible(core: &CoreLoop, cut: VersionCut) -> usize {
        core.ts_registries
            .get(&key())
            .expect("registry")
            .query_partitions(&TimeRange::new(i64::MIN, i64::MAX), cut)
            .len()
    }

    /// An ended partition sharing the live partition's `min_ts` installs
    /// beside it, reads only below its end, installs once, and leaves on
    /// rollback.
    #[test]
    fn an_ended_partition_installs_once_beside_the_live_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, ROWS_A, 1),
            crate::bridge::envelope::Status::Ok
        );
        flush(&mut core);
        let copy = ended_copy(&core, 50);

        let mut undo = Vec::new();
        let first = core
            .install_ts_ended_partition(&key(), copy.clone(), Some(&mut undo))
            .expect("install");
        let EndedPartitionInstalled::Installed(installed) = first else {
            panic!("the first install files the partition: {first:?}");
        };
        assert_eq!(undo.len(), 1);
        assert_eq!(visible(&core, VersionCut::At(49)), 2, "both below the end");
        assert_eq!(
            visible(&core, VersionCut::At(50)),
            1,
            "the live one at the end"
        );
        assert_eq!(
            core.ts_registries
                .get(&key())
                .expect("registry")
                .current_partitions()
                .count(),
            1,
            "an ended partition is never current"
        );

        let again = core
            .install_ts_ended_partition(&key(), copy, None)
            .expect("install again");
        assert_eq!(
            again,
            EndedPartitionInstalled::AlreadyPresent(installed.clone())
        );

        core.rollback_undo_log(0, TENANT, undo).expect("rollback");
        assert_eq!(visible(&core, VersionCut::At(49)), 1);
        assert!(!core.ts_dir_of(&key()).join(&installed.dir_name).exists());
    }

    /// A restart reloads the installed partition, and an install after it
    /// is a no-op.
    #[test]
    fn an_installed_partition_survives_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let copy = {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            assert_eq!(
                ingest(&mut core, ROWS_A, 1),
                crate::bridge::envelope::Status::Ok
            );
            flush(&mut core);
            let copy = ended_copy(&core, 50);
            core.install_ts_ended_partition(&key(), copy.clone(), None)
                .expect("install");
            copy
        };
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ensure_ts_registry(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION)
            .expect("load");
        assert_eq!(visible(&core, VersionCut::At(49)), 2);
        let again = core
            .install_ts_ended_partition(&key(), copy, None)
            .expect("install again");
        assert!(matches!(again, EndedPartitionInstalled::AlreadyPresent(_)));
    }

    /// The redo sub-record installs the partition on replay, and a second
    /// replay of it installs nothing.
    #[test]
    fn the_redo_sub_record_replays_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, ROWS_A, 1),
            crate::bridge::envelope::Status::Ok
        );
        flush(&mut core);
        let copy = ended_copy(&core, 50);
        let payload = crate::wal::TimeseriesEndedPartitionRedo {
            collection: COLLECTION.to_string(),
            meta: copy.meta,
            files: copy.files,
        }
        .encode()
        .expect("encode");
        let record = nodedb_wal::WalRecord::new(nodedb_wal::record::WalRecordArgs {
            record_type: nodedb_wal::record::RecordType::TimeseriesBatch as u32,
            lsn: 9,
            tenant_id: TENANT,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        let tombstones = nodedb_wal::TombstoneSet::new();
        core.replay_timeseries_wal(std::slice::from_ref(&record), 1, &tombstones);
        assert_eq!(visible(&core, VersionCut::At(49)), 2);
        core.replay_timeseries_wal(std::slice::from_ref(&record), 1, &tombstones);
        assert_eq!(visible(&core, VersionCut::At(49)), 2, "installed once");
    }

    #[test]
    fn a_carried_meta_or_stamp_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, ROWS_A, 1),
            crate::bridge::envelope::Status::Ok
        );
        flush(&mut core);
        let mut copy = ended_copy(&core, 50);
        copy.files.push((TS_STAMP_FILE.into(), vec![1]));
        assert!(core.install_ts_ended_partition(&key(), copy, None).is_err());
        let mut open = ended_copy(&core, 50);
        open.meta.ended_at = None;
        assert!(core.install_ts_ended_partition(&key(), open, None).is_err());
    }

    /// Plaintext files install under the core's segment key: the stored
    /// files are encrypted, and the partition reads back with that key.
    #[test]
    fn an_ended_partition_installs_under_the_core_key() {
        use nodedb_wal::crypto::WalEncryptionKey;

        let source_dir = tempfile::tempdir().expect("tempdir");
        let copy = {
            let (mut core, _tx, _rx) = make_core_with_dir(source_dir.path());
            assert_eq!(
                ingest(&mut core, ROWS_A, 1),
                crate::bridge::envelope::Status::Ok
            );
            flush(&mut core);
            ended_copy(&core, 50)
        };
        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.set_ts_segment_kek(kek.clone());
        let EndedPartitionInstalled::Installed(installed) = core
            .install_ts_ended_partition(&key(), copy.clone(), None)
            .expect("install")
        else {
            panic!("the install files the partition");
        };
        let part_dir = core.ts_dir_of(&key()).join(&installed.dir_name);
        for (name, _) in &copy.files {
            let stored = std::fs::read(part_dir.join(name)).expect("stored file");
            assert!(
                crate::engine::timeseries::columnar_segment::encrypt::is_encrypted(&stored)
                    .expect("sniff"),
                "{name} is stored encrypted"
            );
        }
        let read = crate::data::executor::handlers::timeseries::partition_read::read_ts_partition(
            &part_dir,
            "test",
            Some(&kek),
        )
        .expect("the partition reads with the core key");
        assert_eq!(read.columns.len(), read.schema.columns.len());
        assert_eq!(
            core.install_ts_ended_partition(&key(), copy, None)
                .expect("again"),
            EndedPartitionInstalled::AlreadyPresent(installed),
            "the identity names the plaintext"
        );
    }
}

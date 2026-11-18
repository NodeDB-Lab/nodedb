// SPDX-License-Identifier: BUSL-1.1

//! All-or-nothing commit of a flush that writes several partitions.
//!
//! A flush whose rows span several partition intervals writes one partition
//! per interval. Every partition carries the same replay stamp, which names
//! every record the flush holds. A partition visible without the others would
//! let replay skip records whose rows only the missing partitions hold, so
//! the partitions must appear together.
//!
//! ## Protocol
//!
//! 1. Stage each partition fully under `pending-{flush}-{i}`, stamp and
//!    `partition.meta` included. The boot registry scan lists only `ts-`
//!    names, so a staged partition is invisible.
//! 2. Commit: write `flush.commit`, naming each staged directory and its final
//!    `ts-` name, in one atomic write. A collection holds at most one: no new
//!    flush starts while one is present.
//! 3. Rename every staged directory to its final name, then fsync the
//!    collection directory.
//! 4. Remove the manifest.
//!
//! ## Recovery
//!
//! A transaction that rolls back a flush takes it back through a revert
//! manifest (`flush.revert`, [`revert_flush`]) that names every directory
//! the flush wrote, so its partitions go together too.
//!
//! [`recover_interrupted_flushes`] runs before the registry loads. A revert
//! manifest is finished first. A commit manifest is rolled forward: every rename it names that has not happened happens,
//! and the manifest goes. Every staged directory no manifest names belongs to
//! a flush that never committed, and goes. Its rows are in no partition and no
//! stamp names their records, so replay applies them once.
//!
//! While the core runs, a commit whose install fails is rolled forward by the
//! next flush of the collection and by the maintenance tick
//! (`flush_rollforward`).

use std::path::Path;

use crate::data::executor::handlers::timeseries::truncate::remove_dir_tree;

/// Prefix of a staged, uncommitted partition directory.
pub(in crate::data::executor) const PENDING_PREFIX: &str = "pending-";
/// File name of the commit manifest in a collection directory. It does not
/// start with `ts-`, so the partition scan passes over it.
pub(in crate::data::executor) const MANIFEST_FILE: &str = "flush.commit";
/// File name of the revert manifest of a rolled-back flush. It does not
/// start with `ts-` either.
pub(in crate::data::executor) const REVERT_FILE: &str = "flush.revert";
/// Leading byte of an encoded manifest.
const MANIFEST_FORMAT_VERSION: u8 = 1;

/// One staged partition and the name it is installed under.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct FlushMove {
    pub pending: String,
    pub installed: String,
}

/// The partitions one flush commits together.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct FlushManifest {
    pub moves: Vec<FlushMove>,
}

/// The staging directory name of partition `index` of flush `flush_id`.
pub(in crate::data::executor) fn pending_name(flush_id: &str, index: usize) -> String {
    format!("{PENDING_PREFIX}{flush_id}-{index}")
}

/// Commit `manifest` in `ts_dir`: the one atomic write after which the flush
/// is durable.
pub(in crate::data::executor) fn commit_flush(
    ts_dir: &Path,
    manifest: &FlushManifest,
) -> crate::Result<()> {
    let mut bytes = vec![MANIFEST_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(manifest).map_err(|e| crate::Error::Codec {
        detail: format!("encode flush manifest in {}: {e}", ts_dir.display()),
    })?;
    bytes.extend_from_slice(&body);
    nodedb_wal::segment::atomic_write_fsync(ts_dir, MANIFEST_FILE, &bytes)
        .map_err(|e| storage(&ts_dir.join(MANIFEST_FILE), "write", &e))
}

/// The committed, not yet installed manifest of `ts_dir`, or `None`.
pub(in crate::data::executor) fn read_manifest(
    ts_dir: &Path,
) -> crate::Result<Option<FlushManifest>> {
    let path = ts_dir.join(MANIFEST_FILE);
    let bytes = match nodedb_wal::segment::read_checkpoint_dontneed(&path) {
        Ok(bytes) => bytes,
        Err(nodedb_wal::WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(e) => return Err(storage(&path, "read", &e)),
    };
    decode_manifest(&path, &bytes).map(Some)
}

/// Whether `ts_dir` holds a committed flush that is not installed: one stat.
pub(in crate::data::executor) fn has_uninstalled_flush(ts_dir: &Path) -> crate::Result<bool> {
    ts_dir
        .join(MANIFEST_FILE)
        .try_exists()
        .map_err(|e| storage(&ts_dir.join(MANIFEST_FILE), "probe", &e))
}

/// The directory a manifest entry's partition is in now: its installed name
/// once the rename happened, its staged name before.
pub(in crate::data::executor) fn current_dir(
    ts_dir: &Path,
    movement: &FlushMove,
) -> std::path::PathBuf {
    let installed = ts_dir.join(&movement.installed);
    if installed.exists() {
        installed
    } else {
        ts_dir.join(&movement.pending)
    }
}

/// Install a committed flush: rename every staged partition the manifest
/// names, fsync `ts_dir`, then remove the manifest. Idempotent: a rename that
/// already happened is skipped.
pub(in crate::data::executor) fn install_flush(
    ts_dir: &Path,
    manifest: &FlushManifest,
) -> crate::Result<()> {
    for FlushMove { pending, installed } in &manifest.moves {
        if !nodedb_types::is_plain_path_component(pending)
            || !nodedb_types::is_plain_path_component(installed)
        {
            return Err(storage(
                &ts_dir.join(MANIFEST_FILE),
                "install",
                &"a manifest entry is not one plain path component",
            ));
        }
        let from = ts_dir.join(pending);
        let to = ts_dir.join(installed);
        if to.exists() {
            if from.exists() {
                remove_dir_tree(&from)?;
            }
            continue;
        }
        if !from.exists() {
            return Err(storage(
                &from,
                "install",
                &"the committed flush names a staged partition that is missing",
            ));
        }
        std::fs::rename(&from, &to).map_err(|e| storage(&to, "install", &e))?;
    }
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))?;
    let manifest_path = ts_dir.join(MANIFEST_FILE);
    match std::fs::remove_file(&manifest_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(storage(&manifest_path, "remove", &e)),
    }
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))
}

/// Finish an interrupted revert, roll the committed flush in `ts_dir`
/// forward, then remove every staged partition no committed flush names.
pub(in crate::data::executor) fn recover_interrupted_flushes(ts_dir: &Path) -> crate::Result<()> {
    if let Some(revert) = read_revert(ts_dir)? {
        finish_revert(ts_dir, &revert)?;
    }
    if let Some(manifest) = read_manifest(ts_dir)? {
        install_flush(ts_dir, &manifest)?;
    }
    remove_staged(ts_dir)
}

/// The partitions a rolled-back flush filed, which go together.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct FlushRevert {
    /// Every directory the flush wrote: installed partitions, and the staged
    /// and installed names of each partition its commit manifest names.
    pub dirs: Vec<String>,
    /// Whether the flush's commit manifest goes too.
    pub drop_commit: bool,
}

/// Take back a flush: the partitions it filed and the commit manifest it
/// left.
///
/// Every partition of a flush carries the stamp that names every record the
/// flush holds, so no partition may stay once another went: a survivor would
/// name records whose rows only the removed ones held, and replay would skip
/// them. So the revert manifest, naming every directory, lands first in one
/// atomic write. From there a crash finishes the revert at the next open
/// (`recover_interrupted_flushes`) before any partition loads.
pub(in crate::data::executor) fn revert_flush(
    ts_dir: &Path,
    revert: &FlushRevert,
) -> crate::Result<()> {
    if revert.dirs.is_empty() && !revert.drop_commit {
        return Ok(());
    }
    if let Some(dir) = revert
        .dirs
        .iter()
        .find(|dir| !nodedb_types::is_plain_path_component(dir))
    {
        return Err(storage(
            &ts_dir.join(REVERT_FILE),
            "revert",
            &format!("'{dir}' is not one plain path component"),
        ));
    }
    let mut bytes = vec![MANIFEST_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(revert).map_err(|e| crate::Error::Codec {
        detail: format!("encode flush revert in {}: {e}", ts_dir.display()),
    })?;
    bytes.extend_from_slice(&body);
    nodedb_wal::segment::atomic_write_fsync(ts_dir, REVERT_FILE, &bytes)
        .map_err(|e| storage(&ts_dir.join(REVERT_FILE), "write", &e))?;
    finish_revert(ts_dir, revert)
}

/// Remove what `revert` names, then the revert manifest. Idempotent: a
/// directory already removed is skipped.
fn finish_revert(ts_dir: &Path, revert: &FlushRevert) -> crate::Result<()> {
    if revert.drop_commit {
        remove_file(&ts_dir.join(MANIFEST_FILE))?;
    }
    for (index, dir) in revert.dirs.iter().enumerate() {
        if !nodedb_types::is_plain_path_component(dir) {
            return Err(storage(
                &ts_dir.join(REVERT_FILE),
                "revert",
                &"an entry is not one plain path component",
            ));
        }
        remove_dir_tree(&ts_dir.join(dir))?;
        if index + 1 < revert.dirs.len() {
            crate::fail_point_err!(
                "timeseries::flush_revert::between_removals",
                |detail: String| storage(&ts_dir.join(dir), "revert", &detail)
            );
        }
    }
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))?;
    remove_file(&ts_dir.join(REVERT_FILE))?;
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))
}

/// The revert manifest of `ts_dir`, or `None`.
fn read_revert(ts_dir: &Path) -> crate::Result<Option<FlushRevert>> {
    let path = ts_dir.join(REVERT_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(storage(&path, "read", &e)),
    };
    let Some((&version, body)) = bytes.split_first() else {
        return Err(storage(&path, "decode", &"the file is empty"));
    };
    if version != MANIFEST_FORMAT_VERSION {
        return Err(storage(
            &path,
            "decode",
            &format!("format version {version}, expected {MANIFEST_FORMAT_VERSION}"),
        ));
    }
    zerompk::from_msgpack(body)
        .map(Some)
        .map_err(|e| storage(&path, "decode", &e))
}

/// Remove the file at `path`. An absent file is already removed.
fn remove_file(path: &Path) -> crate::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(storage(path, "remove", &e)),
    }
}

/// Remove every staged partition directory under `ts_dir`. Call only while no
/// manifest is present: every staged directory then belongs to a flush that
/// never committed.
pub(in crate::data::executor) fn remove_staged(ts_dir: &Path) -> crate::Result<()> {
    let entries = match std::fs::read_dir(ts_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(storage(ts_dir, "read", &e)),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.starts_with(PENDING_PREFIX))
        {
            remove_dir_tree(&entry.path())?;
        }
    }
    Ok(())
}

fn decode_manifest(path: &Path, bytes: &[u8]) -> crate::Result<FlushManifest> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(storage(path, "decode", &"the file is empty"));
    };
    if version != MANIFEST_FORMAT_VERSION {
        return Err(storage(
            path,
            "decode",
            &format!("format version {version}, expected {MANIFEST_FORMAT_VERSION}"),
        ));
    }
    zerompk::from_msgpack(body).map_err(|e| storage(path, "decode", &e))
}

fn storage(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("flush commit: {action} {}: {detail}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(dir: &Path, name: &str) {
        std::fs::create_dir_all(dir.join(name)).expect("stage");
        std::fs::write(dir.join(name).join("partition.meta"), b"meta").expect("meta");
    }

    fn manifest() -> FlushManifest {
        FlushManifest {
            moves: vec![
                FlushMove {
                    pending: pending_name("f1", 0),
                    installed: "ts-a".into(),
                },
                FlushMove {
                    pending: pending_name("f1", 1),
                    installed: "ts-b".into(),
                },
            ],
        }
    }

    #[test]
    fn a_flush_staged_but_not_committed_is_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        stage(dir.path(), &pending_name("f1", 0));
        stage(dir.path(), &pending_name("f1", 1));
        recover_interrupted_flushes(dir.path()).expect("recovers");
        assert!(!dir.path().join(pending_name("f1", 0)).exists());
        assert!(!dir.path().join("ts-a").exists());
    }

    #[test]
    fn a_committed_flush_is_installed_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        stage(dir.path(), &pending_name("f1", 0));
        stage(dir.path(), &pending_name("f1", 1));
        commit_flush(dir.path(), &manifest()).expect("commit");
        assert!(has_uninstalled_flush(dir.path()).expect("probe"));
        assert_eq!(read_manifest(dir.path()).expect("read"), Some(manifest()));
        recover_interrupted_flushes(dir.path()).expect("recovers");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(dir.path().join("ts-b").is_dir());
        assert!(!has_uninstalled_flush(dir.path()).expect("probe"));
    }

    #[test]
    fn a_flush_stopped_mid_rename_completes() {
        let dir = tempfile::tempdir().expect("tempdir");
        stage(dir.path(), &pending_name("f1", 0));
        stage(dir.path(), &pending_name("f1", 1));
        commit_flush(dir.path(), &manifest()).expect("commit");
        std::fs::rename(
            dir.path().join(pending_name("f1", 0)),
            dir.path().join("ts-a"),
        )
        .expect("first rename");
        let moves = manifest().moves;
        assert_eq!(current_dir(dir.path(), &moves[0]), dir.path().join("ts-a"));
        assert_eq!(
            current_dir(dir.path(), &moves[1]),
            dir.path().join(pending_name("f1", 1))
        );
        recover_interrupted_flushes(dir.path()).expect("recovers");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(dir.path().join("ts-b").is_dir());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn a_committed_flush_missing_a_staged_partition_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        stage(dir.path(), &pending_name("f1", 0));
        commit_flush(dir.path(), &manifest()).expect("commit");
        assert!(recover_interrupted_flushes(dir.path()).is_err());
    }
}

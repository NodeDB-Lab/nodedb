// SPDX-License-Identifier: BUSL-1.1

//! All-or-nothing commit of a merge of timeseries partitions.
//!
//! A merge replaces several partitions of one interval with one partition
//! holding exactly their rows. Every row must stay visible in exactly one
//! partition through a crash at any step.
//!
//! ## Protocol
//!
//! 1. Stage the merged partition fully under `merge-{name}`: its stamp, the
//!    union of the sources' stamps, first, and `partition.meta` last. The
//!    boot registry scan lists only `ts-` names, so it is invisible.
//! 2. Commit: write `merge.commit`, naming the staged directory, its final
//!    `ts-` name and every source, in one atomic write. A collection holds at
//!    most one.
//! 3. Rename the staged directory to its final name and fsync the collection
//!    directory.
//! 4. Replace the source names with the merged name in every continuous
//!    aggregate record rooted at the collection, and write `cagg.state`.
//! 5. Remove every source directory.
//! 6. Remove the manifest.
//!
//! ## Recovery
//!
//! [`recover_interrupted_merges`] runs before the registry loads. A manifest
//! is rolled forward: the rename happens if it has not, `cagg.state` takes
//! the merged name if it still names the sources, the sources go, and the
//! manifest goes. The sources go before the registry lists them, so no row
//! is listed twice. Every staged directory no manifest names belongs to a
//! merge that never committed, and goes. Its sources hold its rows.
//!
//! The merged partition's stamp names every record its sources' stamps
//! name, so the collection stamp, the union of every partition's, names
//! the same records before and after a merge.

use std::path::Path;

use crate::data::executor::handlers::timeseries::truncate::remove_dir_tree;
use crate::engine::timeseries::continuous_agg::{CAGG_IMAGE_FILE, decode_image, encode_image};

/// Prefix of a staged, uncommitted merged partition directory.
pub(in crate::data::executor) const MERGE_STAGED_PREFIX: &str = "merge-";
/// File name of the merge commit manifest in a collection directory. It does
/// not start with `ts-`, so the partition scan passes over it.
pub(in crate::data::executor) const MERGE_MANIFEST_FILE: &str = "merge.commit";
/// Leading byte of an encoded manifest.
const MANIFEST_FORMAT_VERSION: u8 = 1;
/// Prefix of a committed partition directory.
const PARTITION_PREFIX: &str = "ts-";

/// One committed merge.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct MergeManifest {
    /// The staged directory of the merged partition.
    pub staged: String,
    /// The name the merged partition is installed under.
    pub installed: String,
    /// The partitions the merged partition replaces.
    pub sources: Vec<String>,
}

impl MergeManifest {
    /// Refuse a manifest whose names are not the shapes a merge writes: each
    /// is joined onto the collection directory and renamed or removed.
    fn check(&self, path: &Path) -> crate::Result<()> {
        let plain = |name: &str, prefix: &str| {
            nodedb_types::is_plain_path_component(name) && name.starts_with(prefix)
        };
        if plain(&self.staged, MERGE_STAGED_PREFIX)
            && plain(&self.installed, PARTITION_PREFIX)
            && !self.sources.is_empty()
            && self.sources.iter().all(|s| plain(s, PARTITION_PREFIX))
            && !self.sources.contains(&self.installed)
        {
            return Ok(());
        }
        Err(storage(
            path,
            "check",
            &"the manifest names a directory a merge does not write",
        ))
    }
}

/// The staging directory name of a merge installed as `installed`.
pub(in crate::data::executor) fn staged_name(installed: &str) -> String {
    format!("{MERGE_STAGED_PREFIX}{installed}")
}

/// A `ts-` name for the merged partition of rows `min_ts..=max_ts` that no
/// entry under `ts_dir` holds. The `-m` marker keeps it apart from every
/// name a flush gives.
pub(in crate::data::executor) fn merged_partition_name(
    ts_dir: &Path,
    min_ts: i64,
    max_ts: i64,
    wal_lsn: u64,
) -> crate::Result<String> {
    let base = format!("{PARTITION_PREFIX}{min_ts}_{max_ts}-{wal_lsn:016x}-m");
    for suffix in 0u32..=u32::MAX {
        let candidate = format!("{base}{suffix}");
        let taken_live = ts_dir
            .join(&candidate)
            .try_exists()
            .map_err(|e| storage(&ts_dir.join(&candidate), "probe", &e))?;
        let staged = staged_name(&candidate);
        let taken_staged = ts_dir
            .join(&staged)
            .try_exists()
            .map_err(|e| storage(&ts_dir.join(&staged), "probe", &e))?;
        if !taken_live && !taken_staged {
            return Ok(candidate);
        }
    }
    Err(storage(
        ts_dir,
        "name",
        &format!("no free merged partition name remains for {base}"),
    ))
}

/// Commit `manifest` in `ts_dir`: the one atomic write after which the
/// merge is durable.
pub(in crate::data::executor) fn commit_merge(
    ts_dir: &Path,
    manifest: &MergeManifest,
) -> crate::Result<()> {
    manifest.check(&ts_dir.join(MERGE_MANIFEST_FILE))?;
    let mut bytes = vec![MANIFEST_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(manifest).map_err(|e| crate::Error::Codec {
        detail: format!("encode merge manifest in {}: {e}", ts_dir.display()),
    })?;
    bytes.extend_from_slice(&body);
    nodedb_wal::segment::atomic_write_fsync(ts_dir, MERGE_MANIFEST_FILE, &bytes)
        .map_err(|e| storage(&ts_dir.join(MERGE_MANIFEST_FILE), "write", &e))
}

/// The committed, not yet finished merge of `ts_dir`, or `None`.
pub(in crate::data::executor) fn read_merge_manifest(
    ts_dir: &Path,
) -> crate::Result<Option<MergeManifest>> {
    let path = ts_dir.join(MERGE_MANIFEST_FILE);
    let bytes = match nodedb_wal::segment::read_checkpoint_dontneed(&path) {
        Ok(bytes) => bytes,
        Err(nodedb_wal::WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
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
    let manifest: MergeManifest =
        zerompk::from_msgpack(body).map_err(|e| storage(&path, "decode", &e))?;
    manifest.check(&path)?;
    Ok(Some(manifest))
}

/// Rename the staged merged partition to its installed name, then fsync
/// `ts_dir`. Idempotent: an installed partition is left as it is. When
/// neither directory exists, retention removed the merged partition after
/// the commit, and there is nothing to install.
pub(in crate::data::executor) fn install_merged(
    ts_dir: &Path,
    manifest: &MergeManifest,
) -> crate::Result<()> {
    let staged = ts_dir.join(&manifest.staged);
    let installed = ts_dir.join(&manifest.installed);
    if installed.exists() {
        if staged.exists() {
            remove_dir_tree(&staged)?;
        }
        return Ok(());
    }
    if !staged.exists() {
        return Ok(());
    }
    std::fs::rename(&staged, &installed).map_err(|e| storage(&installed, "install", &e))?;
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))
}

/// Replace the sources with the merged name in `ts_dir`'s `cagg.state`.
/// Idempotent: an image that names no source is left as it is.
///
/// An image that does not decode is left as it is: the image load
/// quarantines its aggregates, and their state is kept for repair. An image
/// that names some sources and not others is refused: a merge never commits
/// such a group, so the image is corrupt.
pub(in crate::data::executor) fn rename_in_image_file(
    ts_dir: &Path,
    manifest: &MergeManifest,
) -> crate::Result<()> {
    let path = ts_dir.join(CAGG_IMAGE_FILE);
    let bytes = match nodedb_wal::segment::read_checkpoint_dontneed(&path) {
        Ok(bytes) => bytes,
        Err(nodedb_wal::WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(());
        }
        Err(e) => return Err(storage(&path, "read", &e)),
    };
    let mut image = match decode_image(&bytes) {
        Ok(image) => image,
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "continuous aggregate image does not decode; a merge recovery leaves it for repair"
            );
            return Ok(());
        }
    };
    if !image.rename_merged(&manifest.sources, &manifest.installed)? {
        return Ok(());
    }
    let encoded = encode_image(&image.view())?;
    nodedb_wal::segment::atomic_write_fsync(ts_dir, CAGG_IMAGE_FILE, &encoded)
        .map_err(|e| storage(&path, "write", &e))
}

/// Remove every source directory, then the manifest, fsyncing `ts_dir`
/// after each. Idempotent.
pub(in crate::data::executor) fn finish_merge(
    ts_dir: &Path,
    manifest: &MergeManifest,
) -> crate::Result<()> {
    for source in &manifest.sources {
        remove_dir_tree(&ts_dir.join(source))?;
    }
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))?;
    let path = ts_dir.join(MERGE_MANIFEST_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(storage(&path, "remove", &e)),
    }
    nodedb_wal::segment::fsync_directory(ts_dir).map_err(|e| storage(ts_dir, "fsync", &e))
}

/// Roll the committed merge in `ts_dir` forward, then remove every staged
/// merge no manifest names. Runs before the registry of `ts_dir` loads.
pub(in crate::data::executor) fn recover_interrupted_merges(ts_dir: &Path) -> crate::Result<()> {
    if let Some(manifest) = read_merge_manifest(ts_dir)? {
        install_merged(ts_dir, &manifest)?;
        rename_in_image_file(ts_dir, &manifest)?;
        finish_merge(ts_dir, &manifest)?;
    }
    remove_merge_staged(ts_dir)
}

/// Remove every staged merge directory under `ts_dir`. Call only while no
/// manifest is present: every staged directory then belongs to a merge that
/// never committed.
pub(in crate::data::executor) fn remove_merge_staged(ts_dir: &Path) -> crate::Result<()> {
    let entries = match std::fs::read_dir(ts_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(storage(ts_dir, "read", &e)),
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(MERGE_STAGED_PREFIX))
        {
            remove_dir_tree(&entry.path())?;
        }
    }
    Ok(())
}

fn storage(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("merge commit: {action} {}: {detail}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> MergeManifest {
        MergeManifest {
            staged: staged_name("ts-m"),
            installed: "ts-m".into(),
            sources: vec!["ts-a".into(), "ts-b".into()],
        }
    }

    fn make(dir: &Path, name: &str) {
        std::fs::create_dir_all(dir.join(name)).expect("mkdir");
    }

    #[test]
    fn a_manifest_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        commit_merge(dir.path(), &manifest()).expect("commit");
        assert_eq!(
            read_merge_manifest(dir.path()).expect("read"),
            Some(manifest())
        );
    }

    #[test]
    fn a_manifest_naming_a_foreign_directory_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut bad = manifest();
        bad.sources.push("../escape".into());
        assert!(commit_merge(dir.path(), &bad).is_err());
    }

    /// Without a manifest, a staged merge never committed: it goes and the
    /// sources stay.
    #[test]
    fn an_uncommitted_merge_is_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in ["ts-a", "ts-b", "merge-ts-m"] {
            make(dir.path(), name);
        }
        recover_interrupted_merges(dir.path()).expect("recover");
        assert!(dir.path().join("ts-a").is_dir());
        assert!(dir.path().join("ts-b").is_dir());
        assert!(!dir.path().join("merge-ts-m").exists());
    }

    /// A committed merge at each step rolls forward to the merged partition
    /// alone.
    #[test]
    fn a_committed_merge_rolls_forward_from_every_step() {
        // (staged present, installed present, sources present)
        for (staged, installed, sources) in [
            (true, false, true),
            (false, true, true),
            (false, true, false),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            if staged {
                make(dir.path(), "merge-ts-m");
            }
            if installed {
                make(dir.path(), "ts-m");
            }
            if sources {
                make(dir.path(), "ts-a");
                make(dir.path(), "ts-b");
            }
            commit_merge(dir.path(), &manifest()).expect("commit");
            recover_interrupted_merges(dir.path()).expect("recover");
            assert!(dir.path().join("ts-m").is_dir());
            assert!(!dir.path().join("ts-a").exists());
            assert!(!dir.path().join("ts-b").exists());
            assert!(!dir.path().join("merge-ts-m").exists());
            assert!(!dir.path().join(MERGE_MANIFEST_FILE).exists());
        }
    }

    #[test]
    fn merged_names_skip_taken_ones() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = merged_partition_name(dir.path(), 1, 2, 3).expect("name");
        make(dir.path(), &first);
        let second = merged_partition_name(dir.path(), 1, 2, 3).expect("name");
        assert_ne!(first, second);
        assert!(first.starts_with("ts-") && second.starts_with("ts-"));
    }
}

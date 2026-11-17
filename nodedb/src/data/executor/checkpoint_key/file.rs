// SPDX-License-Identifier: BUSL-1.1

//! Write and read checkpoint files that carry their own [`CheckpointKey`].
//!
//! Every per-collection checkpoint goes through these helpers. The writer
//! names the file from the key and refuses to overwrite a file that holds
//! another key, so a hash collision fails the write. The readers decode the
//! stored key, check it hashes to the file's own stem, and check it against
//! the key the caller expects where there is one.

use std::path::{Path, PathBuf};

use super::key::{CheckpointKey, KEYED_CKPT_EXTENSION};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;

/// Why a keyed checkpoint write failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyedWriteError {
    /// The target name already holds another key: two keys hash to one name.
    #[error("{} already holds key {existing}; refusing to write key {new} over it", path.display())]
    Collision {
        path: PathBuf,
        existing: String,
        new: String,
    },

    /// The framed write itself failed.
    #[error("cannot write {}: {source}", path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: nodedb_wal::WalError,
    },
}

/// The stem of a checkpoint path: its file name up to the first `.`.
fn path_stem(path: &Path) -> &str {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.split('.').next().unwrap_or("")
}

/// Whether `path` names a published keyed checkpoint (`*.ckpt`, not debris).
pub(crate) fn is_keyed_checkpoint_path(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some(KEYED_CKPT_EXTENSION)
}

fn decode_and_check_name(path: &Path, raw: &[u8]) -> Result<CheckpointKey, CheckpointDecodeError> {
    let key =
        CheckpointKey::decode(raw).map_err(|detail| CheckpointDecodeError::KeyUndecodable {
            path: path.to_path_buf(),
            detail,
        })?;
    let stored_stem = key.file_stem();
    let found_stem = path_stem(path);
    if stored_stem != found_stem {
        return Err(CheckpointDecodeError::KeyNameMismatch {
            path: path.to_path_buf(),
            stored: key.to_string(),
            stored_stem,
            found_stem: found_stem.to_string(),
        });
    }
    Ok(key)
}

fn check_expected(
    path: &Path,
    stored: &CheckpointKey,
    expected: &CheckpointKey,
) -> Result<(), CheckpointDecodeError> {
    if stored != expected {
        return Err(CheckpointDecodeError::KeyMismatch {
            path: path.to_path_buf(),
            expected: expected.to_string(),
            stored: stored.to_string(),
        });
    }
    Ok(())
}

/// Write `payload` under `key` into `dir` as `{key.file_stem()}.{extension}`.
/// Returns the path written.
///
/// A file already at that path whose stored key reads back as another key
/// fails the write: a hash collision or a misplaced file. An unreadable one
/// is debris from a write that never finished and is replaced.
pub(crate) fn write_keyed_checkpoint_as(
    dir: &Path,
    key: &CheckpointKey,
    extension: &str,
    payload: &[u8],
) -> Result<PathBuf, KeyedWriteError> {
    let name = format!("{}.{extension}", key.file_stem());
    let path = dir.join(&name);
    // The stored key is compared without the name check: a file at this name
    // that holds any other key is a collision, whether it hashes here or was
    // moved here.
    if path.exists()
        && let Ok(raw) = nodedb_wal::segment::read_checkpoint_key(&path)
        && let Ok(existing) = CheckpointKey::decode(&raw)
        && existing != *key
    {
        return Err(KeyedWriteError::Collision {
            path,
            existing: existing.to_string(),
            new: key.to_string(),
        });
    }
    nodedb_wal::segment::write_checkpoint_keyed(dir, &name, &key.encode(), payload).map_err(
        |source| KeyedWriteError::Write {
            path: path.clone(),
            source,
        },
    )?;
    Ok(path)
}

/// Write `payload` under `key` into `dir` as [`CheckpointKey::file_name`].
pub(crate) fn write_keyed_checkpoint(
    dir: &Path,
    key: &CheckpointKey,
    payload: &[u8],
) -> Result<PathBuf, KeyedWriteError> {
    write_keyed_checkpoint_as(dir, key, KEYED_CKPT_EXTENSION, payload)
}

/// Read a keyed checkpoint whole. Returns its stored key and payload. The key
/// must hash to the file's own stem.
pub(crate) fn read_keyed_checkpoint(
    path: &Path,
) -> Result<(CheckpointKey, Vec<u8>), CheckpointDecodeError> {
    let read = nodedb_wal::segment::read_checkpoint_keyed(path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.to_path_buf(),
            source,
        }
    })?;
    let key = decode_and_check_name(path, &read.key)?;
    Ok((key, read.payload))
}

/// Read a keyed checkpoint whole and require it to hold `expected`.
pub(crate) fn read_keyed_checkpoint_expecting(
    path: &Path,
    expected: &CheckpointKey,
) -> Result<Vec<u8>, CheckpointDecodeError> {
    let (stored, payload) = read_keyed_checkpoint(path)?;
    check_expected(path, &stored, expected)?;
    Ok(payload)
}

/// Read only the stored key of a keyed checkpoint. The key must hash to the
/// file's own stem. The payload is not read.
pub(crate) fn read_checkpoint_key(path: &Path) -> Result<CheckpointKey, CheckpointDecodeError> {
    let raw = nodedb_wal::segment::read_checkpoint_key(path).map_err(|source| {
        CheckpointDecodeError::ReadFile {
            path: path.to_path_buf(),
            source,
        }
    })?;
    decode_and_check_name(path, &raw)
}

#[cfg(test)]
mod tests {
    use super::super::key::CheckpointEngine;
    use super::*;

    fn key(coll: &str) -> CheckpointKey {
        CheckpointKey::new(CheckpointEngine::Crdt, 0, 1, vec![coll.to_string()])
    }

    #[test]
    fn write_then_read_roundtrips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let k = key(&"c".repeat(255));
        let path = write_keyed_checkpoint(dir.path(), &k, b"payload").expect("write");
        assert_eq!(path, dir.path().join(k.file_name()));
        let (stored, payload) = read_keyed_checkpoint(&path).expect("read");
        assert_eq!(stored, k);
        assert_eq!(payload, b"payload");
        assert_eq!(read_checkpoint_key(&path).expect("key"), k);
        assert_eq!(
            read_keyed_checkpoint_expecting(&path, &k).expect("expected"),
            b"payload"
        );
    }

    /// A file moved under another key's name is refused, naming the stored
    /// key and both stems.
    #[test]
    fn renamed_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let written = write_keyed_checkpoint(dir.path(), &key("orders"), b"x").expect("write");
        let moved = dir.path().join(key("users").file_name());
        std::fs::rename(&written, &moved).expect("rename");
        let err = read_keyed_checkpoint(&moved).expect_err("must refuse");
        assert!(
            matches!(err, CheckpointDecodeError::KeyNameMismatch { .. }),
            "{err}"
        );
        let text = err.to_string();
        assert!(
            text.contains("orders") && text.contains(&key("users").file_stem()),
            "{text}"
        );
        assert!(read_checkpoint_key(&moved).is_err());
    }

    #[test]
    fn unexpected_key_is_refused_naming_both_keys() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_keyed_checkpoint(dir.path(), &key("orders"), b"x").expect("write");
        let err = read_keyed_checkpoint_expecting(&path, &key("users")).expect_err("must refuse");
        let text = err.to_string();
        assert!(text.contains("orders") && text.contains("users"), "{text}");
    }

    /// A readable file holding another key at the target name is a collision:
    /// the write fails and the existing file survives.
    #[test]
    fn collision_fails_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = key("users");
        let other = write_keyed_checkpoint(dir.path(), &key("orders"), b"keep").expect("write");
        std::fs::rename(&other, dir.path().join(target.file_name())).expect("rename");
        let err = write_keyed_checkpoint(dir.path(), &target, b"new").expect_err("collision");
        let text = err.to_string();
        assert!(text.contains("orders") && text.contains("users"), "{text}");
        assert_eq!(
            nodedb_wal::segment::read_checkpoint_keyed(&dir.path().join(target.file_name()))
                .expect("read")
                .payload,
            b"keep"
        );
    }

    #[test]
    fn unreadable_debris_is_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let k = key("orders");
        std::fs::write(dir.path().join(k.file_name()), b"garbage").expect("debris");
        let path = write_keyed_checkpoint(dir.path(), &k, b"x").expect("write");
        assert_eq!(read_keyed_checkpoint(&path).expect("read").1, b"x");
    }

    #[test]
    fn custom_extension_shares_the_stem() {
        let dir = tempfile::tempdir().expect("tempdir");
        let k = key("orders");
        let path = write_keyed_checkpoint_as(dir.path(), &k, "docmap", b"m").expect("write");
        assert_eq!(path, dir.path().join(format!("{}.docmap", k.file_stem())));
        assert!(!is_keyed_checkpoint_path(&path));
        assert_eq!(
            read_keyed_checkpoint_expecting(&path, &k).expect("read"),
            b"m"
        );
    }
}

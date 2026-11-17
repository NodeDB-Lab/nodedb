// SPDX-License-Identifier: BUSL-1.1

//! Directory naming for one array's store, and the key file inside it.
//!
//! An array name has no length bound that fits a path component, so the
//! directory is named by a hash: the first 128 bits of SHA-256 over the encoded
//! array id, hex-encoded. The full id is stored in the directory's `KEY` file.
//! Opening, rekeying and purging check it, so a directory that belongs to
//! another array is refused, never used.

use std::path::{Path, PathBuf};

use nodedb_array::types::ArrayId;
use nodedb_types::{DatabaseId, TenantId};

use super::engine::{ArrayEngineError, ArrayEngineResult};

/// File inside an array directory that stores the array id.
pub const ARRAY_KEY_FILE: &str = "KEY";

/// `[database u64 LE][tenant u64 LE][name utf-8]`.
fn encode_key(id: &ArrayId) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + id.name.len());
    out.extend_from_slice(&id.database_id.as_u64().to_le_bytes());
    out.extend_from_slice(&id.tenant_id.as_u64().to_le_bytes());
    out.extend_from_slice(id.name.as_bytes());
    out
}

fn decode_key(bytes: &[u8]) -> Option<ArrayId> {
    if bytes.len() < 16 {
        return None;
    }
    let (db, rest) = bytes.split_at(8);
    let (tid, name) = rest.split_at(8);
    let mut db_buf = [0u8; 8];
    db_buf.copy_from_slice(db);
    let mut tid_buf = [0u8; 8];
    tid_buf.copy_from_slice(tid);
    let name = std::str::from_utf8(name).ok()?;
    Some(ArrayId::in_database(
        TenantId::new(u64::from_le_bytes(tid_buf)),
        DatabaseId::new(u64::from_le_bytes(db_buf)),
        name,
    ))
}

/// The store directory for one array:
/// `{root}/arrays/d{db}/t{tid}/{hash(id)}`, a fixed-length last component.
pub fn array_dir(root: &Path, id: &ArrayId) -> PathBuf {
    root.join("arrays")
        .join(format!("d{}", id.database_id.as_u64()))
        .join(format!("t{}", id.tenant_id.as_u64()))
        .join(nodedb_types::hashed_file_stem(&encode_key(id)))
}

/// The drop tombstone a staged DROP renames `dir` to: `.{name}.drop-pending`
/// beside it. Its `KEY` file moves with it.
pub(super) fn drop_tombstone_dir(dir: &Path) -> PathBuf {
    let name = dir.file_name().unwrap_or_default().to_string_lossy();
    dir.with_file_name(format!(".{name}.drop-pending"))
}

/// The array id stored in `dir`, or `None` when the directory has no `KEY`.
pub(super) fn read_dir_key(dir: &Path) -> ArrayEngineResult<Option<ArrayId>> {
    let path = dir.join(ARRAY_KEY_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        nodedb_wal::segment::read_checkpoint_key(&path).map_err(|e| ArrayEngineError::Io {
            detail: format!("read array key {path:?}: {e}"),
        })?;
    decode_key(&raw)
        .map(Some)
        .ok_or_else(|| ArrayEngineError::Io {
            detail: format!("array key {path:?} does not decode"),
        })
}

/// Fail when `dir` stores an array id other than `id`. A directory with no
/// `KEY` passes.
pub(super) fn check_dir_key(dir: &Path, id: &ArrayId) -> ArrayEngineResult<()> {
    match read_dir_key(dir)? {
        Some(stored) if stored != *id => Err(ArrayEngineError::Io {
            detail: format!("array directory {dir:?} holds array {stored:?}, expected {id:?}"),
        }),
        _ => Ok(()),
    }
}

/// Write `id` as the `KEY` of `dir`, replacing any key already there.
pub(super) fn write_dir_key(dir: &Path, id: &ArrayId) -> ArrayEngineResult<()> {
    std::fs::create_dir_all(dir).map_err(|e| ArrayEngineError::Io {
        detail: format!("mkdir {dir:?}: {e}"),
    })?;
    nodedb_wal::segment::write_checkpoint_keyed(dir, ARRAY_KEY_FILE, &encode_key(id), &[]).map_err(
        |e| ArrayEngineError::Io {
            detail: format!("write array key in {dir:?}: {e}"),
        },
    )
}

/// Bind `dir` to `id` before a store opens in it: refuse a directory that
/// stores another id, and write the key when it has none.
pub(super) fn bind_array_dir(dir: &Path, id: &ArrayId) -> ArrayEngineResult<()> {
    match read_dir_key(dir)? {
        Some(stored) if stored == *id => Ok(()),
        Some(stored) => Err(ArrayEngineError::Io {
            detail: format!("array directory {dir:?} holds array {stored:?}, expected {id:?}"),
        }),
        None => write_dir_key(dir, id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(name: &str) -> ArrayId {
        ArrayId::in_database(TenantId::new(3), DatabaseId::new(9), name)
    }

    #[test]
    fn key_roundtrips() {
        let long = "n".repeat(1000);
        for name in ["a", "", "x/y", long.as_str()] {
            assert_eq!(decode_key(&encode_key(&id(name))), Some(id(name)));
        }
        assert_eq!(decode_key(&[0u8; 15]), None);
    }

    /// The directory and tombstone names stay short whatever the array is
    /// named.
    #[test]
    fn long_names_get_fixed_length_dirs() {
        let dir = array_dir(Path::new("/r"), &id(&"n".repeat(1000)));
        let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        assert_eq!(name.len(), 32);
        let tomb = drop_tombstone_dir(&dir);
        let tomb_name = tomb
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        assert_eq!(tomb_name.len(), 32 + ".".len() + ".drop-pending".len());
    }

    #[test]
    fn distinct_ids_get_distinct_dirs() {
        let root = Path::new("/r");
        assert_ne!(array_dir(root, &id("a")), array_dir(root, &id("b")));
        let other_db = ArrayId::in_database(TenantId::new(3), DatabaseId::new(10), "a");
        assert_ne!(array_dir(root, &id("a")), array_dir(root, &other_db));
    }

    #[test]
    fn bind_writes_then_checks_the_key() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("store");
        bind_array_dir(&dir, &id("a")).expect("bind fresh");
        assert_eq!(read_dir_key(&dir).expect("read"), Some(id("a")));
        bind_array_dir(&dir, &id("a")).expect("rebind same");
        let err = bind_array_dir(&dir, &id("b")).expect_err("another id is refused");
        let text = err.to_string();
        assert!(text.contains("\"a\"") && text.contains("\"b\""), "{text}");
        assert!(check_dir_key(&dir, &id("b")).is_err());
        check_dir_key(&dir, &id("a")).expect("same id passes");
    }

    /// A 1000-byte array name flushes and reopens across a restart.
    #[test]
    fn long_array_name_roundtrips_across_restart() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
        use crate::engine::array::test_support::{put_one_in, schema};
        use crate::types::replay_stamp::ReplayStamp;

        let tmp = tempfile::tempdir().expect("tempdir");
        let long = id(&"n".repeat(1000));
        {
            let mut e =
                ArrayEngine::new(ArrayEngineConfig::new(tmp.path().to_path_buf())).expect("engine");
            e.open_array(long.clone(), schema(), 0xBEEF).expect("open");
            put_one_in(&mut e, &long, 1, 1, 7, 1);
            e.flush(&long, ReplayStamp::through(2)).expect("flush");
        }
        let mut e =
            ArrayEngine::new(ArrayEngineConfig::new(tmp.path().to_path_buf())).expect("engine");
        e.open_array(long.clone(), schema(), 0xBEEF)
            .expect("reopen");
        assert_eq!(e.store(&long).expect("store").manifest().segments.len(), 1);
        assert_eq!(
            read_dir_key(&array_dir(tmp.path(), &long)).expect("key"),
            Some(long)
        );
    }

    /// A staged drop of a long-named array leaves a tombstone holding its key,
    /// and the purge removes it. A tombstone holding another array's key is
    /// refused and kept.
    #[test]
    fn drop_pending_cleanup_checks_the_key() {
        use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
        use crate::engine::array::test_support::schema;

        let tmp = tempfile::tempdir().expect("tempdir");
        let long = id(&"n".repeat(1000));
        let mut e =
            ArrayEngine::new(ArrayEngineConfig::new(tmp.path().to_path_buf())).expect("engine");
        e.open_array(long.clone(), schema(), 0xBEEF).expect("open");
        e.stage_drop_array(&long).expect("stage drop");
        let tombstone = drop_tombstone_dir(&array_dir(tmp.path(), &long));
        assert_eq!(read_dir_key(&tombstone).expect("key"), Some(long.clone()));
        e.purge_drop_array(&long).expect("purge");
        assert!(!tombstone.exists());

        // A tombstone at this array's name that holds another array's key.
        let other = id("other");
        e.open_array(other.clone(), schema(), 0xBEEF)
            .expect("open other");
        e.stage_drop_array(&other).expect("stage other");
        let other_tomb = drop_tombstone_dir(&array_dir(tmp.path(), &other));
        std::fs::rename(&other_tomb, &tombstone).expect("move tombstone");
        assert!(e.purge_drop_array(&long).is_err());
        assert!(tombstone.exists());
    }
}

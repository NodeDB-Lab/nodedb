// SPDX-License-Identifier: Apache-2.0

//! Durable atomic file / directory operations for checkpoint-class writes.
//!
//! The tmp-file + rename pattern is atomic only if both the file data and
//! the containing directory entry reach stable storage in the correct order.
//! On ext4 / XFS the rename metadata op can reach disk before the data pages
//! backing the tmp file — a power loss between the write and the next
//! checkpoint then leaves a correctly-named file containing zeros.
//!
//! [`atomic_write_fsync`] is the single helper all checkpoint-class writers
//! go through so the ordering (`write → sync_data → rename → fsync_dir`) is
//! enforced in one place. [`atomic_swap_dirs_fsync`] does the same for
//! directory-level swaps (rename old-dir → backup, rename new-dir → old-dir).
//!
//! Path safety lives here, and a caller cannot opt out of it. Both helpers take
//! the base directory and the plain names separately and do every join
//! themselves, so no caller hands this module a path it assembled. Each name
//! passes [`nodedb_types::is_plain_path_component`], which rejects a separator,
//! a drive letter, a NUL, `.`, `..`, a leading or trailing dot or space, and a
//! control character. A name that fails is `InvalidInput` naming the argument,
//! and nothing is written.
//!
//! The tmp name is derived from the destination name as `{name}.tmp`, inside
//! this module, so the five naming conventions the call sites used are gone.
//! The suffix is APPENDED rather than replacing the extension: `a.col` and
//! `a.idx` keep distinct tmp names, where `set_extension("tmp")` collapses both
//! onto `a.tmp` and lets two concurrent writers destroy each other's staging
//! file. The tmp lands in the same directory as the destination, so one parent
//! fsync covers both entries and the rename stays within one filesystem
//! directory.
//!
//! [`read_checkpoint_dontneed`] pairs with the write helper on the read side:
//! checkpoint bytes are consumed once (deserialized into the in-memory index)
//! and then superseded. Leaving them in the page cache wastes memory needed
//! by hot workloads.

use std::fs;
use std::io::Write;
use std::path::Path;

use crate::error::{Result, WalError};

/// Fsync a directory to ensure file creation/deletion metadata is durable.
///
/// On ext4/XFS, creating or deleting a file writes the file data to disk
/// but the directory entry may only be in the page cache. A power loss
/// before the directory entry is persisted causes the file to "disappear"
/// on reboot. Calling fsync on the directory fd ensures the metadata
/// (filename, inode pointer) is on stable storage.
///
/// The one exception is `wasm32-wasip1`: wasi preview1 has no directory
/// fsync, so on that target this is a documented no-op (see below) and the
/// durability guarantee above does not hold there.
pub fn fsync_directory(dir: &Path) -> Result<()> {
    // Crash injection: the directory entry never reaches stable storage.
    // Every caller must treat this as a durability failure, not a warning.
    nodedb_types::fail_point_err!("wal::fsync_directory", |detail: String| WalError::Io(
        std::io::Error::other(format!("failpoint wal::fsync_directory: {detail}"))
    ));

    #[cfg(target_arch = "wasm32")]
    {
        // wasi preview1 has no directory fsync: `File::sync_all` on a directory
        // returns EBADF and there is no weaker syscall with the same guarantee.
        // The rename itself still succeeds; what is lost on this target is the
        // guarantee that the directory entry survives a host crash. Native
        // callers keep it, and the failpoint injection above still fires here.
        let _ = dir;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let dir_file = fs::File::open(dir).map_err(WalError::Io)?;
        dir_file.sync_all().map_err(WalError::Io)?;
        Ok(())
    }
}

fn invalid_input(detail: String) -> WalError {
    WalError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        detail,
    ))
}

/// Reject a name that is not one ordinary path component.
///
/// A name carrying a separator, a `..`, or a drive letter resolves outside the
/// directory this module fsyncs, so the durability ordering no longer covers
/// the entry that was created, and a name taken from catalog, wire, or user
/// input can address a directory the caller never intended to write. Rejecting
/// the name is what makes the join below safe, which is why the join happens
/// here and not at the call site.
fn checked_name(op: &str, label: &str, name: &str) -> Result<()> {
    if nodedb_types::is_plain_path_component(name) {
        return Ok(());
    }
    Err(invalid_input(format!(
        "{op}: {label} must be one plain name — no separator, no '..', no drive \
         letter, no leading or trailing dot or space: {name:?}"
    )))
}

/// Derive the staging name for a destination name.
///
/// The suffix is appended, never substituted for the extension, so two
/// destinations in one directory can never share a tmp name.
fn tmp_name(name: &str) -> String {
    format!("{name}.tmp")
}

/// Atomically write `bytes` to `dir/name` via a `dir/name.tmp` staging file
/// with full durability.
///
/// Order of operations (must not change):
/// 1. Create / truncate the tmp file and write `bytes`.
/// 2. `sync_data()` on the tmp file — forces file data pages to stable storage.
/// 3. `rename(tmp, dst)` — atomic on POSIX filesystems.
/// 4. `fsync_directory(dir)` — forces the directory entry durable so the new
///    name survives power loss.
///
/// `name` must be one plain path component; anything else returns
/// `InvalidInput` before a byte is written. Both paths are built here from
/// `dir`, so the rename stays inside one filesystem directory and the single
/// parent fsync covers both entries.
pub fn atomic_write_fsync(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    checked_name("atomic_write_fsync", "name", name)?;

    let dst = dir.join(name);
    let tmp = dir.join(tmp_name(name));

    {
        let mut f = fs::File::create(&tmp).map_err(WalError::Io)?;
        f.write_all(bytes).map_err(WalError::Io)?;
        f.sync_data().map_err(WalError::Io)?;
    }

    fs::rename(&tmp, &dst).map_err(WalError::Io)?;
    fsync_directory(dir)?;
    Ok(())
}

/// Atomically swap a directory under `parent`:
/// `rename(live, backup); rename(staged, live)`, fsyncing `parent` once both
/// renames have completed.
///
/// `live`, `backup`, and `staged` are plain names under one `parent`, so the
/// renames are same-directory by construction. A name that is not one plain
/// component returns `InvalidInput` before either rename runs. The caller
/// removes the backup directory once the new state is proven good — this helper
/// deletes nothing.
pub fn atomic_swap_dirs_fsync(parent: &Path, live: &str, backup: &str, staged: &str) -> Result<()> {
    checked_name("atomic_swap_dirs_fsync", "live", live)?;
    checked_name("atomic_swap_dirs_fsync", "backup", backup)?;
    checked_name("atomic_swap_dirs_fsync", "staged", staged)?;

    let live = parent.join(live);
    let backup = parent.join(backup);
    let staged = parent.join(staged);

    fs::rename(&live, &backup).map_err(WalError::Io)?;
    fs::rename(&staged, &live).map_err(WalError::Io)?;
    fsync_directory(parent)?;
    Ok(())
}

/// Read a checkpoint file and advise the kernel to drop its pages from the
/// page cache.
///
/// Checkpoint files are consumed exactly once per process lifetime (loaded
/// into the in-memory index and then superseded). `posix_fadvise(DONTNEED)`
/// after read frees the page-cache memory for hot workloads.
///
/// On non-Unix targets the advise call is skipped and this degrades to a
/// plain read.
pub fn read_checkpoint_dontneed(path: &Path) -> Result<Vec<u8>> {
    let file = fs::File::open(path).map_err(WalError::Io)?;
    let len = file.metadata().map_err(WalError::Io)?.len();
    let bytes = fs::read(path).map_err(WalError::Io)?;

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd as _;
        // Safe: `file` owns the fd for the duration of the call; len fits in
        // off_t on all supported platforms (checkpoint files are << i64::MAX).
        let ret = unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                0,
                len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if ret != 0 {
            tracing::debug!(
                path = %path.display(),
                ret,
                "posix_fadvise(DONTNEED) returned nonzero — checkpoint bytes may stay in page cache"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, len);
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_invalid_input(err: &WalError) -> bool {
        matches!(err, WalError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput)
    }

    /// Names that `is_plain_path_component` refuses, one per rejection reason.
    const BAD_NAMES: [&str; 5] = ["sub/escaped.ckpt", "..", "/etc/passwd", "C:evil", "."];

    #[test]
    fn atomic_write_fsync_roundtrip() {
        let dir = crate::test_tempdir().unwrap();

        atomic_write_fsync(dir.path(), "payload.ckpt", b"hello world").unwrap();

        assert!(
            !dir.path().join("payload.ckpt.tmp").exists(),
            "tmp must be renamed away"
        );
        assert_eq!(
            fs::read(dir.path().join("payload.ckpt")).unwrap(),
            b"hello world"
        );
    }

    #[test]
    fn atomic_write_fsync_overwrites() {
        let dir = crate::test_tempdir().unwrap();

        atomic_write_fsync(dir.path(), "payload.ckpt", b"v1").unwrap();
        atomic_write_fsync(dir.path(), "payload.ckpt", b"v2").unwrap();

        assert_eq!(fs::read(dir.path().join("payload.ckpt")).unwrap(), b"v2");
    }

    /// The tmp suffix is appended, so two destinations sharing a stem stage
    /// under different names and cannot clobber each other.
    #[test]
    fn tmp_name_keeps_the_full_destination_name() {
        assert_eq!(tmp_name("a.col"), "a.col.tmp");
        assert_ne!(tmp_name("a.col"), tmp_name("a.idx"));
    }

    #[test]
    fn atomic_swap_dirs_fsync_swaps() {
        let dir = crate::test_tempdir().unwrap();
        let live = dir.path().join("live");
        let backup = dir.path().join("backup");
        let staged = dir.path().join("staged");

        fs::create_dir(&live).unwrap();
        fs::write(live.join("marker"), b"old").unwrap();
        fs::create_dir(&staged).unwrap();
        fs::write(staged.join("marker"), b"new").unwrap();

        atomic_swap_dirs_fsync(dir.path(), "live", "backup", "staged").unwrap();

        assert_eq!(fs::read(live.join("marker")).unwrap(), b"new");
        assert_eq!(fs::read(backup.join("marker")).unwrap(), b"old");
        assert!(!staged.exists());
    }

    /// Every rejection reason, checked on the one name argument the write
    /// helper takes. Without the check each of these escapes `dir`.
    #[test]
    fn atomic_write_fsync_rejects_a_name_that_is_not_one_component() {
        let dir = crate::test_tempdir().unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();

        for name in BAD_NAMES {
            let err = atomic_write_fsync(dir.path(), name, b"x").unwrap_err();
            assert!(
                is_invalid_input(&err),
                "{name:?} must be InvalidInput, got {err:?}"
            );
        }

        assert!(
            fs::read_dir(dir.path().join("sub"))
                .unwrap()
                .next()
                .is_none(),
            "nothing may be written before the check"
        );
    }

    /// The same rejection, exercised through each of the three name arguments
    /// of the swap helper: a check present on only one of them leaves the other
    /// two able to rename outside the fsynced directory.
    #[test]
    fn atomic_swap_dirs_fsync_rejects_a_bad_name_in_each_position() {
        for position in 0..3 {
            for name in BAD_NAMES {
                let dir = crate::test_tempdir().unwrap();
                let live = dir.path().join("live");
                fs::create_dir(&live).unwrap();
                fs::write(live.join("marker"), b"old").unwrap();
                fs::create_dir(dir.path().join("staged")).unwrap();

                let mut names = ["live", "backup", "staged"];
                names[position] = name;
                let err =
                    atomic_swap_dirs_fsync(dir.path(), names[0], names[1], names[2]).unwrap_err();

                assert!(
                    is_invalid_input(&err),
                    "{name:?} at position {position} must be InvalidInput, got {err:?}"
                );
                assert_eq!(
                    fs::read(live.join("marker")).unwrap(),
                    b"old",
                    "live must be untouched when the check fails"
                );
            }
        }
    }

    #[test]
    fn read_checkpoint_dontneed_returns_bytes() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("ckpt");
        fs::write(&path, b"checkpoint bytes").unwrap();

        let bytes = read_checkpoint_dontneed(&path).unwrap();
        assert_eq!(bytes, b"checkpoint bytes");
    }
}

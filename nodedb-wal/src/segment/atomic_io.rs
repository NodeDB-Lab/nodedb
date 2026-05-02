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
//! [`read_checkpoint_dontneed`] pairs with the write helper on the read side:
//! checkpoint bytes are consumed once (deserialized into the in-memory index)
//! and then superseded. Leaving them in the page cache wastes memory needed
//! by hot workloads.

use std::fs;
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::error::{Result, WalError};

/// Fsync a directory to ensure file creation/deletion metadata is durable.
///
/// On ext4/XFS, creating or deleting a file writes the file data to disk
/// but the directory entry may only be in the page cache. A power loss
/// before the directory entry is persisted causes the file to "disappear"
/// on reboot. Calling fsync on the directory fd ensures the metadata
/// (filename, inode pointer) is on stable storage.
pub fn fsync_directory(dir: &Path) -> Result<()> {
    let dir_file = fs::File::open(dir).map_err(WalError::Io)?;
    dir_file.sync_all().map_err(WalError::Io)?;
    Ok(())
}

/// Atomically write `bytes` to `dst` via a `tmp` file with full durability.
///
/// Order of operations (must not change):
/// 1. Create / truncate `tmp` and write `bytes`.
/// 2. `sync_data()` on `tmp` — forces file data pages to stable storage.
/// 3. `rename(tmp, dst)` — atomic on POSIX filesystems.
/// 4. `fsync_directory(parent)` — forces the directory entry durable so the
///    new name survives power loss.
///
/// `tmp` and `dst` MUST be in the same directory; otherwise rename is not
/// atomic and the parent fsync won't cover both entries.
pub fn atomic_write_fsync(tmp: &Path, dst: &Path, bytes: &[u8]) -> Result<()> {
    let parent = dst.parent().ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic_write_fsync: dst has no parent directory",
        ))
    })?;

    {
        let mut f = fs::File::create(tmp).map_err(WalError::Io)?;
        f.write_all(bytes).map_err(WalError::Io)?;
        f.sync_data().map_err(WalError::Io)?;
    }

    fs::rename(tmp, dst).map_err(WalError::Io)?;
    fsync_directory(parent)?;
    Ok(())
}

/// Atomically swap a directory: `rename(live, backup); rename(staged, live)`,
/// fsyncing the parent directory once both renames have completed.
///
/// `live`, `backup`, and `staged` MUST share the same parent directory. The
/// caller is responsible for removing the backup directory once the new
/// state is proven good — this helper does not delete anything.
pub fn atomic_swap_dirs_fsync(live: &Path, backup: &Path, staged: &Path) -> Result<()> {
    let parent = live.parent().ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic_swap_dirs_fsync: live has no parent directory",
        ))
    })?;

    fs::rename(live, backup).map_err(WalError::Io)?;
    fs::rename(staged, live).map_err(WalError::Io)?;
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
        let _ = len;
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_fsync_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");
        let tmp = dir.path().join("payload.ckpt.tmp");

        atomic_write_fsync(&tmp, &dst, b"hello world").unwrap();
        assert!(!tmp.exists(), "tmp must be renamed away");
        assert_eq!(fs::read(&dst).unwrap(), b"hello world");
    }

    #[test]
    fn atomic_write_fsync_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");
        let tmp = dir.path().join("payload.ckpt.tmp");

        atomic_write_fsync(&tmp, &dst, b"v1").unwrap();
        atomic_write_fsync(&tmp, &dst, b"v2").unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"v2");
    }

    #[test]
    fn atomic_swap_dirs_fsync_swaps() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("live");
        let backup = dir.path().join("backup");
        let staged = dir.path().join("staged");

        fs::create_dir(&live).unwrap();
        fs::write(live.join("marker"), b"old").unwrap();
        fs::create_dir(&staged).unwrap();
        fs::write(staged.join("marker"), b"new").unwrap();

        atomic_swap_dirs_fsync(&live, &backup, &staged).unwrap();

        assert_eq!(fs::read(live.join("marker")).unwrap(), b"new");
        assert_eq!(fs::read(backup.join("marker")).unwrap(), b"old");
        assert!(!staged.exists());
    }

    #[test]
    fn read_checkpoint_dontneed_returns_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt");
        fs::write(&path, b"checkpoint bytes").unwrap();

        let bytes = read_checkpoint_dontneed(&path).unwrap();
        assert_eq!(bytes, b"checkpoint bytes");
    }
}

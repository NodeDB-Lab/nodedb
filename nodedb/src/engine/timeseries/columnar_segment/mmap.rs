//! mmap wrapper for columnar column files with access-pattern advice.
//!
//! Columnar scans are forward sequential reads of compressed column files.
//! Without MADV_SEQUENTIAL the kernel underreads and retains consumed pages;
//! without POSIX_FADV_DONTNEED after a scan, cold partitions pin page cache
//! away from hotter engines.
//!
//! For plaintext column files the backing storage is a `memmap2::Mmap`
//! (zero-copy). For encrypted files the backing storage is an owned
//! `Vec<u8>` of the decrypted plaintext — mmap zero-copy is not available
//! for encrypted on-disk blobs, which is acceptable given the one-time open
//! cost at partition open time.

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Module-scoped counters for observing mmap advice + fadvise behaviour.
pub mod test_hooks {
    use super::{AtomicU64, Ordering};
    pub(super) static MADV_SEQUENTIAL_COUNT: AtomicU64 = AtomicU64::new(0);
    pub(super) static FADV_DONTNEED_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn madv_sequential_count() -> u64 {
        MADV_SEQUENTIAL_COUNT.load(Ordering::Relaxed)
    }
    pub fn fadv_dontneed_count() -> u64 {
        FADV_DONTNEED_COUNT.load(Ordering::Relaxed)
    }
}

/// Backing storage for an open column file.
pub(super) enum BackingStore {
    /// Memory-mapped plaintext file (zero-copy, POSIX_FADV_DONTNEED on drop).
    Mmap {
        mmap: memmap2::Mmap,
        file: std::fs::File,
    },
    /// Heap-allocated decrypted plaintext (encrypted on-disk file).
    Decrypted(Vec<u8>),
}

impl BackingStore {
    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            BackingStore::Mmap { mmap, .. } => mmap,
            BackingStore::Decrypted(v) => v,
        }
    }
}

/// Wrapper around a column-file backing store that advises `MADV_SEQUENTIAL`
/// on construction (plaintext path) and `POSIX_FADV_DONTNEED` on drop
/// (plaintext path). Returned by `ColumnarSegmentReader::mmap_column`.
///
/// For encrypted files the backing store is an owned decrypted buffer; the
/// MADV/fadvise calls are skipped since there is no mmap region to advise.
pub struct ColumnMmap {
    pub(super) backing: BackingStore,
    pub(super) path: PathBuf,
}

impl ColumnMmap {
    pub fn bytes(&self) -> &[u8] {
        self.backing.bytes()
    }

    pub fn len(&self) -> usize {
        self.backing.bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.backing.bytes().is_empty()
    }

    /// Returns `true` if the backing store is a mmap'd plaintext file.
    pub fn is_mmap(&self) -> bool {
        matches!(self.backing, BackingStore::Mmap { .. })
    }

    /// Returns `true` if the backing store is a decrypted owned buffer.
    pub fn is_decrypted_owned(&self) -> bool {
        matches!(self.backing, BackingStore::Decrypted(_))
    }
}

impl std::ops::Deref for ColumnMmap {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.backing.bytes()
    }
}

impl Drop for ColumnMmap {
    fn drop(&mut self) {
        let BackingStore::Mmap { ref mmap, ref file } = self.backing else {
            return;
        };
        let len = mmap.len();
        if len == 0 {
            return;
        }
        #[cfg(not(target_os = "linux"))]
        let _ = file;
        #[cfg(target_os = "linux")]
        {
            let rc = unsafe {
                libc::posix_fadvise(
                    file.as_raw_fd(),
                    0,
                    len as libc::off_t,
                    libc::POSIX_FADV_DONTNEED,
                )
            };
            if rc == 0 {
                test_hooks::FADV_DONTNEED_COUNT.fetch_add(1, Ordering::Relaxed);
            } else {
                tracing::warn!(
                    path = %self.path.display(),
                    errno = rc,
                    "posix_fadvise(DONTNEED) failed on columnar mmap drop",
                );
            }
        }
    }
}

/// Advise `MADV_SEQUENTIAL` on a freshly-mapped column region.
pub(super) fn advise_sequential(mmap: &memmap2::Mmap, col_path: &std::path::Path) {
    if mmap.is_empty() {
        return;
    }
    let rc = unsafe {
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_SEQUENTIAL,
        )
    };
    if rc == 0 {
        test_hooks::MADV_SEQUENTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
    } else {
        tracing::warn!(
            path = %col_path.display(),
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "madvise(MADV_SEQUENTIAL) failed on column mmap",
        );
    }
}

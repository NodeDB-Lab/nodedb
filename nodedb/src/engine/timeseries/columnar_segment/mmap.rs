//! mmap wrapper for columnar column files with access-pattern advice.
//!
//! Columnar scans are forward sequential reads of compressed column files.
//! Without MADV_SEQUENTIAL the kernel underreads and retains consumed pages;
//! without POSIX_FADV_DONTNEED after a scan, cold partitions pin page cache
//! away from hotter engines.

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

/// Wrapper around a column-file `memmap2::Mmap` that keeps the fd alive so
/// the file can be `posix_fadvise`d on drop, and advises `MADV_SEQUENTIAL`
/// on construction. Returned by `ColumnarSegmentReader::mmap_column`.
pub struct ColumnMmap {
    pub(super) mmap: memmap2::Mmap,
    pub(super) file: std::fs::File,
    pub(super) path: PathBuf,
}

impl ColumnMmap {
    pub fn as_mmap(&self) -> &memmap2::Mmap {
        &self.mmap
    }
    pub fn len(&self) -> usize {
        self.mmap.len()
    }
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

impl std::ops::Deref for ColumnMmap {
    type Target = memmap2::Mmap;
    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl Drop for ColumnMmap {
    fn drop(&mut self) {
        let len = self.mmap.len();
        if len == 0 {
            return;
        }
        let rc = unsafe {
            libc::posix_fadvise(
                self.file.as_raw_fd(),
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

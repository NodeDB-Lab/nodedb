//! File-descriptor-based prefetch via `posix_fadvise`.
//!
//! Unlike `madvise` (which works on mmap'd memory), `posix_fadvise` works
//! on file descriptors — making it suitable for the columnar segment read
//! path which uses `std::fs::read()`.
//!
//! ## Usage
//!
//! Before reading a set of `.col` files during a partition scan, call
//! `advise_willneed_files()` on all needed column paths. The kernel
//! initiates readahead I/O in the background. By the time the sequential
//! `std::fs::read()` calls execute, data is likely in the page cache.
//!
//! After aggregation completes and partition data is no longer needed,
//! call `advise_dontneed_files()` to hint the kernel that those pages
//! can be evicted — freeing page cache for other engines.

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// Advise the kernel to prefetch an entire file into page cache.
///
/// Uses `POSIX_FADV_WILLNEED` — the kernel initiates asynchronous
/// readahead and returns immediately. Non-blocking.
///
/// Returns `Ok(file_len)` on success so callers can track bytes advised.
pub fn advise_willneed(path: &Path) -> std::io::Result<u64> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    {
        let ret = unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, len as i64, libc::POSIX_FADV_WILLNEED)
        };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
    }
    Ok(len)
}

/// Advise the kernel that a file's pages are no longer needed.
///
/// Uses `POSIX_FADV_DONTNEED` — hints the kernel to evict pages from
/// the page cache. Useful after aggregation to free cache for other
/// engines sharing the same process.
pub fn advise_dontneed(path: &Path) -> std::io::Result<()> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        let ret = unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, len as i64, libc::POSIX_FADV_DONTNEED)
        };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
    }
    Ok(())
}

/// Prefetch multiple column files in a partition directory.
///
/// Issues `POSIX_FADV_WILLNEED` on each `.col` file for columns in
/// `needed_columns`. The kernel batches readahead I/O internally.
///
/// Call this BEFORE the sequential `std::fs::read()` calls in the
/// partition scan loop. By the time reads execute, pages are warm.
pub fn prefetch_partition_columns(partition_dir: &Path, needed_columns: &[String]) {
    for col_name in needed_columns {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        // Best-effort: ignore errors (file may not exist for projected-away columns).
        let _ = advise_willneed(&col_path);
    }
    // Also prefetch metadata files (small, frequently accessed).
    let _ = advise_willneed(&partition_dir.join("schema.json"));
    let _ = advise_willneed(&partition_dir.join("partition.meta"));
}

/// Release page cache for column files after aggregation completes.
///
/// Issues `POSIX_FADV_DONTNEED` on each `.col` file. Frees page cache
/// for other engines (vector, graph, document) sharing the process.
pub fn release_partition_columns(partition_dir: &Path, needed_columns: &[String]) {
    for col_name in needed_columns {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let _ = advise_dontneed(&col_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn advise_willneed_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.col");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&[0u8; 8192]).unwrap();
        }
        let len = advise_willneed(&path).unwrap();
        assert_eq!(len, 8192);
    }

    #[test]
    fn advise_willneed_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.col");
        assert!(advise_willneed(&path).is_err());
    }

    #[test]
    fn advise_dontneed_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.col");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&[0u8; 4096]).unwrap();
        }
        advise_dontneed(&path).unwrap();
    }

    #[test]
    fn prefetch_partition_columns_best_effort() {
        let dir = tempfile::tempdir().unwrap();
        // Create some column files.
        for name in &["timestamp", "value", "qtype"] {
            let path = dir.path().join(format!("{name}.col"));
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&[0u8; 4096]).unwrap();
        }
        // schema.json and partition.meta also prefetched.
        std::fs::write(dir.path().join("schema.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("partition.meta"), b"{}").unwrap();

        let cols = vec![
            "timestamp".to_string(),
            "value".to_string(),
            "qtype".to_string(),
            "missing_col".to_string(), // should not panic
        ];
        prefetch_partition_columns(dir.path(), &cols);
        release_partition_columns(dir.path(), &cols);
    }

    #[test]
    fn empty_file_no_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.col");
        std::fs::File::create(&path).unwrap();
        let len = advise_willneed(&path).unwrap();
        assert_eq!(len, 0);
        advise_dontneed(&path).unwrap();
    }
}

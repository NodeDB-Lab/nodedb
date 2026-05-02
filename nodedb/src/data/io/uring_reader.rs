//! Batched io_uring reader for columnar segment files.
//!
//! Submits multiple file reads as io_uring SQEs in a single batch, then
//! waits for all completions. The kernel processes reads in parallel
//! internally, giving ~2-4x throughput vs sequential `std::fs::read()`.
//!
//! ## Design
//!
//! - One `UringReader` per TPC core (stored in `CoreLoop`, `!Send`)
//! - Reusable aligned buffer pool (pre-allocated, avoids per-read allocation)
//! - `read_files()` opens files, submits reads, waits for all completions
//! - Falls back gracefully if io_uring setup fails (old kernels)
//!
//! ## Integration
//!
//! ```text
//! aggregate_partition(dir, ..., uring_reader)
//!   → uring_reader.read_files([timestamp.col, value.col, qtype.col])
//!   → submit 3 IORING_OP_READ SQEs
//!   → submit_and_wait(3)
//!   → kernel reads 3 files in parallel from NVMe
//!   → return [Vec<u8>, Vec<u8>, Vec<u8>]
//! ```

use std::path::Path;
use std::time::Instant;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
use super::aligned_buf::{ALIGNMENT, AlignedBuf};
use super::io_metrics::{IoMetrics, TIER_CRITICAL, TIER_HIGH, TIER_LOW};
use crate::bridge::envelope::Priority;

/// Queue depth for the io_uring instance.
#[cfg(target_os = "linux")]
const QUEUE_DEPTH: u32 = 64;

/// Maximum number of pre-allocated buffers in the pool.
#[cfg(target_os = "linux")]
const POOL_SIZE: usize = 32;

/// Default buffer size (4 MiB — fits most column files).
#[cfg(target_os = "linux")]
const DEFAULT_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Per-core batched io_uring reader.
///
/// Not `Send` — owned by a single Data Plane core.
/// On non-Linux platforms this is a zero-size stub; `new()` always returns
/// `None` and `read_files()` falls back to `std::fs::read`.
pub struct UringReader {
    #[cfg(target_os = "linux")]
    ring: io_uring::IoUring,
    #[cfg(target_os = "linux")]
    /// Pre-allocated aligned buffer pool.
    pool: Vec<AlignedBuf>,
    #[cfg(target_os = "linux")]
    /// Indices of available buffers in the pool.
    free: Vec<usize>,
    #[cfg(target_os = "linux")]
    /// Buffer size for each pool slot.
    buf_size: usize,
}

impl UringReader {
    /// Create a new io_uring reader with a pre-allocated buffer pool.
    ///
    /// Returns `None` if io_uring is not available (old kernel, non-Linux).
    pub fn new() -> Option<Self> {
        #[cfg(not(target_os = "linux"))]
        return None;
        #[cfg(target_os = "linux")]
        Self::with_config(QUEUE_DEPTH, POOL_SIZE, DEFAULT_BUF_SIZE)
    }

    /// Create with custom configuration.
    ///
    /// Always returns `None` on non-Linux platforms.
    pub fn with_config(queue_depth: u32, pool_size: usize, buf_size: usize) -> Option<Self> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (queue_depth, pool_size, buf_size);
            return None;
        }
        #[cfg(target_os = "linux")]
        {
            let ring = io_uring::IoUring::new(queue_depth).ok()?;

            let mut pool = Vec::with_capacity(pool_size);
            let mut free = Vec::with_capacity(pool_size);
            for i in 0..pool_size {
                match AlignedBuf::new(buf_size) {
                    Ok(buf) => {
                        pool.push(buf);
                        free.push(i);
                    }
                    Err(_) => break,
                }
            }

            if pool.is_empty() {
                return None;
            }

            Some(Self {
                ring,
                pool,
                free,
                buf_size,
            })
        }
    }

    /// Read multiple files in a single batched submission (io_uring on Linux,
    /// sequential `std::fs::read` on other platforms).
    ///
    /// Files that fail to open are returned as empty `Vec<u8>`.
    pub fn read_files(&mut self, paths: &[&Path]) -> Vec<Vec<u8>> {
        #[cfg(not(target_os = "linux"))]
        {
            return paths
                .iter()
                .map(|p| std::fs::read(p).unwrap_or_default())
                .collect();
        }

        #[cfg(target_os = "linux")]
        {
        if paths.is_empty() {
            return Vec::new();
        }

        // Phase 1: Open files, determine sizes, assign buffers.
        let mut reads: Vec<PendingRead> = Vec::with_capacity(paths.len());
        let mut oversized: Vec<AlignedBuf> = Vec::new();

        for (i, path) in paths.iter().enumerate() {
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(_) => {
                    reads.push(PendingRead::failed(i));
                    continue;
                }
            };
            let size = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            if size == 0 {
                reads.push(PendingRead::failed(i));
                continue;
            }

            let buf_source = if size <= self.buf_size {
                if let Some(slot) = self.free.pop() {
                    BufSource::Pool(slot)
                } else {
                    // Pool exhausted — allocate dedicated.
                    match AlignedBuf::new(size) {
                        Ok(buf) => {
                            let idx = oversized.len();
                            oversized.push(buf);
                            BufSource::Oversized(idx)
                        }
                        Err(_) => {
                            reads.push(PendingRead::failed(i));
                            continue;
                        }
                    }
                }
            } else {
                match AlignedBuf::new(size) {
                    Ok(buf) => {
                        let idx = oversized.len();
                        oversized.push(buf);
                        BufSource::Oversized(idx)
                    }
                    Err(_) => {
                        reads.push(PendingRead::failed(i));
                        continue;
                    }
                }
            };

            reads.push(PendingRead {
                index: i,
                file: Some(file),
                size,
                buf_source,
            });
        }

        // Phase 2: Submit all SQEs.
        let mut submitted = 0u32;
        for read in &reads {
            let Some(ref file) = read.file else {
                continue;
            };

            let (buf_ptr, buf_cap) = match read.buf_source {
                BufSource::Pool(slot) => (self.pool[slot].as_mut_ptr(), self.pool[slot].capacity()),
                BufSource::Oversized(idx) => {
                    (oversized[idx].as_mut_ptr(), oversized[idx].capacity())
                }
                BufSource::None => continue,
            };

            let read_len = round_up_read(read.size).min(buf_cap) as u32;
            let fd = io_uring::types::Fd(file.as_raw_fd());
            let read_op = io_uring::opcode::Read::new(fd, buf_ptr, read_len)
                .offset(0)
                .build()
                .user_data(read.index as u64);

            // SAFETY: buf_ptr points to a buffer that outlives the SQE.
            // Pool buffers live in self.pool. Oversized buffers live in `oversized`
            // Vec. File fds valid until reads Vec is dropped (end of function).
            unsafe {
                if self.ring.submission().push(&read_op).is_ok() {
                    submitted += 1;
                }
            }
        }

        // Phase 3: Wait for all completions.
        let mut failed_indices = Vec::new();
        if submitted > 0 {
            let _ = self.ring.submit_and_wait(submitted as usize);

            let mut drained = 0u32;
            while drained < submitted {
                if let Some(cqe) = self.ring.completion().next() {
                    if cqe.result() < 0 {
                        failed_indices.push(cqe.user_data() as usize);
                    }
                    drained += 1;
                } else {
                    break;
                }
            }
        }

        // Phase 4: Extract results and return pool buffers.
        let mut results = vec![Vec::new(); paths.len()];
        for read in &reads {
            if read.file.is_none() || read.size == 0 {
                continue;
            }
            if failed_indices.contains(&read.index) {
                // Return pool buffer on failure.
                if let BufSource::Pool(slot) = read.buf_source {
                    self.free.push(slot);
                }
                continue;
            }

            let data = match read.buf_source {
                BufSource::Pool(slot) => {
                    // SAFETY: io_uring wrote read.size bytes into pool[slot].
                    let v = unsafe { self.pool[slot].to_vec(read.size) };
                    self.free.push(slot);
                    v
                }
                BufSource::Oversized(idx) => {
                    // SAFETY: io_uring wrote read.size bytes into oversized[idx].
                    unsafe { oversized[idx].to_vec(read.size) }
                }
                BufSource::None => Vec::new(),
            };

            results[read.index] = data;
        }

        results
        } // end #[cfg(target_os = "linux")]
    }

    /// Priority-aware variant of [`read_files`].
    ///
    /// Identical to `read_files` but records IO wait time in `metrics`
    /// under the bucket corresponding to `priority`:
    ///
    /// - `Background` / `Normal` → `TIER_LOW`
    /// - `High`                  → `TIER_HIGH`
    /// - `Critical`              → `TIER_CRITICAL`
    ///
    /// The wait clock starts just before the first SQE submission and stops
    /// after all CQEs are reaped.  File open + buffer setup time is included
    /// because it reflects the total latency seen by the caller.
    pub fn read_files_with_priority(
        &mut self,
        paths: &[&Path],
        priority: Priority,
        metrics: &IoMetrics,
    ) -> Vec<Vec<u8>> {
        let tier = match priority {
            Priority::Background | Priority::Normal => TIER_LOW,
            Priority::High => TIER_HIGH,
            Priority::Critical => TIER_CRITICAL,
        };
        let t0 = Instant::now();
        let result = self.read_files(paths);
        let wait_ns = t0.elapsed().as_nanos() as u64;
        metrics.record_wait(tier, wait_ns);
        result
    }
}

/// Tracks which buffer a read uses.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum BufSource {
    Pool(usize),
    Oversized(usize),
    None,
}

/// A pending read operation.
#[cfg(target_os = "linux")]
struct PendingRead {
    index: usize,
    file: Option<std::fs::File>,
    size: usize,
    buf_source: BufSource,
}

#[cfg(target_os = "linux")]
impl PendingRead {
    fn failed(index: usize) -> Self {
        Self {
            index,
            file: None,
            size: 0,
            buf_source: BufSource::None,
        }
    }
}

/// Round a read size up to ALIGNMENT.
#[cfg(target_os = "linux")]
#[inline]
fn round_up_read(size: usize) -> usize {
    super::aligned_buf::round_up(size, ALIGNMENT)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_file(dir: &Path, name: &str, size: usize) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        f.write_all(&data).unwrap();
        f.sync_all().unwrap();
        path
    }

    #[test]
    fn read_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_file(dir.path(), "test.col", 8192);

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[path.as_path()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 8192);
        for (i, &b) in results[0].iter().enumerate() {
            assert_eq!(b, (i % 256) as u8, "mismatch at byte {i}");
        }
    }

    #[test]
    fn read_multiple_files_batched() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = create_test_file(dir.path(), "a.col", 4096);
        let p2 = create_test_file(dir.path(), "b.col", 8192);
        let p3 = create_test_file(dir.path(), "c.col", 1024);

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[p1.as_path(), p2.as_path(), p3.as_path()]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].len(), 4096);
        assert_eq!(results[1].len(), 8192);
        assert_eq!(results[2].len(), 1024);
    }

    #[test]
    fn read_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let existing = create_test_file(dir.path(), "exists.col", 4096);
        let missing = dir.path().join("missing.col");

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[existing.as_path(), missing.as_path()]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].len(), 4096);
        assert_eq!(results[1].len(), 0);
    }

    #[test]
    fn read_oversized_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_file(dir.path(), "big.col", 16384);

        let mut reader = UringReader::with_config(8, 4, 4096).unwrap();
        let results = reader.read_files(&[path.as_path()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 16384);
    }

    #[test]
    fn read_empty_paths() {
        let mut reader = UringReader::with_config(8, 4, 4096).unwrap();
        let results = reader.read_files(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn pool_buffers_are_reused() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = create_test_file(dir.path(), "a.col", 1024);
        let p2 = create_test_file(dir.path(), "b.col", 2048);

        let mut reader = UringReader::with_config(8, 2, 4096).unwrap();

        let r1 = reader.read_files(&[p1.as_path(), p2.as_path()]);
        assert_eq!(r1[0].len(), 1024);
        assert_eq!(r1[1].len(), 2048);
        assert_eq!(reader.free.len(), 2);

        let r2 = reader.read_files(&[p1.as_path()]);
        assert_eq!(r2[0].len(), 1024);
        assert_eq!(reader.free.len(), 2);
    }
}

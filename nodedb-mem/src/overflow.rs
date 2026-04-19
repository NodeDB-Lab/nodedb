//! mmap-backed overflow region for spilled arena allocations.
//!
//! Each Data Plane core gets its own overflow file. The owning core writes
//! spilled data; other cores may open read-only mmaps for cross-core access.
//! This preserves zero-lock TPC isolation.
//!
//! The region uses a bump allocator within the mmap'd file. When the file fills,
//! it grows via `ftruncate` + `mremap`.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::EngineId;
use crate::error::{MemError, Result};

/// Module-scoped counters for observing madvise behaviour.
pub mod test_hooks {
    use super::{AtomicU64, Ordering};
    pub(super) static MADV_RANDOM_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn madv_random_count() -> u64 {
        MADV_RANDOM_COUNT.load(Ordering::Relaxed)
    }
}

/// Advise MADV_RANDOM on a freshly-mapped (or freshly-remapped) overflow
/// region. Returns the hint on success, `None` on failure or zero-length.
fn advise_random(base: *mut u8, len: usize, path: &Path) -> Option<libc::c_int> {
    if len == 0 {
        return None;
    }
    let rc = unsafe { libc::madvise(base as *mut libc::c_void, len, libc::MADV_RANDOM) };
    if rc == 0 {
        test_hooks::MADV_RANDOM_COUNT.fetch_add(1, Ordering::Relaxed);
        Some(libc::MADV_RANDOM)
    } else {
        tracing::warn!(
            path = %path.display(),
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "madvise(MADV_RANDOM) failed on overflow region; continuing with kernel default",
        );
        None
    }
}

/// Metadata for one spilled allocation in the overflow region.
#[derive(Debug, Clone)]
pub struct OverflowSlot {
    /// Offset within the mmap region.
    pub offset: usize,
    /// Size of the spilled data.
    pub size: usize,
    /// Engine that owned this allocation.
    pub engine: EngineId,
    /// Whether this slot is occupied (false = freed/compacted).
    pub occupied: bool,
}

/// mmap-backed overflow region owned by a single core.
///
/// Uses a bump allocator within the mmap'd file with a free-list for
/// slot reuse. When a slot is freed, it's added to the free-list so
/// future writes of equal or smaller size can reclaim the space without
/// advancing the bump cursor.
///
/// Not `Send` or `Sync` — it's single-thread owned.
pub struct OverflowRegion {
    /// Path to the overflow file (used for debugging/diagnostics).
    path: PathBuf,
    /// File descriptor wrapped in Arc for drop safety.
    _fd: Arc<std::fs::File>,
    /// mmap'd region. null if not yet mapped.
    base: *mut u8,
    /// Current capacity of the mmap in bytes.
    capacity: usize,
    /// Bump pointer: next write starts here.
    cursor: usize,
    /// Slot metadata.
    slots: Vec<OverflowSlot>,
    /// Free-list: indices of freed slots, sorted largest-first for best-fit.
    free_list: Vec<usize>,
    /// Maximum capacity (prevents unbounded growth).
    max_capacity: usize,
    /// Last madvise hint applied to `base` (for observability).
    madvise_state: Option<libc::c_int>,
}

impl OverflowRegion {
    /// Default initial mmap size.
    ///
    /// Corresponds to `MemoryTuning::overflow_initial_bytes`.
    pub const DEFAULT_INITIAL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MiB

    /// Default maximum capacity (prevents unbounded growth).
    ///
    /// Corresponds to `MemoryTuning::overflow_max_bytes`.
    pub const DEFAULT_MAX_CAPACITY: usize = 1024 * 1024 * 1024; // 1 GiB

    /// Open or create an overflow region at the given path.
    ///
    /// If the file doesn't exist, it's created with the initial capacity.
    /// If the file exists, it's mapped at current size.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_capacity(path, Self::DEFAULT_INITIAL_CAPACITY)
    }

    /// Open or create an overflow region with explicit initial and maximum capacity.
    ///
    /// Use this when applying runtime config from `MemoryTuning`.
    pub fn open_with_config(
        path: &Path,
        initial_capacity: usize,
        max_capacity: usize,
    ) -> Result<Self> {
        let mut region = Self::open_with_capacity(path, initial_capacity)?;
        region.max_capacity = max_capacity;
        Ok(region)
    }

    /// Open or create an overflow region with a specific initial capacity.
    pub fn open_with_capacity(path: &Path, initial_capacity: usize) -> Result<Self> {
        let fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| MemError::Overflow(format!("failed to open overflow file: {e}")))?;

        let current_size = fd
            .metadata()
            .map_err(|e| MemError::Overflow(format!("failed to get file metadata: {e}")))?
            .len() as usize;

        let capacity = if current_size == 0 {
            // New file — truncate to initial capacity.
            fd.set_len(initial_capacity as u64)
                .map_err(|e| MemError::Overflow(format!("failed to truncate file: {e}")))?;
            initial_capacity
        } else {
            current_size
        };

        // SAFETY: We pass null to let the kernel choose the mapping address.
        // `capacity` is non-zero (either from file metadata or `initial_capacity`).
        // `fd` is a valid, open file descriptor with read/write permissions.
        // MAP_SHARED is correct for file-backed overflow that may be read by other
        // cores via separate read-only mappings. We check for MAP_FAILED below.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };

        if base == libc::MAP_FAILED {
            return Err(MemError::Overflow(
                "failed to mmap overflow region".to_string(),
            ));
        }

        // Overflow spill is written in per-slot bursts and read from
        // potentially any core. Access is scattered, so MADV_NORMAL readahead
        // wastes page cache on adjacent slots unlikely to be read together.
        let madvise_state = advise_random(base as *mut u8, capacity, path);

        Ok(Self {
            path: path.to_path_buf(),
            _fd: Arc::new(fd),
            base: base as *mut u8,
            capacity,
            cursor: 0,
            slots: Vec::new(),
            free_list: Vec::new(),
            max_capacity: Self::DEFAULT_MAX_CAPACITY,
            madvise_state,
        })
    }

    /// The madvise hint currently in effect on the mapped region.
    pub fn madvise_state(&self) -> Option<libc::c_int> {
        self.madvise_state
    }

    /// Write data to the overflow region and return the slot index.
    ///
    /// First attempts to reuse a freed slot from the free-list (best-fit).
    /// Falls back to bump allocation if no suitable free slot exists.
    pub fn write(&mut self, data: &[u8], engine: EngineId) -> Result<usize> {
        // Try to reuse a freed slot that fits this data.
        if let Some(reused) = self.try_reuse_slot(data, engine) {
            return Ok(reused);
        }

        // Bump allocation path.
        let required = self.cursor + data.len();

        // Check if we need to grow.
        if required > self.capacity {
            self.grow(required)?;
        }

        // SAFETY: `self.base` is non-null (checked at construction, and after every
        // mremap in `grow`). `self.cursor + data.len() <= self.capacity` is guaranteed
        // because we called `grow(required)` above when `required > capacity`.
        // The source (`data`) and destination (`base + cursor`) cannot overlap because
        // `data` is a caller-owned slice and `base` is an mmap'd region.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.base.add(self.cursor), data.len());
        }

        // Record slot.
        let slot_index = self.slots.len();
        self.slots.push(OverflowSlot {
            offset: self.cursor,
            size: data.len(),
            engine,
            occupied: true,
        });

        self.cursor += data.len();

        Ok(slot_index)
    }

    /// Try to reuse a freed slot from the free-list.
    ///
    /// Uses best-fit: finds the smallest free slot that can hold `data`.
    /// This minimizes internal fragmentation.
    fn try_reuse_slot(&mut self, data: &[u8], engine: EngineId) -> Option<usize> {
        if self.free_list.is_empty() {
            return None;
        }

        // Find best-fit: smallest free slot >= data.len().
        let mut best_idx = None;
        let mut best_waste = usize::MAX;

        for (fl_idx, &slot_idx) in self.free_list.iter().enumerate() {
            let slot_size = self.slots[slot_idx].size;
            if slot_size >= data.len() {
                let waste = slot_size - data.len();
                if waste < best_waste {
                    best_waste = waste;
                    best_idx = Some(fl_idx);
                }
            }
        }

        let fl_idx = best_idx?;
        let slot_index = self.free_list.swap_remove(fl_idx);
        let slot = &mut self.slots[slot_index];

        // SAFETY: The slot's offset and size were validated when originally
        // written. The slot is marked unoccupied (checked by free_list membership).
        // data.len() <= slot.size (checked above). base is non-null.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.base.add(slot.offset), data.len());
        }

        slot.occupied = true;
        slot.engine = engine;
        // Keep original slot.size — the allocated region doesn't shrink.
        // Internal fragmentation (slot.size - data.len()) is acceptable
        // to avoid splitting complexity.

        Some(slot_index)
    }

    /// Read data from a slot (returns a slice into the mmap).
    pub fn read(&self, slot_index: usize) -> Result<&[u8]> {
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(|| MemError::Overflow(format!("invalid slot index: {slot_index}")))?;

        if !slot.occupied {
            return Err(MemError::Overflow(format!(
                "slot {slot_index} is not occupied"
            )));
        }

        // SAFETY: `self.base` is non-null. `slot.offset + slot.size` is within
        // `self.capacity` because slots are only created by `write()` which enforces
        // this via the grow check. After `mremap` (MREMAP_MAYMOVE), `self.base` is
        // updated to the new address, so all prior slot offsets remain valid within
        // the (potentially relocated) region. The slot is confirmed `occupied` above.
        let slice = unsafe { std::slice::from_raw_parts(self.base.add(slot.offset), slot.size) };

        Ok(slice)
    }

    /// Mark a slot as freed and add it to the free-list for reuse.
    pub fn free(&mut self, slot_index: usize) -> Result<()> {
        let slot = self
            .slots
            .get_mut(slot_index)
            .ok_or_else(|| MemError::Overflow(format!("invalid slot index: {slot_index}")))?;

        if !slot.occupied {
            return Err(MemError::Overflow(format!(
                "slot {slot_index} is already freed"
            )));
        }

        slot.occupied = false;
        self.free_list.push(slot_index);
        Ok(())
    }

    /// Current utilization of the mmap region in bytes.
    pub fn used_bytes(&self) -> usize {
        self.cursor
    }

    /// Total capacity of the mmap region in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Path to the backing overflow file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of slots (occupied or freed).
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Grow the mmap region to accommodate at least `required` bytes.
    fn grow(&mut self, required: usize) -> Result<()> {
        let new_capacity = (self.capacity * 2).max(required);

        if new_capacity > self.max_capacity {
            return Err(MemError::Overflow(format!(
                "overflow region would exceed max capacity: {} > {}",
                new_capacity, self.max_capacity
            )));
        }

        // SAFETY: `self._fd` is a valid open file descriptor (kept alive by Arc).
        // `new_capacity` has been validated to be <= `self.max_capacity`.
        unsafe {
            if libc::ftruncate(self._fd.as_raw_fd(), new_capacity as libc::off_t) != 0 {
                return Err(MemError::Overflow(
                    "failed to truncate file for growth".to_string(),
                ));
            }
        }

        // SAFETY: `self.base` is a valid mmap'd pointer with size `self.capacity`
        // (established at construction or the last successful mremap). `new_capacity`
        // is the ftruncate'd file size. MREMAP_MAYMOVE allows the kernel to relocate
        // the mapping; we update `self.base` below so all subsequent accesses use the
        // new address. No other thread accesses this region (!Send + !Sync).
        let new_base = unsafe {
            libc::mremap(
                self.base as *mut libc::c_void,
                self.capacity,
                new_capacity,
                libc::MREMAP_MAYMOVE,
            )
        };

        if new_base == libc::MAP_FAILED {
            return Err(MemError::Overflow(
                "failed to remap overflow region".to_string(),
            ));
        }

        self.base = new_base as *mut u8;
        self.capacity = new_capacity;

        // mremap produces a fresh mapping that inherits no advice — re-advise
        // so growth doesn't silently regress to MADV_NORMAL.
        self.madvise_state = advise_random(self.base, self.capacity, &self.path);

        Ok(())
    }
}

impl Drop for OverflowRegion {
    fn drop(&mut self) {
        // SAFETY: `self.base` was obtained from a successful `mmap` or `mremap` call,
        // and `self.capacity` matches the current mapping size. The null check guards
        // against double-unmap (though this should never happen in normal operation).
        unsafe {
            if !self.base.is_null() {
                let _ = libc::munmap(self.base as *mut libc::c_void, self.capacity);
            }
        }
    }
}

// SAFETY: OverflowRegion is intentionally !Send and !Sync because it holds
// a raw mutable pointer to an mmap'd region. The pointer is only safe to
// access from the single thread that owns the region (the core that created it).
// This enforces the TPC invariant: no cross-core sharing of mutable state.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_write() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");
        assert_eq!(region.used_bytes(), 0);
        assert!(region.capacity() > 0);

        let data = b"hello, world!";
        let slot_idx = region
            .write(data, EngineId::Vector)
            .expect("failed to write");

        assert_eq!(region.used_bytes(), data.len());
        assert_eq!(slot_idx, 0);
        assert_eq!(region.slot_count(), 1);
    }

    #[test]
    fn regrowth_re_advises_random_after_mremap() {
        // Regression coverage for issue #77: mremap-grown spill regions
        // inherited no advice, silently regressing to MADV_NORMAL. Private
        // to this file because it drives the internal bump allocator until
        // `grow()` fires — no public API exposes the growth path directly.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("regrow.mmap");

        let mut region = OverflowRegion::open_with_capacity(&path, 4096).unwrap();
        let before = test_hooks::madv_random_count();
        // open() already advised once.
        assert_eq!(region.madvise_state(), Some(libc::MADV_RANDOM));

        // Write 4096+ bytes to force grow().
        let blob = vec![0xABu8; 4096];
        region.write(&blob, EngineId::Vector).unwrap();
        region.write(&blob, EngineId::Vector).unwrap();

        assert!(
            region.capacity() > 4096,
            "test must have triggered at least one regrowth"
        );
        let after = test_hooks::madv_random_count();
        assert!(
            after > before,
            "mremap-grown region must re-advise MADV_RANDOM (before={before}, after={after})"
        );
        assert_eq!(region.madvise_state(), Some(libc::MADV_RANDOM));
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");

        let data1 = b"first";
        let data2 = b"second";

        let slot1 = region
            .write(data1, EngineId::Vector)
            .expect("failed to write slot 1");
        let slot2 = region
            .write(data2, EngineId::Sparse)
            .expect("failed to write slot 2");

        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);

        let read1 = region.read(slot1).expect("failed to read slot 1");
        let read2 = region.read(slot2).expect("failed to read slot 2");

        assert_eq!(read1, data1);
        assert_eq!(read2, data2);
    }

    #[test]
    fn free_slot() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");

        let slot = region
            .write(b"data", EngineId::Vector)
            .expect("failed to write");

        // Should read successfully before free.
        assert!(region.read(slot).is_ok());

        // Free the slot.
        region.free(slot).expect("failed to free slot");

        // Should fail after free.
        assert!(region.read(slot).is_err());
    }

    #[test]
    fn grow_region() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let initial = 1024; // 1 KiB for testing
        let mut region =
            OverflowRegion::open_with_capacity(&path, initial).expect("failed to open region");

        assert_eq!(region.capacity(), initial);

        // Write data larger than initial capacity.
        let large_data = vec![0u8; initial * 2];
        let slot = region
            .write(&large_data, EngineId::Vector)
            .expect("failed to write large data");

        // Region should have grown.
        assert!(region.capacity() > initial);

        // Should still be readable.
        let read_back = region.read(slot).expect("failed to read after growth");
        assert_eq!(read_back.len(), large_data.len());
        assert_eq!(read_back, &large_data[..]);
    }

    #[test]
    fn invalid_slot_index() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");

        // Try to read non-existent slot.
        assert!(region.read(999).is_err());

        // Try to free non-existent slot.
        assert!(region.free(999).is_err());
    }

    #[test]
    fn free_list_reuse() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");

        // Write three slots.
        let s0 = region.write(b"aaaa", EngineId::Vector).expect("write s0");
        let s1 = region.write(b"bbbb", EngineId::Sparse).expect("write s1");
        let _s2 = region.write(b"cccc", EngineId::Vector).expect("write s2");

        let cursor_before = region.used_bytes();

        // Free s0 and s1.
        region.free(s0).expect("free s0");
        region.free(s1).expect("free s1");

        // Write new data that fits in a freed slot.
        let s3 = region.write(b"dd", EngineId::Sparse).expect("write s3");

        // Should have reused a freed slot, not advanced the cursor.
        assert_eq!(region.used_bytes(), cursor_before);
        // s3 should reuse s0 or s1 (best-fit: both are 4 bytes, either works).
        assert!(s3 == s0 || s3 == s1);

        // Data should be readable.
        let data = region.read(s3).expect("read s3");
        assert_eq!(&data[..2], b"dd");
    }

    #[test]
    fn double_free_is_error() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");
        let slot = region.write(b"data", EngineId::Vector).expect("write");
        region.free(slot).expect("first free");
        assert!(region.free(slot).is_err());
    }

    #[test]
    fn slot_metadata() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = dir.path().join("overflow.mmap");

        let mut region = OverflowRegion::open(&path).expect("failed to open region");

        let slot1 = region
            .write(b"abc", EngineId::Vector)
            .expect("failed to write");
        let slot2 = region
            .write(b"defgh", EngineId::Sparse)
            .expect("failed to write");

        let s1 = &region.slots[slot1];
        let s2 = &region.slots[slot2];

        assert_eq!(s1.size, 3);
        assert_eq!(s1.engine, EngineId::Vector);
        assert!(s1.occupied);

        assert_eq!(s2.size, 5);
        assert_eq!(s2.engine, EngineId::Sparse);
        assert!(s2.occupied);

        region.free(slot1).expect("failed to free");
        assert!(!region.slots[slot1].occupied);
    }
}

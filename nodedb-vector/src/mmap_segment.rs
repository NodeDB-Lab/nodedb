//! Memory-mapped vector segment for L1 NVMe tiering.
//!
//! Stores FP32 vectors contiguously in a file, memory-mapped for read access.
//! Layout: `[dim:u32][count:u32][v0_f0..v0_fD][v1_f0..v1_fD]...]`

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

/// Memory-mapped vector segment file.
///
/// Not `Send` or `Sync` — owned by a single Data Plane core.
pub struct MmapVectorSegment {
    path: PathBuf,
    _fd: std::fs::File,
    base: *const u8,
    mmap_size: usize,
    dim: usize,
    count: usize,
    data_offset: usize,
}

const HEADER_SIZE: usize = 8;

impl MmapVectorSegment {
    /// Create a new segment file and write vectors to it.
    pub fn create(path: &Path, dim: usize, vectors: &[&[f32]]) -> std::io::Result<Self> {
        use std::io::Write;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let count = vectors.len();

        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        fd.write_all(&(dim as u32).to_le_bytes())?;
        fd.write_all(&(count as u32).to_le_bytes())?;

        for v in vectors {
            debug_assert_eq!(v.len(), dim);
            let bytes: &[u8] =
                unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4) };
            fd.write_all(bytes)?;
        }
        fd.sync_all()?;

        drop(fd);
        Self::open(path)
    }

    /// Open an existing segment file and memory-map it.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let fd = std::fs::OpenOptions::new().read(true).open(path)?;

        let file_size = fd.metadata()?.len() as usize;
        if file_size < HEADER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mmap vector segment too small for header",
            ));
        }

        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd.as_raw_fd(),
                0,
            )
        };

        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        let base = base as *const u8;

        let dim = unsafe {
            let ptr = base as *const u32;
            u32::from_le(*ptr) as usize
        };
        let count = unsafe {
            let ptr = base.add(4) as *const u32;
            u32::from_le(*ptr) as usize
        };

        // Reject dim=0 with nonzero count: get_vector would compute offset=HEADER_SIZE
        // for every ID, aliasing header bytes as vector data.
        if dim == 0 && count > 0 {
            unsafe {
                libc::munmap(base as *mut libc::c_void, file_size);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mmap segment has dim=0 with nonzero count",
            ));
        }

        // Use checked arithmetic to prevent usize overflow on crafted headers.
        let data_bytes = dim
            .checked_mul(count)
            .and_then(|dc| dc.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(HEADER_SIZE));
        let expected = match data_bytes {
            Some(v) => v,
            None => {
                unsafe {
                    libc::munmap(base as *mut libc::c_void, file_size);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("mmap segment header overflow: dim={dim}, count={count}"),
                ));
            }
        };
        if file_size < expected {
            unsafe {
                libc::munmap(base as *mut libc::c_void, file_size);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("mmap segment truncated: expected {expected} bytes, got {file_size}"),
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            _fd: fd,
            base,
            mmap_size: file_size,
            dim,
            count,
            data_offset: HEADER_SIZE,
        })
    }

    /// Get a vector by ID. Returns a slice into the mmap'd region.
    #[inline]
    pub fn get_vector(&self, id: u32) -> Option<&[f32]> {
        let idx = id as usize;
        if idx >= self.count {
            return None;
        }
        let byte_len = self.dim.checked_mul(4)?;
        let offset = self.data_offset.checked_add(idx.checked_mul(byte_len)?)?;
        let end = offset.checked_add(byte_len)?;
        if end > self.mmap_size {
            return None;
        }
        unsafe {
            let ptr = self.base.add(offset) as *const f32;
            Some(std::slice::from_raw_parts(ptr, self.dim))
        }
    }

    /// Prefetch a vector's page into memory via `madvise(MADV_WILLNEED)`.
    pub fn prefetch(&self, id: u32) {
        let idx = id as usize;
        if idx >= self.count {
            return;
        }
        let byte_len = match self.dim.checked_mul(4) {
            Some(v) => v,
            None => return,
        };
        let Some(idx_bytes) = idx.checked_mul(byte_len) else {
            return;
        };
        let Some(offset) = self.data_offset.checked_add(idx_bytes) else {
            return;
        };
        if offset
            .checked_add(byte_len)
            .is_none_or(|e| e > self.mmap_size)
        {
            return;
        }
        let page_start = offset & !(4095);
        let len = (byte_len + 4095) & !(4095);
        unsafe {
            libc::madvise(
                self.base.add(page_start) as *mut libc::c_void,
                len,
                libc::MADV_WILLNEED,
            );
        }
    }

    /// Prefetch a batch of vector IDs.
    pub fn prefetch_batch(&self, ids: &[u32]) {
        for &id in ids {
            self.prefetch(id);
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mmap_bytes(&self) -> usize {
        self.mmap_size
    }

    pub fn file_size(&self) -> usize {
        self.mmap_size
    }
}

impl Drop for MmapVectorSegment {
    fn drop(&mut self) {
        if !self.base.is_null() && self.mmap_size > 0 {
            unsafe {
                libc::munmap(self.base as *mut libc::c_void, self.mmap_size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vseg");

        let v0: Vec<f32> = vec![1.0, 2.0, 3.0];
        let v1: Vec<f32> = vec![4.0, 5.0, 6.0];
        let v2: Vec<f32> = vec![7.0, 8.0, 9.0];

        let seg = MmapVectorSegment::create(&path, 3, &[&v0, &v1, &v2]).unwrap();

        assert_eq!(seg.dim(), 3);
        assert_eq!(seg.count(), 3);

        assert_eq!(seg.get_vector(0).unwrap(), &[1.0, 2.0, 3.0]);
        assert_eq!(seg.get_vector(1).unwrap(), &[4.0, 5.0, 6.0]);
        assert_eq!(seg.get_vector(2).unwrap(), &[7.0, 8.0, 9.0]);
        assert!(seg.get_vector(3).is_none());
    }

    #[test]
    fn reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.vseg");

        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![i as f32, (i as f32).sin(), (i as f32).cos()])
            .collect();
        let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

        MmapVectorSegment::create(&path, 3, &refs).unwrap();

        let seg = MmapVectorSegment::open(&path).unwrap();
        assert_eq!(seg.count(), 100);
        for (i, v) in vectors.iter().enumerate() {
            let loaded = seg.get_vector(i as u32).unwrap();
            assert_eq!(loaded, v.as_slice());
        }
    }

    #[test]
    fn prefetch_does_not_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefetch.vseg");

        let v: Vec<f32> = vec![1.0; 768];
        let seg = MmapVectorSegment::create(&path, 768, &[&v]).unwrap();

        seg.prefetch(0);
        seg.prefetch(999);
    }

    #[test]
    fn empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.vseg");

        let seg = MmapVectorSegment::create(&path, 3, &[]).unwrap();
        assert_eq!(seg.count(), 0);
        assert!(seg.get_vector(0).is_none());
    }

    #[test]
    fn overflow_dim_count_rejected() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow.vseg");

        // dim=0x40000001, count=0x40000001: count * dim * 4 overflows usize on 64-bit
        // (0x40000001 * 0x40000001 * 4 = 0x4000000280000004, which wraps to a small value).
        let dim: u32 = 0x40000001;
        let count: u32 = 0x40000001;

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&dim.to_le_bytes()).unwrap();
        f.write_all(&count.to_le_bytes()).unwrap();
        // No actual vector data — just a 8-byte header.
        drop(f);

        let result = MmapVectorSegment::open(&path);
        assert!(
            result.is_err(),
            "expected Err for overflow-inducing dim/count, got Ok"
        );
    }

    #[test]
    fn truncated_file_rejected() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.vseg");

        // Header claims dim=3, count=100 but only 8 bytes of actual data.
        let dim: u32 = 3;
        let count: u32 = 100;

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&dim.to_le_bytes()).unwrap();
        f.write_all(&count.to_le_bytes()).unwrap();
        drop(f);

        let result = MmapVectorSegment::open(&path);
        match result {
            Err(e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::InvalidData,
                "expected InvalidData, got {:?}",
                e.kind()
            ),
            Ok(_) => panic!("expected Err for truncated file, got Ok"),
        }
    }

    #[test]
    fn zero_dim_with_nonzero_count_rejected() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zerodim.vseg");

        // dim=0, count=1000: expected size = HEADER_SIZE + 0 = 8, so the size
        // check passes, but get_vector would read header bytes as vector data.
        // dim=0 must be rejected outright.
        let dim: u32 = 0;
        let count: u32 = 1000;

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.write_all(&dim.to_le_bytes()).unwrap();
        f.write_all(&count.to_le_bytes()).unwrap();
        // Write enough padding so the file passes a naive size check.
        f.write_all(&[0u8; 64]).unwrap();
        drop(f);

        let result = MmapVectorSegment::open(&path);
        assert!(
            result.is_err(),
            "expected Err for dim=0 with nonzero count, got Ok"
        );
    }
}

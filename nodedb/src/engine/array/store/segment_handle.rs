//! Open segment file — `mmap`'d bytes plus the cached MBR R-tree.
//!
//! `SegmentReader` borrows the file slice with an explicit lifetime so
//! it cannot be stored alongside the `Mmap` directly (self-referential).
//! Instead the handle owns the mmap (via `Arc<Mmap>` so reader scans
//! can outlive a manifest swap) and exposes [`SegmentHandle::bytes`] to
//! reconstruct a borrowed reader on demand. The R-tree is built once
//! at open time and reused for every query.

use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use nodedb_array::segment::{HilbertPackedRTree, SegmentReader};

#[derive(Debug, thiserror::Error)]
pub enum SegmentHandleError {
    #[error("mmap segment failed: {detail}")]
    Mmap { detail: String },
    #[error("segment open: {detail}")]
    Open { detail: String },
    #[error("segment schema_hash mismatch: array={array:x} segment={seg:x}")]
    SchemaHashMismatch { array: u64, seg: u64 },
}

#[derive(Clone)]
pub struct SegmentHandle {
    bytes: Arc<Mmap>,
    /// Cached R-tree. Built once at open from the segment footer; the
    /// segment file itself is immutable for the life of the handle.
    rtree: Arc<HilbertPackedRTree>,
    schema_hash: u64,
    tile_count: usize,
    id: String,
}

impl SegmentHandle {
    /// Open and validate a segment file. Verifies schema_hash matches
    /// the array's expected value before caching the R-tree.
    pub fn open(
        path: &Path,
        id: String,
        expected_schema_hash: u64,
    ) -> Result<Self, SegmentHandleError> {
        let file = std::fs::File::open(path).map_err(|e| SegmentHandleError::Open {
            detail: format!("{path:?}: {e}"),
        })?;
        // Safety: the segment file is treated as read-only; we never
        // mutate it through the mmap and the file is not shared for
        // writing while the handle is alive.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| SegmentHandleError::Mmap {
            detail: format!("{path:?}: {e}"),
        })?;
        let mmap = Arc::new(mmap);
        let (rtree, schema_hash, tile_count) = {
            let reader = SegmentReader::open(&mmap[..]).map_err(|e| SegmentHandleError::Open {
                detail: format!("{path:?}: {e}"),
            })?;
            if reader.schema_hash() != expected_schema_hash {
                return Err(SegmentHandleError::SchemaHashMismatch {
                    array: expected_schema_hash,
                    seg: reader.schema_hash(),
                });
            }
            let rtree = HilbertPackedRTree::build(reader.tiles());
            (rtree, reader.schema_hash(), reader.tile_count())
        };
        Ok(Self {
            bytes: mmap,
            rtree: Arc::new(rtree),
            schema_hash,
            tile_count,
            id,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn schema_hash(&self) -> u64 {
        self.schema_hash
    }

    pub fn tile_count(&self) -> usize {
        self.tile_count
    }

    pub fn rtree(&self) -> &HilbertPackedRTree {
        &self.rtree
    }

    /// Build a borrowing `SegmentReader`. The reader must not outlive
    /// the handle reference it was built from.
    pub fn reader(&self) -> SegmentReader<'_> {
        // Already validated at open time; unwrap is safe because we
        // hold a live mmap of the same bytes that already parsed.
        SegmentReader::open(&self.bytes[..]).expect("segment validated at open")
    }
}

impl std::fmt::Debug for SegmentHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentHandle")
            .field("id", &self.id)
            .field("schema_hash", &format_args!("{:x}", self.schema_hash))
            .field("tile_count", &self.tile_count)
            .finish()
    }
}

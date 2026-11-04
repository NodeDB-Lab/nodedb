// SPDX-License-Identifier: BUSL-1.1

//! Open segment file — mmap'd bytes (plaintext) or owned decrypted buffer.
//!
//! `SegmentReader` borrows its byte slice with an explicit lifetime so it
//! cannot be stored alongside the buffer directly (self-referential). Instead
//! the handle owns the backing storage and exposes [`SegmentHandle::reader`]
//! to reconstruct a borrowed reader on demand. The R-tree is built once at
//! open time and reused for every query.
//!
//! For plaintext segments the backing storage is an `Arc<Mmap>` (zero-copy).
//! For encrypted segments the backing storage is an `Arc<Vec<u8>>` holding
//! the decrypted plaintext — mmap zero-copy is not available for encrypted
//! on-disk blobs, which is acceptable given the one-time open cost.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use nodedb_array::segment::{HilbertPackedRTree, SegmentFooter, SegmentHeader, SegmentReader};
use nodedb_wal::crypto::WalEncryptionKey;

#[derive(Debug, thiserror::Error)]
pub enum SegmentHandleError {
    #[error("mmap segment failed: {detail}")]
    Mmap { detail: String },
    #[error("segment open: {detail}")]
    Open { detail: String },
    #[error("segment schema_hash mismatch: array={array:x} segment={seg:x}")]
    SchemaHashMismatch { array: u64, seg: u64 },
}

/// Backing storage for an open segment.
enum Backing {
    /// Memory-mapped plaintext file (zero-copy).
    Mmap(Arc<Mmap>),
    /// Heap-allocated decrypted plaintext.
    Decrypted(Arc<Vec<u8>>),
}

impl Backing {
    fn bytes(&self) -> &[u8] {
        match self {
            Backing::Mmap(m) => m,
            Backing::Decrypted(v) => v,
        }
    }
}

#[derive(Clone)]
pub struct SegmentHandle {
    backing: Arc<Backing>,
    /// Cached R-tree. Built once at open from the segment footer.
    rtree: Arc<HilbertPackedRTree>,
    schema_hash: u64,
    tile_count: usize,
    id: String,
    /// Header and footer decoded once at open, shared by every reader.
    header: SegmentHeader,
    footer: Arc<SegmentFooter>,
    /// Encoded tile bytes under each placement, summed from the footer once
    /// at open.
    placement_bytes: Arc<HashMap<u32, u64>>,
}

impl SegmentHandle {
    /// Open and validate a segment file.
    ///
    /// When `kek` is `Some` the file is read into memory and AES-256-GCM
    /// decrypted; the resulting plaintext is used as the backing buffer.
    /// An encrypted (`SEGA`) segment without a KEK or a plaintext (`NDAS`)
    /// segment with a KEK both return a typed error from the array crate.
    ///
    /// When `kek` is `None` the file is memory-mapped directly (zero-copy)
    /// and must be a plaintext (`NDAS`) segment.
    pub fn open(
        path: &Path,
        id: String,
        expected_schema_hash: u64,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<Self, SegmentHandleError> {
        let backing: Arc<Backing> = if let Some(key) = kek {
            // Encrypted path: read raw bytes, decrypt via OwnedSegmentReader.
            let raw = std::fs::read(path).map_err(|e| SegmentHandleError::Open {
                detail: format!("{path:?}: {e}"),
            })?;
            let owned =
                nodedb_array::segment::reader::OwnedSegmentReader::open_with_kek(&raw, Some(key))
                    .map_err(|e| SegmentHandleError::Open {
                    detail: format!("{path:?}: {e}"),
                })?;
            // Extract the decrypted plaintext bytes for long-term storage.
            Arc::new(Backing::Decrypted(Arc::new(owned.into_plaintext())))
        } else {
            // Plaintext path: mmap directly.
            let file = std::fs::File::open(path).map_err(|e| SegmentHandleError::Open {
                detail: format!("{path:?}: {e}"),
            })?;
            // Safety: the segment file is treated as read-only; we never
            // mutate it through the mmap and the file is not shared for
            // writing while the handle is alive.
            let mmap = unsafe { Mmap::map(&file) }.map_err(|e| SegmentHandleError::Mmap {
                detail: format!("{path:?}: {e}"),
            })?;
            Arc::new(Backing::Mmap(Arc::new(mmap)))
        };

        let (rtree, schema_hash, tile_count, header, footer, placement_bytes) = {
            let reader =
                SegmentReader::open(backing.bytes()).map_err(|e| SegmentHandleError::Open {
                    detail: format!("{path:?}: {e}"),
                })?;
            if reader.schema_hash() != expected_schema_hash {
                return Err(SegmentHandleError::SchemaHashMismatch {
                    array: expected_schema_hash,
                    seg: reader.schema_hash(),
                });
            }
            let rtree = HilbertPackedRTree::build(reader.tiles());
            let mut placement_bytes: HashMap<u32, u64> = HashMap::new();
            for entry in reader.tiles() {
                let held = placement_bytes.entry(entry.placement).or_insert(0);
                *held = held.saturating_add(u64::from(entry.length));
            }
            (
                rtree,
                reader.schema_hash(),
                reader.tile_count(),
                *reader.header(),
                Arc::clone(reader.footer()),
                placement_bytes,
            )
        };

        Ok(Self {
            backing,
            rtree: Arc::new(rtree),
            schema_hash,
            tile_count,
            id,
            header,
            footer,
            placement_bytes: Arc::new(placement_bytes),
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

    /// Encoded tile bytes under each placement. Header and footer framing
    /// belong to no placement and do not count.
    pub fn placement_bytes(&self) -> &HashMap<u32, u64> {
        &self.placement_bytes
    }

    pub fn rtree(&self) -> &HilbertPackedRTree {
        &self.rtree
    }

    /// Build a borrowing `SegmentReader`. The reader must not outlive
    /// the handle reference it was built from. It shares the header and
    /// footer decoded at open, so no read decodes the footer again.
    pub fn reader(&self) -> SegmentReader<'_> {
        SegmentReader::from_parts(self.backing.bytes(), self.header, Arc::clone(&self.footer))
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

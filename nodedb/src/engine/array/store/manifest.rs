//! Per-array manifest — durable list of segment files plus the schema
//! hash that the segments were written with.
//!
//! Persisted as a single zerompk file at `<root>/manifest.ndam`. Updates
//! use the standard write-tmp-then-rename atomic swap so a torn write
//! never replaces the live manifest.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use nodedb_array::types::TileId;

const MANIFEST_FILENAME: &str = "manifest.ndam";
const MANIFEST_TMP_FILENAME: &str = "manifest.ndam.tmp";

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SegmentRef {
    /// File name relative to the array root, no path separators.
    pub id: String,
    /// Compaction level. New flushes land at L0; merges produce Ln+1.
    pub level: u8,
    pub min_tile: TileId,
    pub max_tile: TileId,
    pub tile_count: u32,
    /// LSN watermark recorded by the `ArrayFlush` record that produced
    /// this segment. Recovery uses it to skip already-durable WAL
    /// records.
    pub flush_lsn: u64,
}

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct Manifest {
    pub schema_hash: u64,
    pub segments: Vec<SegmentRef>,
    /// Highest WAL LSN reflected in any segment in this manifest.
    /// Recovery replays WAL records strictly greater than this LSN
    /// into the live memtable.
    pub durable_lsn: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest io: {detail}")]
    Io { detail: String },
    #[error("manifest decode failed: {detail}")]
    Decode { detail: String },
    #[error("manifest encode failed: {detail}")]
    Encode { detail: String },
}

impl Manifest {
    pub fn new(schema_hash: u64) -> Self {
        Self {
            schema_hash,
            segments: Vec::new(),
            durable_lsn: 0,
        }
    }

    /// Load the manifest at `<root>/manifest.ndam`. Returns a fresh
    /// empty manifest tagged with `schema_hash` if the file does not
    /// exist (caller is opening a brand-new array).
    pub fn load_or_new(root: &Path, schema_hash: u64) -> Result<Self, ManifestError> {
        let path = root.join(MANIFEST_FILENAME);
        match fs::read(&path) {
            Ok(bytes) => {
                let m: Manifest =
                    zerompk::from_msgpack(&bytes).map_err(|e| ManifestError::Decode {
                        detail: format!("{path:?}: {e}"),
                    })?;
                Ok(m)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new(schema_hash)),
            Err(e) => Err(ManifestError::Io {
                detail: format!("{path:?}: {e}"),
            }),
        }
    }

    /// Atomically write the manifest to disk. Steps: serialise → write
    /// tmp file → fsync tmp → rename → fsync directory.
    pub fn persist(&self, root: &Path) -> Result<(), ManifestError> {
        let bytes = zerompk::to_msgpack_vec(self).map_err(|e| ManifestError::Encode {
            detail: e.to_string(),
        })?;
        let tmp = root.join(MANIFEST_TMP_FILENAME);
        let final_path = root.join(MANIFEST_FILENAME);
        {
            let mut f = fs::File::create(&tmp).map_err(|e| ManifestError::Io {
                detail: format!("create {tmp:?}: {e}"),
            })?;
            f.write_all(&bytes).map_err(|e| ManifestError::Io {
                detail: format!("write {tmp:?}: {e}"),
            })?;
            f.sync_all().map_err(|e| ManifestError::Io {
                detail: format!("fsync {tmp:?}: {e}"),
            })?;
        }
        fs::rename(&tmp, &final_path).map_err(|e| ManifestError::Io {
            detail: format!("rename to {final_path:?}: {e}"),
        })?;
        // Best-effort directory fsync. POSIX guarantees rename durability
        // only after a directory fsync.
        if let Ok(dir) = fs::File::open(root) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    pub fn append(&mut self, seg: SegmentRef) {
        self.durable_lsn = self.durable_lsn.max(seg.flush_lsn);
        self.segments.push(seg);
    }

    /// Replace `removed` ids with the new `added` segments. Used by the
    /// compaction merger after it has produced a replacement segment.
    pub fn replace(&mut self, removed: &[String], added: Vec<SegmentRef>) {
        self.segments.retain(|s| !removed.contains(&s.id));
        for seg in added {
            self.durable_lsn = self.durable_lsn.max(seg.flush_lsn);
            self.segments.push(seg);
        }
    }

    pub fn segments_at_level(&self, level: u8) -> impl Iterator<Item = &SegmentRef> {
        self.segments.iter().filter(move |s| s.level == level)
    }
}

/// Returns the absolute path the engine writes a segment file to.
pub fn segment_path(root: &Path, id: &str) -> PathBuf {
    root.join(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn seg(id: &str, level: u8, lsn: u64) -> SegmentRef {
        SegmentRef {
            id: id.into(),
            level,
            min_tile: TileId::snapshot(0),
            max_tile: TileId::snapshot(0),
            tile_count: 1,
            flush_lsn: lsn,
        }
    }

    #[test]
    fn persist_and_reload_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut m = Manifest::new(0xCAFE);
        m.append(seg("0001.ndas", 0, 5));
        m.persist(dir.path()).unwrap();
        let loaded = Manifest::load_or_new(dir.path(), 0xCAFE).unwrap();
        assert_eq!(loaded.schema_hash, 0xCAFE);
        assert_eq!(loaded.segments.len(), 1);
        assert_eq!(loaded.durable_lsn, 5);
    }

    #[test]
    fn load_missing_returns_empty_manifest() {
        let dir = TempDir::new().unwrap();
        let m = Manifest::load_or_new(dir.path(), 0x1).unwrap();
        assert!(m.segments.is_empty());
        assert_eq!(m.durable_lsn, 0);
    }

    #[test]
    fn replace_swaps_segments_and_keeps_max_lsn() {
        let mut m = Manifest::new(0x1);
        m.append(seg("a", 0, 1));
        m.append(seg("b", 0, 2));
        m.replace(&["a".into(), "b".into()], vec![seg("c", 1, 2)]);
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].id, "c");
        assert_eq!(m.durable_lsn, 2);
    }
}

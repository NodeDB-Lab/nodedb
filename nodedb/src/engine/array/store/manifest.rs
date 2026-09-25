// SPDX-License-Identifier: BUSL-1.1

//! Per-array manifest — durable list of segment files plus the schema
//! hash that the segments were written with.
//!
//! Persisted as a single zerompk file at `<root>/manifest.ndam`. Updates
//! use the standard write-tmp-then-rename atomic swap so a torn write
//! never replaces the live manifest.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use nodedb_array::types::TileId;

use crate::types::replay_stamp::{InvalidReplayStamp, ReplayStamp};

const MANIFEST_FILENAME: &str = "manifest.ndam";

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
    /// The highest LSN the stamp of the flush that wrote this segment named.
    /// Informational: replay decides a record by the manifest's stamp.
    pub flush_lsn: u64,
}

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct Manifest {
    pub schema_hash: u64,
    pub segments: Vec<SegmentRef>,
    /// The records the segments this manifest names hold: the replay stamp
    /// of the flush that last published it. Restart replay skips an array
    /// record exactly when this stamp names it.
    ///
    /// A newer flush's stamp names every record an older one named, because
    /// the core's applied set only grows and its outcome floor only rises. So
    /// the newest stamp replaces the older one rather than merging with it.
    /// Compaction and purge rewrite segments without changing which records
    /// they hold, so they leave the stamp alone.
    pub replay: ReplayStamp,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest io: {detail}")]
    Io { detail: String },
    #[error("manifest decode failed: {detail}")]
    Decode { detail: String },
    #[error("manifest encode failed: {detail}")]
    Encode { detail: String },
    #[error("manifest carries an invalid replay stamp: {source}")]
    InvalidReplayStamp {
        #[source]
        source: InvalidReplayStamp,
    },
}

impl Manifest {
    pub fn new(schema_hash: u64) -> Self {
        Self {
            schema_hash,
            segments: Vec::new(),
            replay: ReplayStamp::default(),
        }
    }

    /// Load the manifest at `<root>/manifest.ndam`. Returns a fresh
    /// empty manifest tagged with `schema_hash` if the file does not
    /// exist (caller is opening a brand-new array).
    pub fn load_or_new(root: &Path, schema_hash: u64) -> Result<Self, ManifestError> {
        let path = root.join(MANIFEST_FILENAME);
        match nodedb_wal::segment::read_checkpoint_framed(&path) {
            Ok(bytes) => {
                let m: Manifest =
                    zerompk::from_msgpack(&bytes).map_err(|e| ManifestError::Decode {
                        detail: format!("{path:?}: {e}"),
                    })?;
                // `skips` relies on the stamp's shape; a stamp that breaks it
                // could skip a record no segment holds.
                m.replay
                    .validate()
                    .map_err(|source| ManifestError::InvalidReplayStamp { source })?;
                Ok(m)
            }
            Err(nodedb_wal::WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::new(schema_hash))
            }
            Err(e) => Err(ManifestError::Io {
                detail: format!("{path:?}: {e}"),
            }),
        }
    }

    /// Atomically write the manifest to disk: serialise → write tmp → fsync tmp
    /// → rename → fsync directory, via the shared
    /// `nodedb_wal::segment::write_checkpoint_framed`.
    ///
    /// This write is the commit point of a flush — a segment file is only
    /// reachable once the manifest names it — so the whole ordering is
    /// load-bearing, and the directory fsync is a requirement rather than a
    /// best effort: without it the rename can be visible while the manifest's
    /// own directory entry is not, and the array comes back at the PREVIOUS
    /// manifest, silently dropping every cell the flush had just made durable
    /// and whose WAL records the checkpoint then authorised deleting.
    pub fn persist(&self, root: &Path) -> Result<(), ManifestError> {
        let bytes = zerompk::to_msgpack_vec(self).map_err(|e| ManifestError::Encode {
            detail: e.to_string(),
        })?;
        nodedb_wal::segment::write_checkpoint_framed(root, MANIFEST_FILENAME, &bytes).map_err(|e| {
            ManifestError::Io {
                detail: format!("publish {:?}: {e}", root.join(MANIFEST_FILENAME)),
            }
        })
    }

    pub fn append(&mut self, seg: SegmentRef) {
        self.segments.push(seg);
    }

    /// Replace `removed` ids with the new `added` segments. Used by the
    /// compaction merger after it has produced a replacement segment.
    pub fn replace(&mut self, removed: &[String], added: Vec<SegmentRef>) {
        self.segments.retain(|s| !removed.contains(&s.id));
        self.segments.extend(added);
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
        assert_eq!(loaded.replay, ReplayStamp::default());
    }

    #[test]
    fn load_missing_returns_empty_manifest() {
        let dir = TempDir::new().unwrap();
        let m = Manifest::load_or_new(dir.path(), 0x1).unwrap();
        assert!(m.segments.is_empty());
        assert_eq!(m.replay, ReplayStamp::default());
    }

    #[test]
    fn replace_swaps_segments_and_keeps_the_stamp() {
        let mut m = Manifest::new(0x1);
        m.replay = ReplayStamp::through(2);
        m.append(seg("a", 0, 1));
        m.append(seg("b", 0, 2));
        m.replace(&["a".into(), "b".into()], vec![seg("c", 1, 2)]);
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].id, "c");
        assert_eq!(m.replay, ReplayStamp::through(2));
    }

    #[test]
    fn the_stamp_round_trips_through_persist() {
        let dir = TempDir::new().unwrap();
        let mut m = Manifest::new(0xCAFE);
        m.replay = ReplayStamp {
            prefix: 5,
            applied_above: vec![crate::types::replay_stamp::LsnRange { start: 9, end: 9 }],
        };
        m.persist(dir.path()).unwrap();
        let loaded = Manifest::load_or_new(dir.path(), 0xCAFE).unwrap();
        assert_eq!(loaded.replay, m.replay);
    }
}

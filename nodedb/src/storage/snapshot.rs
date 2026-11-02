// SPDX-License-Identifier: BUSL-1.1

//! Layered snapshots and Point-In-Time Recovery (PITR).
//!
//! - **Incremental bases**: every base is a full image built from
//!   content-addressed chunks. A base that reuses stored chunks names its
//!   parent, and still restores without it.
//! - **PITR**: Restore base image, then replay WAL to exact target timestamp.
//!   The offline `nodedb restore` plans and runs it from this catalog.
//!
//! Snapshot operations emit begin/end markers with consistent LSN boundaries.

use tracing::info;

use crate::types::Lsn;

/// On-disk format version for [`SnapshotMeta`].
///
/// Readers must reject any snapshot whose stored `format_version` does not
/// match this value.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Snapshot metadata stored alongside the snapshot data.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotMeta {
    /// On-disk format version. Must equal [`SNAPSHOT_FORMAT_VERSION`] on read.
    pub format_version: u32,
    /// Unique snapshot identifier.
    pub snapshot_id: u64,
    /// The lowest per-core replay floor. WAL records above it bring every
    /// core forward from this snapshot.
    pub begin_lsn: Lsn,
    /// The highest per-core replay floor. Every core holds every record at
    /// or below it that the core applied.
    pub end_lsn: Lsn,
    /// The highest LSN whose effect the snapshot includes. A core can hold
    /// records above its own floor, so this can exceed `end_lsn`.
    pub applied_high_lsn: Lsn,
    /// UTC timestamp when snapshot was initiated (microseconds since epoch).
    pub created_at_us: u64,
    /// Node that created this snapshot.
    pub created_by: String,
    /// Whether the base reused stored chunks.
    pub kind: SnapshotKind,
    /// The newest base before this one, when this one reused stored chunks.
    pub parent_id: Option<u64>,
    /// Total uncompressed data size in bytes.
    pub data_bytes: u64,
}

impl SnapshotMeta {
    /// Verify that the deserialized `format_version` matches the current
    /// on-disk format. Returns an error if the versions differ.
    pub fn validate_format_version(&self) -> crate::Result<()> {
        if self.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(crate::Error::VersionCompat {
                detail: format!(
                    "snapshot {} has format_version {} but this node expects {}; \
                     upgrade required or restore from a compatible snapshot",
                    self.snapshot_id, self.format_version, SNAPSHOT_FORMAT_VERSION,
                ),
            });
        }
        Ok(())
    }
}

/// Snapshot type.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum SnapshotKind {
    /// A full image whose chunks were all new to the store.
    Base = 0,
    /// A full image that reuses chunks already stored. `parent_id` names the
    /// newest base before it. Restore reads only its own manifest.
    Delta = 1,
}

/// Snapshot catalog: tracks all available snapshots for restore planning.
#[derive(Debug, Clone)]
pub struct SnapshotCatalog {
    snapshots: Vec<SnapshotMeta>,
}

impl SnapshotCatalog {
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
    }

    /// Register a completed snapshot.
    pub fn add(&mut self, meta: SnapshotMeta) {
        info!(
            id = meta.snapshot_id,
            kind = ?meta.kind,
            begin_lsn = meta.begin_lsn.as_u64(),
            end_lsn = meta.end_lsn.as_u64(),
            applied_high_lsn = meta.applied_high_lsn.as_u64(),
            "registered snapshot"
        );
        self.snapshots.push(meta);
    }

    /// Find the best base snapshot for a given target LSN.
    ///
    /// Returns the most recent snapshot of either kind whose
    /// `applied_high_lsn` is at or below `target_lsn`, so the base holds no
    /// write after the target. Every snapshot is a full image.
    pub fn find_base(&self, target_lsn: Lsn) -> Option<&SnapshotMeta> {
        self.snapshots
            .iter()
            .filter(|s| s.applied_high_lsn <= target_lsn)
            .max_by_key(|s| s.applied_high_lsn)
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// All registered snapshots, in registration order.
    pub fn all(&self) -> &[SnapshotMeta] {
        &self.snapshots
    }

    /// Emit a snapshot begin marker with the current LSN boundary.
    ///
    /// Called at the start of a snapshot operation. The begin LSN
    /// is the current WAL position — all data up to this point will
    /// be included in the snapshot.
    pub fn emit_begin_marker(&self, current_lsn: Lsn) -> SnapshotMarker {
        info!(lsn = current_lsn.as_u64(), "snapshot BEGIN marker");
        SnapshotMarker {
            marker_type: MarkerType::Begin,
            lsn: current_lsn,
            timestamp_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        }
    }

    /// Emit a snapshot end marker with the final LSN boundary.
    ///
    /// Called after all snapshot data has been flushed. The end LSN
    /// is the WAL position at completion — the snapshot covers
    /// [begin_lsn, end_lsn] inclusively.
    pub fn emit_end_marker(&self, end_lsn: Lsn) -> SnapshotMarker {
        info!(lsn = end_lsn.as_u64(), "snapshot END marker");
        SnapshotMarker {
            marker_type: MarkerType::End,
            lsn: end_lsn,
            timestamp_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        }
    }
}

/// Snapshot begin/end marker for consistent LSN boundaries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMarker {
    pub marker_type: MarkerType,
    pub lsn: Lsn,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MarkerType {
    Begin,
    End,
}

impl Default for SnapshotCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_snapshot(id: u64, end_lsn: u64) -> SnapshotMeta {
        base_holding(id, end_lsn, end_lsn)
    }

    /// A base whose cores hold records up to `applied_high`, above their
    /// floors up to `end_lsn`.
    fn base_holding(id: u64, end_lsn: u64, applied_high: u64) -> SnapshotMeta {
        SnapshotMeta {
            format_version: SNAPSHOT_FORMAT_VERSION,
            snapshot_id: id,
            begin_lsn: Lsn::new(1),
            end_lsn: Lsn::new(end_lsn),
            applied_high_lsn: Lsn::new(applied_high),
            created_at_us: 1_700_000_000_000_000,
            created_by: "node-1".into(),
            kind: SnapshotKind::Base,
            parent_id: None,
            data_bytes: 1_000_000,
        }
    }

    /// A base that reuses the chunks of `parent`.
    fn incremental(id: u64, applied_high: u64, parent: u64) -> SnapshotMeta {
        SnapshotMeta {
            kind: SnapshotKind::Delta,
            parent_id: Some(parent),
            ..base_holding(id, applied_high, applied_high)
        }
    }

    #[test]
    fn empty_catalog() {
        let cat = SnapshotCatalog::new();
        assert!(cat.is_empty());
        assert!(cat.find_base(Lsn::new(100)).is_none());
    }

    #[test]
    fn find_base_snapshot() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));
        cat.add(base_snapshot(2, 500));

        // Target LSN 300: should pick base #1 (end_lsn=100 <= 300).
        let base = cat.find_base(Lsn::new(300)).unwrap();
        assert_eq!(base.snapshot_id, 1);

        // Target LSN 600: should pick base #2 (end_lsn=500 <= 600).
        let base = cat.find_base(Lsn::new(600)).unwrap();
        assert_eq!(base.snapshot_id, 2);

        // Target LSN 50: no base covers it.
        assert!(cat.find_base(Lsn::new(50)).is_none());
    }

    #[test]
    fn an_incremental_base_restores_on_its_own() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_snapshot(1, 100));
        cat.add(incremental(2, 200, 1));
        assert_eq!(cat.find_base(Lsn::new(250)).unwrap().snapshot_id, 2);
        assert_eq!(cat.find_base(Lsn::new(150)).unwrap().snapshot_id, 1);
    }

    /// A core applies records out of LSN order, so its files can hold a record
    /// above its floor. A base holding a write after the target must never be
    /// chosen, whatever its floors say.
    #[test]
    fn find_base_never_picks_a_base_above_the_target() {
        let mut cat = SnapshotCatalog::new();
        cat.add(base_holding(1, 100, 100));
        cat.add(base_holding(2, 200, 260));

        // Base #2's floor (200) is below 250, but it holds record 260.
        assert_eq!(cat.find_base(Lsn::new(250)).unwrap().snapshot_id, 1);
        assert_eq!(cat.find_base(Lsn::new(260)).unwrap().snapshot_id, 2);
        for target in [0, 50, 99, 100, 150, 259, 260, 1_000] {
            if let Some(base) = cat.find_base(Lsn::new(target)) {
                assert!(
                    base.applied_high_lsn <= Lsn::new(target),
                    "base #{} holds lsn {} past target {target}",
                    base.snapshot_id,
                    base.applied_high_lsn.as_u64()
                );
            }
        }
    }

    #[test]
    fn snapshot_meta_roundtrip_format_version() {
        let meta = base_snapshot(42, 500);
        assert_eq!(meta.format_version, SNAPSHOT_FORMAT_VERSION);

        // Serialize and deserialize via MessagePack.
        let bytes = zerompk::to_msgpack_vec(&meta).expect("serialize");
        let decoded: SnapshotMeta = zerompk::from_msgpack(&bytes).expect("deserialize");

        assert_eq!(decoded, meta);
        decoded
            .validate_format_version()
            .expect("current version must validate");
    }

    #[test]
    fn snapshot_meta_rejects_unknown_format_version() {
        let mut meta = base_snapshot(7, 200);
        meta.format_version = SNAPSHOT_FORMAT_VERSION + 1;

        let err = meta
            .validate_format_version()
            .expect_err("must reject future version");

        let msg = err.to_string();
        assert!(
            msg.contains("format_version"),
            "error message should mention format_version: {msg}"
        );
        assert!(
            msg.contains(&(SNAPSHOT_FORMAT_VERSION + 1).to_string()),
            "error message should contain the bad version: {msg}"
        );
    }
}

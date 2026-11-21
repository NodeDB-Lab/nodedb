// SPDX-License-Identifier: BUSL-1.1

//! Errors of the offline point-in-time restore.

use std::path::PathBuf;

use crate::storage::snapshot_restore::CoverageError;

/// One node life that holds base snapshots, as an ambiguity error lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifeSummary {
    pub incarnation: String,
    pub bases: usize,
    /// Creation time of the newest base, microseconds since the epoch.
    pub newest_created_at_us: Option<u64>,
    /// The highest `applied_high_lsn` of any base.
    pub newest_applied_high_lsn: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("{detail}")]
    Usage { detail: String },

    #[error(transparent)]
    Node(#[from] crate::Error),

    #[error("WAL segment: {0}")]
    Wal(#[from] nodedb_wal::WalError),

    #[error(transparent)]
    Coverage(#[from] CoverageError),

    #[error(transparent)]
    Generation(#[from] crate::storage::restore_generation::GenerationError),

    #[error("the config has no [{section}] section; restore needs it because {why}")]
    MissingConfig {
        section: &'static str,
        why: &'static str,
    },

    #[error(
        "[{section}] resolves to {}, inside the data directory {}; point {section}.local_dir \
         outside the data directory the restore writes",
        store_dir.display(),
        data_dir.display()
    )]
    StoreInsideDataDir {
        section: &'static str,
        store_dir: PathBuf,
        data_dir: PathBuf,
    },

    #[error("the snapshot store holds no base snapshot of node {node_id}")]
    NoBaseSnapshots { node_id: u64 },

    #[error(
        "node {node_id} has no base snapshot under incarnation {requested}; \
         incarnations with bases: {}",
        available.join(", ")
    )]
    UnknownIncarnation {
        node_id: u64,
        requested: String,
        available: Vec<String>,
    },

    #[error(
        "node {node_id} has base snapshots under {} incarnations; pass --incarnation with one \
         of them:\n{}",
        lives.len(),
        describe_lives(lives)
    )]
    AmbiguousIncarnation {
        node_id: u64,
        lives: Vec<LifeSummary>,
    },

    #[error("the WAL archive of incarnation {incarnation} holds no time anchor")]
    NoTimeAnchors { incarnation: String },

    #[error(
        "target time {target_ns}ns precedes the first archived WAL time anchor \
         ({first_anchor_ns}ns); no committed state is known that early"
    )]
    TargetBeforeFirstAnchor {
        target_ns: u64,
        first_anchor_ns: u64,
    },

    #[error(
        "no archived WAL time anchor is later than the target {target_ns}ns; the last anchor \
         is at LSN {anchored_lsn} and the archive ends at LSN {archived_through}, so a batch \
         committed by the target can be missing from the archive; wait for the WAL archiver \
         to upload a segment with a later anchor, or restore to an earlier target"
    )]
    NoAnchorPastTarget {
        target_ns: u64,
        anchored_lsn: u64,
        archived_through: u64,
    },

    #[error(
        "no base snapshot holds only writes at or below LSN {target_lsn}; the oldest base \
         holds writes through LSN {oldest_applied_high_lsn}"
    )]
    NoBaseAtOrBelow {
        target_lsn: u64,
        oldest_applied_high_lsn: u64,
    },

    #[error(
        "a Raft snapshot install at WAL LSN {install_lsn} replaced rows no WAL record carries, \
         and no base snapshot taken after it holds only writes at or below LSN {target_lsn}; \
         restore to an earlier target, or wait for the base the install forces"
    )]
    NoBaseAfterSnapshotInstall { install_lsn: u64, target_lsn: u64 },

    #[error(
        "the archived metadata log reaches HLC {archived_through_ns}ns, before the target \
         {target_ns}ns; restore to an earlier target, or wait for the next metadata log \
         archive pass"
    )]
    MetadataNotArchived {
        target_ns: u64,
        archived_through_ns: u64,
    },

    #[error(
        "no time anchor places LSN {target_lsn} in time, so the catalog cannot be cut there; \
         restore to a time target, or to an LSN whose commit batch the archive holds"
    )]
    TargetTimeUnknown { target_lsn: u64 },

    #[error(
        "every base at or below LSN {target_lsn} holds metadata log entry {entry_index}, which \
         committed at or after the target; restore to a later target"
    )]
    NoBaseBeforeCatalogChange { entry_index: u64, target_lsn: u64 },

    #[error(
        "archived WAL segment {key} has CRC32C {actual:08x}, which matches none of its \
         checksum markers ({})",
        hex_list(expected)
    )]
    ChecksumMismatch {
        key: String,
        actual: u32,
        expected: Vec<u32>,
    },

    #[error("archived WAL segment {key} is {actual} bytes, but the archive lists {listed}")]
    SizeMismatch {
        key: String,
        actual: u64,
        listed: u64,
    },

    #[error(
        "archived WAL segment {key} changed after the restore planned it (CRC32C {planned:08x}, \
         now {actual:08x}); run the restore again"
    )]
    SegmentChanged {
        key: String,
        planned: u32,
        actual: u32,
    },

    #[error(
        "archived WAL segment {key} holds LSN {lsn} after LSN {previous}; a segment holds \
         records in LSN order"
    )]
    SegmentOutOfOrder {
        key: String,
        previous: u64,
        lsn: u64,
    },

    #[error("WAL alignment {alignment} is not a power of two; fix [tuning.wal] alignment")]
    BadAlignment { alignment: usize },

    #[error(
        "the archived WAL of node {node_id} incarnation {incarnation} holds no record of \
         restore point {restore_point}; check the id with SHOW RESTORE POINTS, and wait for \
         the WAL archiver to upload the segment that records it"
    )]
    RestorePointNotArchived {
        restore_point: u64,
        node_id: u64,
        incarnation: String,
    },

    #[error(
        "the records of restore point {restore_point} carry different watermarks: {}",
        watermarks.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")
    )]
    RestorePointWatermarks {
        restore_point: u64,
        watermarks: Vec<u64>,
    },

    #[error(
        "no base snapshot of this node life fits restore point {restore_point}: a base must \
         hold no write at or above the point's watermark (the first is at LSN {}), and its \
         catalogs must hold no metadata entry above index {restore_point}",
        first_dropped_lsn.map_or_else(|| "none".to_string(), |lsn| lsn.to_string())
    )]
    NoClusterBase {
        restore_point: u64,
        first_dropped_lsn: Option<u64>,
    },

    #[error(
        "the metadata log archive holds no entry {missing} of the range {from}..={through} the \
         restore replays after the base; no node archived it"
    )]
    MetadataLogGap {
        from: u64,
        through: u64,
        missing: u64,
    },

    #[error(
        "restore failed ({error}), and removing its partial files from {} failed ({cleanup}); \
         empty that directory before retrying",
        data_dir.display()
    )]
    CleanupFailed {
        error: Box<RestoreError>,
        cleanup: Box<RestoreError>,
        data_dir: PathBuf,
    },
}

impl From<RestoreError> for crate::Error {
    fn from(e: RestoreError) -> Self {
        match e {
            RestoreError::Node(inner) => inner,
            RestoreError::Wal(inner) => crate::Error::Wal(inner),
            RestoreError::Usage { detail } => crate::Error::BadRequest { detail },
            other => crate::Error::Storage {
                engine: "restore".into(),
                detail: other.to_string(),
            },
        }
    }
}

fn describe_lives(lives: &[LifeSummary]) -> String {
    lives
        .iter()
        .map(|life| {
            let created = life
                .newest_created_at_us
                .map_or_else(|| "unknown".to_string(), format_micros);
            let applied = life
                .newest_applied_high_lsn
                .map_or_else(|| "unknown".to_string(), |lsn| lsn.to_string());
            format!(
                "  {}: {} bases, newest created {created}, holds writes through LSN {applied}",
                life.incarnation, life.bases
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hex_list(values: &[u32]) -> String {
    if values.is_empty() {
        return "none".into();
    }
    values
        .iter()
        .map(|v| format!("{v:08x}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// RFC 3339 text for microseconds since the epoch.
pub fn format_micros(micros: u64) -> String {
    i64::try_from(micros)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_micros)
        .map_or_else(
            || format!("{micros}us"),
            |at| at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        )
}

/// RFC 3339 text for nanoseconds since the epoch.
pub fn format_nanos(nanos: u64) -> String {
    i64::try_from(nanos)
        .ok()
        .map(chrono::DateTime::from_timestamp_nanos)
        .map_or_else(
            || format!("{nanos}ns"),
            |at| at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
}

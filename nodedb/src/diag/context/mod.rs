// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for capture sites outside the WAL, grouped by the
//! subsystem that detects them.
//!
//! A grouping key excludes per-occurrence values, so a retry loop files one
//! report with a rising count instead of one per attempt.

mod catalog;
mod crdt;
mod data_plane;
mod ingest;
mod quota;
mod recovery;
mod retention;
mod vector;
mod write_path;

pub(in crate::diag) use catalog::{
    CatalogApplyOrphanRow, CollectionPurgeRowMissing, ConsumerGroupOffsetsRetained,
    MetadataApplyWedged, SynonymGroupNotApplied,
};
pub(in crate::diag) use crdt::HistoryCompactionNotApplied;
pub use data_plane::LostResponseWrite;
pub(in crate::diag) use data_plane::{CalvinCompletionTimeout, DataPlaneResponseLost};
pub(in crate::diag) use ingest::IlpAcceptedLinesDropped;
pub use ingest::IlpFlushOutcome;
pub use quota::{DATABASE_SCOPE, TENANT_SCOPE};
pub(in crate::diag) use quota::{
    QuotaRowNotInstalled, QuotaRowWriteFailed, QuotaScopePurgeIncomplete, QuotaScopeReplayAborted,
    ScopeQuotaNotInstalled,
};
pub(in crate::diag) use recovery::{ReplayRecordUnapplied, WalArchivalFailedTruncationHeld};
pub(in crate::diag) use retention::RetentionAutowireOrphaned;
pub(in crate::diag) use vector::VectorIndexNotApplied;
pub(in crate::diag) use write_path::{
    BatchInsertWithoutSurrogates, FtsIndexUpdateFailed, OrphanedIndexEntryAfterDelete,
    StrictRowUndecodable, WriteAckedWithoutDurability,
};

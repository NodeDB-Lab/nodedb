// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for capture sites outside the WAL, grouped by the
//! subsystem that detects them.
//!
//! A grouping key excludes per-occurrence values, so a retry loop files one
//! report with a rising count instead of one per attempt.

mod catalog;
mod columnar;
mod continuous_agg;
mod crdt;
mod crdt_dead_letter_store;
mod cut_barrier;
mod data_plane;
mod event_image;
mod index_rebuild;
mod ingest;
mod lease;
mod outcome_floor;
mod quota;
mod raft_apply;
mod recovery;
mod redo_stream;
mod retention;
mod vector;
mod vector_build;
mod write_path;

pub(in crate::diag) use catalog::{
    CatalogApplyOrphanRow, CollectionPurgeRowMissing, ConsumerGroupOffsetsRetained,
    MetadataApplyWedged, SynonymGroupNotApplied,
};
pub(in crate::diag) use columnar::{ColumnarSegmentCorrupt, TimeseriesPartitionUnreadable};
pub(in crate::diag) use continuous_agg::ContinuousAggregateNotApplied;
pub(in crate::diag) use crdt::{CrdtDeadLetterNotEnqueued, HistoryCompactionNotApplied};
pub(in crate::diag) use crdt_dead_letter_store::{
    CrdtDeadLetterNotRestored, CrdtDeadLetterNotStored,
};
pub(in crate::diag) use cut_barrier::CutBarrierNotPlaced;
pub use data_plane::LostResponseWrite;
pub(in crate::diag) use data_plane::{
    CalvinApplyHalted, CalvinCompletionTimeout, CoreFailStopped, DataPlaneResponseLost,
};
pub(in crate::diag) use event_image::{StrictRowImageUnrendered, TimeseriesRowImageUndecodable};
pub(in crate::diag) use index_rebuild::IndexRebuildNotInstalled;
pub(in crate::diag) use ingest::IlpAcceptedLinesDropped;
pub use ingest::IlpFlushOutcome;
pub(in crate::diag) use lease::DescriptorLeaseNotRenewed;
pub(in crate::diag) use outcome_floor::{WriteWindowHeld, WriteWindowLeaked};
pub use quota::{DATABASE_SCOPE, TENANT_SCOPE};
pub(in crate::diag) use quota::{
    QuotaRowNotInstalled, QuotaRowWriteFailed, QuotaScopePurgeIncomplete, QuotaScopeReplayAborted,
    ScopeQuotaNotInstalled,
};
pub(in crate::diag) use raft_apply::{CalvinBarrierLogStoreFailed, RaftEntryReapplied};
pub(in crate::diag) use recovery::{ReplayRecordUnapplied, WalArchivalFailedTruncationHeld};
pub(in crate::diag) use redo_stream::{
    RedoAbandonGivenUp, RedoSnapshotDebtNotRecorded, RedoStreamLostHere,
};
pub(in crate::diag) use retention::RetentionAutowireOrphaned;
pub(in crate::diag) use vector::VectorIndexNotApplied;
pub(in crate::diag) use vector_build::{VectorBuildNotInstalled, VectorBuilderUnavailable};
pub(in crate::diag) use write_path::{
    BatchInsertWithoutSurrogates, FtsIndexUpdateFailed, OrphanedIndexEntryAfterDelete,
    StrictRowUndecodable, WriteAckedWithoutDurability,
};

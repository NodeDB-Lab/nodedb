// SPDX-License-Identifier: BUSL-1.1

//! Immutable audit log for security-relevant events.
//!
//! The log is organized into focused submodules:
//!
//! - [`auth`] — authenticated-identity context attached to every entry.
//! - [`level`] — the recorded-event severity filter.
//! - [`event`] — the `AuditEvent` enum + level/routing rules.
//! - [`entry`] — the durable `AuditEntry` struct + hash-chain helper.
//! - [`ddl_detail`] — structured detail body for `AuditEvent::DdlChange`.
//! - [`undrop_detail`] — structured detail body for `UNDROP COLLECTION`.
//! - [`log`] — the in-memory append-only `AuditLog` itself.
//! - [`flush_control`] — early-flush signal and flush serialization for the buffer.
//! - [`chain`] — hash-chain verification over persisted and buffered entries.
//! - [`wal_sink`] — the durable append target every recorded entry reaches.
//! - [`restore`] — boot-time replay of the audit WAL into the catalog.
//! - [`export_sink`] — the export target, such as SIEM, every recorded entry reaches.
//! - [`guard`] — the lock guard that exports recorded entries after it unlocks.
//! - [`recorder`] — the bounded hand-off that records Event Plane entries on the Control Plane.

pub mod auth;
pub mod chain;
pub mod ddl_detail;
pub mod emitter;
pub mod entry;
pub mod event;
pub mod export_sink;
pub mod flush_control;
pub mod guard;
pub mod level;
pub mod log;
pub mod log_commit;
pub mod recorder;
pub mod restore;
pub mod undrop_detail;
pub mod wal_sink;

pub use auth::AuditAuth;
#[cfg(test)]
pub use chain::verify_chain;
pub use chain::{ChainReport, ChainVerifier, checkpoint_detail};
pub use ddl_detail::DdlAuditDetail;
pub use emitter::{ArcAuditEmitter, AuditEmitContext, AuditEmitter, NoopAuditEmitter};
pub use entry::AuditEntry;
pub use event::AuditEvent;
pub use export_sink::AuditExportSink;
pub use flush_control::{
    AuditFlushControl, BACKPRESSURE_MULTIPLE, DEFAULT_BACKPRESSURE_WAIT, DEFAULT_FLUSH_BOUND,
    HARD_BOUND_MULTIPLE,
};
pub use guard::{AuditLogGuard, lock_audit_log};
pub use level::AuditLevel;
pub use log::AuditLog;
pub use log::AuditRecordInput;
pub use log_commit::{
    StagedBatch, record_batch_best_effort, record_batch_concurrent, record_concurrent,
};
pub use recorder::{AuditRecorder, PendingAudit};
pub use restore::restore_audit_log;
pub use undrop_detail::{UndropAuditDetail, UndropStage};
pub use wal_sink::AuditWalSink;

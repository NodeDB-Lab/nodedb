// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral transaction commit outcome + the single Data-Plane
//! dispatch seam the neutral orchestrator drives.
//!
//! Both pgwire and native build a [`TxnDataPlane`] over their own dispatch
//! path and hand it to [`run_commit`](super::commit::run_commit) /
//! [`run_rollback`](super::lifecycle::run_rollback) /
//! [`run_savepoint`](super::savepoint_ops::run_savepoint); the orchestrator
//! never references a transport type. Each transport maps the returned
//! [`CommitOutcome`] (and [`AbortReason`]) into its own wire response.

use std::future::Future;
use std::pin::Pin;

use crate::bridge::envelope::{ErrorCode, Response};
use nodedb_physical::physical_task::PhysicalTask;

/// Terminal outcome of a COMMIT driven through the neutral orchestrator.
pub enum CommitOutcome {
    /// The transaction committed: every durable batch flushed, side effects
    /// (offsets, GAP_FREE finalize, DDL propose, cursor/notify flush) fired.
    Committed,
    /// The transaction aborted before durable commit. Carries enough to let
    /// each transport reconstruct the exact wire error it emitted before the
    /// orchestrator was extracted.
    Aborted { reason: AbortReason },
}

/// Why a COMMIT aborted. Each variant carries enough context for a transport
/// adapter to reproduce the SQLSTATE (and, where the payload allows, the
/// message) the pre-extraction path emitted.
pub enum AbortReason {
    /// Snapshot-isolation conflict: a read key's collection advanced past the
    /// read LSN. pgwire/native → `40001` serialization failure.
    Serialization,
    /// `SessionStore::commit` reported no committable transaction. → `25000`.
    NoTransaction,
    /// A durable transaction batch dispatched successfully but the Data Plane
    /// reported a logical failure (`Status::Error`). `code` is the response's
    /// `error_code`, mapped to SQLSTATE via `error_code_to_sqlstate`.
    BatchRejected { code: Option<ErrorCode> },
    /// A Calvin coordinator channel closed (deadline-driven cancel). → `57014`.
    CalvinCancelled,
    /// Timed out awaiting a Calvin sequencer assignment or completion. → `57014`.
    CalvinTimeout,
    /// A dispatch call returned a hard error (batch dispatch `Err`, sequencer
    /// unavailable, WAL append/serialize failure, or an unexpected Calvin
    /// outcome). Adapters map the carried error per their existing rules.
    Dispatch(crate::Error),
    /// A collection this transaction's buffered writes were planned against
    /// has been altered since the statement that buffered them. The
    /// transaction is retryable as a whole. → `40001`.
    SchemaChanged { detail: String },
    /// Proposing the buffered DDL batch to the metadata Raft group failed.
    /// → `XX000`.
    DdlPropose(crate::Error),
}

/// The one Data-Plane dispatch seam the neutral transaction orchestrator uses.
///
/// A transport implements this over its own no-WAL dispatch path (pgwire keeps
/// its materialize-freeze gate; native routes through the gateway-or-SPSC
/// branch). `dispatch_no_wal` runs a single already-built [`PhysicalTask`] and
/// returns the neutral [`Response`], exactly as the transport's own single-task
/// dispatch would.
///
/// The returned future is boxed: the orchestrator awaits it inside the deeply
/// nested listener request pipeline, and a boxed (type-erased) future keeps that
/// async type-layout depth bounded rather than compounding per transport impl.
pub trait TxnDataPlane {
    /// Dispatch one task to the Data Plane without a per-task WAL append (the
    /// whole transaction is written as a single WAL record by the caller).
    ///
    /// `wal_lsn` is the LSN of that single transaction WAL record: it is
    /// stamped onto the dispatched `Request` so the Data Plane records the
    /// committed write version for every key in the batch. `None` when no WAL
    /// record was written (empty / read-only commit).
    fn dispatch_no_wal<'a>(
        &'a self,
        task: PhysicalTask,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>>;

    /// The source the transaction's committed writes carry into the Event
    /// Plane, on every replica that applies them.
    fn event_source(&self) -> crate::event::EventSource;
}

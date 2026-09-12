// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the write and replay paths: a record that did not
//! apply, a write acked without durability, an index update that was lost.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a committed, CRC-valid WAL record that startup replay could not
/// apply. Called only from `replay_abort`, so a WAL tail that fails
/// identically on every core files one growing report, not one per core.
pub fn replay_record_unapplied(
    engine: &str,
    stage: &str,
    core_id: usize,
    record_lsn: u64,
    detail: &str,
) {
    let ctx = context::ReplayRecordUnapplied {
        engine,
        stage,
        core_id,
        record_lsn,
        detail,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "WAL replay: a committed record could not be applied",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an acknowledged write whose redo record the Control-Plane funnel
/// was supposed to mint but did not. Called only from the durable-at-ack
/// barrier in `submit_write`, so a hammered op files one growing report.
pub fn write_acked_without_durability(engine: &'static str) {
    let ctx = context::WriteAckedWithoutDurability { engine };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "write acknowledged with no durable redo record",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document write rejected because its inverted-index update
/// failed. Called from `index_document_in_txn`'s error arm — the client's
/// error message says the write failed, not that the FTS index caused it.
pub fn fts_index_update_failed(err: &crate::Error, collection: &str, surrogate: u32) {
    let class = error_class(err);
    let ctx = context::FtsIndexUpdateFailed {
        collection,
        surrogate,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "document write rejected: full-text index update failed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an index entry a committed DELETE's cascade failed to remove.
/// Called from the bulk-delete cascade's warn sites, after the row's own
/// transaction has already committed — `index_kind` names which cascaded
/// structure (`"inverted"`, `"secondary"`, `"graph_edge"`) still carries it.
pub fn orphaned_index_entry_after_delete(
    err: &crate::Error,
    collection: &str,
    index_kind: &'static str,
) {
    let class = error_class(err);
    let ctx = context::OrphanedIndexEntryAfterDelete {
        collection,
        index_kind,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "committed delete's cascade left an index entry behind",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document batch insert refused because its rows carry no
/// surrogates. Called from the batch-insert handler's parallel-length guard;
/// the actual defect is in whatever produced the mismatched plan.
pub fn batch_insert_without_surrogates(
    collection: &str,
    document_count: usize,
    surrogate_count: usize,
) {
    let ctx = context::BatchInsertWithoutSurrogates {
        collection,
        document_count,
        surrogate_count,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "document batch insert refused: rows carry no cross-engine identity",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a WAL segment that cold storage did not accept before the segment
/// was due for deletion. Called only from the checkpoint's archival loop,
/// which also holds truncation back at the segment.
pub fn wal_archival_failed_truncation_held(
    stage: &str,
    err: Option<&crate::Error>,
    segment_first_lsn: u64,
) {
    let class = match err {
        Some(e) => error_class(e),
        None => "none".to_owned(),
    };
    let ctx = context::WalArchivalFailedTruncationHeld {
        stage,
        error_class: &class,
        segment_first_lsn,
    };
    let capture = Capture::new(
        EventKind::Error,
        "WAL archival failed: truncation held back to keep the archive continuous",
    )
    .domain(&ctx)
    .with_backtrace();
    let _ = match err {
        Some(e) => capture.error_chain(error_chain_of(e)).emit(),
        None => capture.emit(),
    };
}

/// Report a stored Binary Tuple that does not decode against its
/// collection's strict schema. Called from each UPDATE site that detects
/// the row, alongside the error it returns; `site` names that site.
pub fn strict_row_undecodable(collection: &str, doc_id: &str, site: &'static str) {
    let ctx = context::StrictRowUndecodable {
        collection,
        doc_id,
        site,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "stored strict row did not decode, so the statement reading it is refused",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

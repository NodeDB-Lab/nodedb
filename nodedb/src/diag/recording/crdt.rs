// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for CRDT history work that never reached the Data Plane, and
//! for dead-letter entries the queue or the store could not hold.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report the per-node oplog compaction that failed while applying a
/// committed `CompactHistory`. Called from the compaction fan-out, whose
/// owed-compaction row stays for a retry: `stage` names which part failed.
pub fn history_compaction_not_applied(
    err: &crate::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) {
    let class = error_class(err);
    let ctx = context::HistoryCompactionNotApplied {
        stage,
        database_id,
        tenant_id,
        collection,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "history post-apply: the committed COMPACT HISTORY never reached this node's \
         Data Plane",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a constraint-rejected CRDT delta the dead-letter queue refused.
/// Called from the validated apply, which refuses the delta as an error so
/// the sender keeps it.
pub fn crdt_dead_letter_not_enqueued(
    err: &nodedb_crdt::CrdtError,
    tenant_id: u64,
    collection: &str,
    constraint: &str,
) {
    let class = error_class(err);
    let ctx = context::CrdtDeadLetterNotEnqueued {
        tenant_id,
        collection,
        constraint,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "crdt apply: a constraint-rejected delta could not be dead-lettered",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}

/// Report a dead-letter entry the store refused. Called only from
/// `store_crdt_dead_letter`, the one site every apply path stores through.
pub fn crdt_dead_letter_not_stored(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    entry: &nodedb_crdt::DeadLetter,
    source_lsn: u64,
) {
    let class = error_class(err);
    let ctx = context::CrdtDeadLetterNotStored {
        database_id,
        tenant_id,
        collection: &entry.collection,
        constraint: &entry.violated_constraint,
        source_lsn,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "crdt apply: a rejected delta's dead-letter entry could not be stored",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}

/// Report a stored dead-letter entry the queue refused on restore. Called
/// only from `restore_dead_letters`.
pub fn crdt_dead_letter_not_restored(
    err: &nodedb_crdt::CrdtError,
    tenant_id: u64,
    source_lsn: Option<u64>,
) {
    let class = error_class(err);
    let ctx = context::CrdtDeadLetterNotRestored {
        tenant_id,
        source_lsn,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "crdt engine open: a stored dead-letter entry does not fit the queue",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

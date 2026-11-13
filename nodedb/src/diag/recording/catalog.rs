// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for the metadata applier and the catalog rows it leaves
//! behind when an apply is incomplete.

use faultbox::{Capture, EventKind, error_chain_of};
use nodedb_cluster::MetadataEntry;

use super::shared::{entry_kind, error_class};
use crate::diag::context;

/// Report a durable host-side effect failure that stopped the metadata
/// applier without advancing its watermark. Called from the `apply` loop's
/// `break` on `apply_host_side_effects` error, so a re-delivered failing
/// entry files one growing report, not one per retry.
pub fn metadata_apply_wedged(
    err: &crate::Error,
    entry: &MetadataEntry,
    raft_index: u64,
    last_applied_watermark: u64,
    permanent: bool,
) {
    let kind = entry_kind(entry);
    let class = error_class(err);
    let ctx = context::MetadataApplyWedged {
        raft_index,
        last_applied_watermark,
        entry_kind: &kind,
        error_class: &class,
        permanent,
    };
    let _ = Capture::new(
        EventKind::Error,
        "metadata applier: durable host-side effect failed; watermark not advanced",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a `catalog_entry::apply_to` call that left redb with an orphaned
/// parent-replicated row (a primary row with no matching `StoredOwner`, or
/// the reverse). Called from `apply_to`, right after `verify_redb_integrity`.
pub fn catalog_apply_orphan_row(entry_kind: &str, orphan_kind: &str, orphan_count: usize) {
    let ctx = context::CatalogApplyOrphanRow {
        entry_kind,
        orphan_kind,
        orphan_count,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "catalog_entry::apply_to left an orphaned parent-replicated catalog row",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report consumer-group offsets that a replicated deletion or migration left
/// behind on this node. Called from the post-apply offset-store arms.
pub fn consumer_group_offsets_retained(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    stream: &str,
    group: &str,
    operation: &'static str,
) {
    let class = error_class(err);
    let ctx = context::ConsumerGroupOffsetsRetained {
        database_id,
        tenant_id,
        stream,
        group,
        operation,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "post-apply: consumer-group offsets survived a replicated deletion on this node",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report per-node synonym group work that failed while applying a committed
/// catalog entry. Called from the synonym group post-apply arms, which cannot
/// propagate: `stage` names whether the record encode or the Data Plane
/// dispatch was lost.
pub fn synonym_group_not_applied(
    err: &crate::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    group: &str,
) {
    let class = error_class(err);
    let ctx = context::SynonymGroupNotApplied {
        stage,
        database_id,
        tenant_id,
        group,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "synonym group post-apply: the committed group change never reached this node's \
         FTS backend",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a collection purge-log prune this node could not decide on: the
/// log did not read. Called from the collection-GC sweep, which retries on
/// its next tick. Rows past retention stay on record until a prune lands.
pub fn purge_log_prune_not_proposed(err: &crate::Error) {
    let class = error_class(err);
    let ctx = context::PurgeLogPruneNotProposed {
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "collection-gc: the collection purge log did not read, so no prune was proposed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

// SPDX-License-Identifier: BUSL-1.1

//! The single entry point every write path calls to run its enforcement.
//!
//! # Why enforcement lives one level ABOVE `apply_point_put`
//!
//! The materialized-sum target write is itself an
//! [`apply_point_put`](crate::data::executor::core_loop::CoreLoop::apply_point_put).
//! If enforcement ran INSIDE `apply_point_put`, that derived write would re-enter
//! enforcement, and the only thing standing between it and unbounded recursion
//! would be a re-entrancy flag — a piece of mutable state that has to be set
//! before the inner call and cleared after it, on every path. There is no such
//! path discipline available here: every step in the write path is a `?`, and a
//! single early return between "set" and "clear" leaves the flag stuck, silently
//! disabling enforcement for every subsequent write on that core. That failure
//! is invisible; it looks exactly like a collection with no constraints.
//!
//! Keeping enforcement above `apply_point_put` removes the flag and the failure
//! mode with it. The derived target write calls `apply_point_put` directly, which
//! runs no enforcement, so it IS the recursion floor by construction rather than
//! by a runtime guard that can be left in the wrong state.
//!
//! No depth counter is needed either: a materialized sum whose target is itself
//! the source of another materialized sum (an A→B→C chain) is refused at DDL
//! time, so a target write can never produce another target write even in
//! principle.
//!
//! # The hash chain does NOT flow through here
//!
//! Hash chaining rewrites the row BODY — it wraps the document with a
//! `_chain_hash` field — so it must run BEFORE the body is encoded and stored,
//! and its output is the body `apply_point_put` receives. This funnel runs
//! against the images of a write that has already been encoded and applied, so
//! it is structurally the wrong place for a body rewrite. Hash chaining stays a
//! separate pre-write call on the paths that use it.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::{self, BalancedEntry};
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::types::{DatabaseId, TenantId};

/// What one enforcement pass produced for its caller to account for.
///
/// The `Default` is the "nothing to enforce" result, which a caller uses when
/// the write has no images to fold at all.
#[derive(Default)]
pub(in crate::data::executor) struct WriteEnforcementOutcome {
    /// Target rows the materialized-sum bindings updated, with their pre-images,
    /// so a transactional caller can push one undo entry per derived write.
    pub target_writes: Vec<TargetWrite>,
    /// Signed BALANCED entries this write contributes to its boundary's
    /// balance check. The check itself spans the whole boundary — debits and
    /// credits arrive on different rows — so the entries are handed back to the
    /// caller that owns that scope, which settles them through
    /// [`CoreLoop::settle_balanced_entries`](crate::data::executor::core_loop::CoreLoop::settle_balanced_entries)
    /// before it commits.
    pub balanced_entries: Vec<BalancedEntry>,
}

/// Run every write-path enforcement for one write.
///
/// `txn` is the caller's transaction: every derived write this performs lands in
/// it, so the source row and everything its constraints implied commit or roll
/// back as one unit. On `Err` the caller must drop `txn` without committing.
pub(in crate::data::executor) fn run_write_enforcement(
    core: &mut CoreLoop,
    txn: &WriteTransaction,
    ctx: EnforcementCtx<'_>,
    images: RowImages<'_>,
) -> crate::Result<WriteEnforcementOutcome> {
    let config_key = (
        DatabaseId::new(ctx.database_id),
        TenantId::new(ctx.tid),
        ctx.collection.to_string(),
    );
    // An unregistered collection has no declared constraints, so there is
    // nothing to enforce and nothing to report. The bindings and the
    // BALANCED rule read the judged config, as a committed redo's validate
    // does: a pinned write folds and judges by its proposer's rules.
    let Some(config) = core.judged_doc_config(&config_key) else {
        return Ok(WriteEnforcementOutcome {
            target_writes: Vec::new(),
            balanced_entries: Vec::new(),
        });
    };
    // Cloned out of the config so the immutable borrow of `core` ends here: the
    // materialized-sum apply below needs `&mut CoreLoop` because a target write
    // is a full document write.
    let bindings = config.enforcement.materialized_sum_sources.clone();
    let balanced_def = config.enforcement.balanced.clone();

    // Every mutation shape contributes, signed by its effect on the stored set:
    // an insert adds, a delete subtracts, an update does both. Counting inserts
    // alone would let a boundary delete one leg of a balanced journal and still
    // pass.
    let balanced_entries = match &balanced_def {
        Some(def) => balanced::entries_for(def, &images),
        None => Vec::new(),
    };

    let target_writes = if bindings.is_empty() {
        Vec::new()
    } else {
        core.apply_materialized_sums(txn, &ctx, &bindings, &images)?
    };

    Ok(WriteEnforcementOutcome {
        target_writes,
        balanced_entries,
    })
}

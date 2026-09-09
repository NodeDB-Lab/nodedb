// SPDX-License-Identifier: BUSL-1.1

//! The MERGE request shape and the choice of which pass runs it.
//!
//! Its own file because the mode selection is the one thing every MERGE path
//! shares and none of them owns: [`MergeParams`] is filled in by the Data-Plane
//! dispatcher, read by the orchestrated RESOLVE and APPLY passes in
//! `merge_orchestrated`, and consumed by the legacy walk here. Keeping the
//! struct and the three-way branch apart from any one pass is what stops a
//! future field from being interpreted differently by whichever pass happens to
//! declare it.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;

/// Parameters for `execute_merge`.
pub(in crate::data::executor) struct MergeParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
    /// RESOLVE-ONLY read pass (orchestrator phase 1): classify without writing
    /// and return the NOT-MATCHED insert rows.
    pub resolve_only: bool,
    /// Control-Plane-pre-assigned surrogates for the NOT-MATCHED insert rows,
    /// keyed by source join value (orchestrator phase 3). `Some` selects the
    /// atomic verify-and-apply path; `None` (with `resolve_only == false`)
    /// selects the legacy per-row apply, retained only as a fallback — the
    /// in-transaction MERGE that once used it is now resolved + staged at
    /// statement time into concrete point ops
    /// (`control::server::shared::session::expander_stage`).
    pub resolved_inserts: Option<&'a [(String, u32)]>,
    /// Control-Plane-shipped source rows for cross-core MERGE. When `Some`, the
    /// source join-map is built from these pre-scanned
    /// `(source_doc_id, raw_stored_source_bytes)` rows instead of a local read
    /// of the source collection (whose vShard may live on a different core).
    /// `None` selects the legacy local-storage read (co-resident / in-txn
    /// buffered replay).
    pub source_rows: Option<&'a [(String, Vec<u8>)]>,
    /// Projection for a `MERGE ... RETURNING`. Honoured only by the orchestrated
    /// apply pass — the only pass that walks every arm's row bodies. `None`
    /// selects the affected-count payload.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled RLS read policy of the TARGET collection, gating the
    /// `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy of the TARGET collection, gating the PERSIST.
    /// Every arm writes a target row, decided against the image it stores: the
    /// post-image for an UPDATE or INSERT arm, the pre-image for a DELETE arm.
    /// A separate slot from `rls_filters`: that one bounds what may be shown
    /// back, this one bounds what may be written.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this merge's arms may touch, resolved on the Control Plane from the
    /// RESOLVE pass's classification. Empty on the RESOLVE pass itself, which
    /// writes nothing and therefore folds nothing.
    pub resolved_sum_targets: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless target, `None`
    /// otherwise. `Some` makes `build_update_doc`'s post-image guard run.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Execute a MERGE statement.
    ///
    /// Three modes, selected by [`MergeParams`]:
    /// - `resolve_only` → [`Self::execute_merge_resolve`]: a read pass that
    ///   returns the NOT-MATCHED insert rows for Control-Plane surrogate
    ///   assignment (no writes).
    /// - `resolved_inserts.is_some()` → [`Self::execute_merge_apply`]: the
    ///   atomic apply with CP-assigned surrogates + resolve→apply drift verify.
    ///
    /// There is no third mode. An UNRESOLVED merge (`resolve_only == false` and
    /// `resolved_inserts == None`) is intercepted on the Control Plane by every
    /// entry point — autocommit by the merge orchestrator, in-transaction by the
    /// statement-time expander — so it cannot reach the Data Plane. It is
    /// refused rather than executed: the per-row walk that once served it wrote
    /// NOT-MATCHED inserts under a raw `sparse.put` with no surrogate (invisible
    /// to every cross-engine index), minted no replicated entry, and applied
    /// each arm outside any shared transaction. Keeping such a path alive would
    /// mean a second, silent way for a merge to land — including one that folds
    /// no materialized-sum delta.
    pub(in crate::data::executor) fn execute_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        if params.resolve_only {
            return self.execute_merge_resolve(task, tid, params);
        }
        if params.resolved_inserts.is_some() {
            return self.execute_merge_apply(task, tid, params);
        }
        self.response_error(
            task,
            crate::bridge::envelope::ErrorCode::Internal {
                detail: "MERGE reached the Data Plane unresolved: every entry point \
                         resolves it on the Control Plane before dispatch"
                    .into(),
            },
        )
    }
}

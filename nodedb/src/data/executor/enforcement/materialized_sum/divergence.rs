// SPDX-License-Identifier: BUSL-1.1

//! The leader-side check that a plan's materialized-sum resolution still covers
//! the rows the write actually matched.
//!
//! A predicate-driven write (`BulkUpdate`, `BulkDelete`, `TRUNCATE`,
//! `UPDATE ... FROM`) carries a resolution the Control Plane derived from a
//! reconnaissance scan taken BEFORE execution. Between that scan and this apply,
//! a concurrent write can add a row with a join value the scan never saw, or
//! rewrite a matched row's join key. Folding such a row would address a target
//! the plan carries no surrogate for — and the fold's own
//! `MaterializedSumTargetNotFound` fires only once the statement is already
//! part-written, leaving a stored total that disagrees with the `SUM(...)` over
//! the source rows.
//!
//! So the leader re-derives the join-key set from the rows it matched and
//! answers `ErrorCode::OllpRetryRequired` BEFORE any write, exactly as the
//! surrogate-set and implicit-edge OLLP verifications in
//! [`crate::data::executor::handlers::bulk_dml`] do — same leader-only gate,
//! same "return without writing" contract, same retry code the coordinator
//! re-recons on. Followers accept the leader's decision: a follower whose redb
//! snapshot lags would re-derive a different set and poison an attempt that is
//! valid on the leader.
//!
//! # Coverage, not equality
//!
//! The surrogate-set check compares for EQUALITY because the predicted set IS
//! the set every replica applies to. This resolution is a lookup TABLE, not an
//! apply set: an entry the write turns out not to need costs one unused
//! surrogate, while an entry it needs and does not have is the wrong total. So
//! the guard is coverage — every join value the matched rows require must be
//! present — which fires on exactly the drift that corrupts a total and never on
//! drift that cannot.
//!
//! # Coverage is demanded only of the bindings THIS core applies
//!
//! A binding whose target does not share the source's vShard is applied by a
//! sibling `ApplyBalanceDelta` task, and the Control Plane records that by
//! REMOVING its join values from the resolution. Those values are missing on
//! purpose, so checking them would report a divergence every single time and
//! the coordinator would re-recon, resolve, remove them again, and resubmit
//! forever. They are covered instead by the settlement's own read-set entry,
//! which this core validates through the ordinary Calvin OCC path — same
//! "abort before any mutation" contract, on the images the shipped deltas were
//! actually folded from.

use nodedb_physical::physical_plan::{ResolvedSumTarget, UpdateValue};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::types::{DatabaseId, TenantId};

/// Scope of one divergence check.
pub(in crate::data::executor) struct SumTargetCheck<'a> {
    pub database_id: u64,
    pub tid: u64,
    /// The SOURCE collection the statement writes — the one whose bindings
    /// decide which join column each matched row is read on.
    pub collection: &'a str,
    /// The statement's `SET` assignments, so an update that rewrites a join
    /// column is checked against the target it moves rows ONTO as well. Empty
    /// for a delete-shaped statement, and for a caller that hands in the
    /// post-images itself.
    pub updates: &'a [(String, UpdateValue)],
    /// The resolution the plan carried.
    pub resolved: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Whether the carried resolution fails to cover the join values `rows`
    /// require.
    ///
    /// `rows` are already-decoded row images. A collection that declares no
    /// materialized-sum binding, and every replica that is not the group leader,
    /// diverge vacuously — they return `false` without reading anything.
    pub(in crate::data::executor) fn sum_targets_diverged(
        &self,
        check: &SumTargetCheck<'_>,
        rows: &[serde_json::Value],
    ) -> bool {
        if !self.ollp_is_group_leader {
            return false;
        }
        let key = (
            DatabaseId::new(check.database_id),
            TenantId::new(check.tid),
            check.collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&key) else {
            return false;
        };
        for binding in &config.enforcement.materialized_sum_sources {
            // A CROSS-SHARD binding's join values are deliberately ABSENT from
            // the resolution: the Control Plane settled their deltas at plan
            // time and removed them, which is how this core knows not to apply
            // them itself. Demanding coverage for them would report every such
            // statement as diverged, and the coordinator would re-recon and
            // resubmit a plan that omits them again — a livelock, not a retry.
            //
            // Their drift is caught by the settlement's own OCC read-set entry
            // instead: the images the shipped deltas were folded from are
            // stamped as a read, and this core votes ABORT before any mutation
            // if they have moved.
            if !crate::query::sum_target_is_co_resident(
                DatabaseId::new(check.database_id),
                check.collection,
                &binding.target_collection,
            ) {
                continue;
            }
            // An assignment that will not evaluate fails the statement in the
            // write path, on the same row and with the same typed error. It is
            // not a prediction drift, so it must not be reported as one — a
            // retry would re-run the same failing expression forever.
            let Ok(required) = crate::query::binding_join_keys(binding, check.updates, rows) else {
                continue;
            };
            if let Some(missing) = crate::query::missing_join_key(
                &binding.target_collection,
                &required,
                check.resolved,
            ) {
                tracing::debug!(
                    core = self.core_id,
                    collection = %check.collection,
                    target = %binding.target_collection,
                    join_column = %binding.join_column,
                    %missing,
                    "materialized-sum resolution no longer covers the matched rows"
                );
                return true;
            }
        }
        false
    }

    /// Same check, over rows named by their storage keys.
    ///
    /// Each row is read and decoded in the collection's own encoding — a strict
    /// collection stores Binary Tuples the schemaless decoder cannot read. A row
    /// that is already gone, or whose body will not decode, carries no join
    /// value: the write path reaches the same conclusion about it, and a body
    /// that is genuinely corrupt fails there rather than being laundered into a
    /// retry here.
    pub(in crate::data::executor) fn sum_targets_diverged_for_ids(
        &self,
        check: &SumTargetCheck<'_>,
        doc_ids: &[String],
    ) -> bool {
        if !self.ollp_is_group_leader || !self.declares_materialized_sums(check) {
            return false;
        }
        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            // `doc_id` is a bare string from a raw-table scan; a shape that
            // fails to parse as a storage key contributes no row, same as a
            // `get` miss right below.
            let Some(key) = nodedb_types::StorageKey::parse(doc_id) else {
                continue;
            };
            let Ok(Some(bytes)) =
                self.sparse
                    .get(check.database_id, check.tid, check.collection, &key)
            else {
                continue;
            };
            if let Some(row) = self.decode_source_row(check, &bytes) {
                rows.push(row);
            }
        }
        self.sum_targets_diverged(check, &rows)
    }

    /// Decode one stored row of the checked collection into a document.
    ///
    /// Public to the executor so a handler that already holds a row's stored
    /// bytes — a matched pre-image it read for its own reasons — can feed the
    /// check without a second read.
    pub(in crate::data::executor) fn decode_source_row(
        &self,
        check: &SumTargetCheck<'_>,
        body: &[u8],
    ) -> Option<serde_json::Value> {
        let format = self.sparse_body_format(
            DatabaseId::new(check.database_id),
            TenantId::new(check.tid),
            check.collection,
        );
        decode_scanned_document(body, format.as_format_ref()).ok()
    }

    /// Whether the checked collection drives any materialized-sum binding.
    ///
    /// The gate that keeps this whole path free for the collections — nearly all
    /// of them — that declare nothing: no read, no decode, no fold.
    pub(in crate::data::executor) fn declares_materialized_sums(
        &self,
        check: &SumTargetCheck<'_>,
    ) -> bool {
        let key = (
            DatabaseId::new(check.database_id),
            TenantId::new(check.tid),
            check.collection.to_string(),
        );
        self.doc_configs
            .get(&key)
            .is_some_and(|config| !config.enforcement.materialized_sum_sources.is_empty())
    }
}

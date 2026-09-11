// SPDX-License-Identifier: BUSL-1.1

//! Everything a bulk predicate write verifies BEFORE it writes anything, and
//! the apply set it settles on.
//!
//! Three predictions travel on a bulk plan, all derived by the Control Plane
//! from a scan taken before execution: the matched surrogate set, the implicit
//! edges of the matched documents, and the materialized-sum target resolution.
//! Each is verified the same way — recompute the actual value on the group
//! LEADER, and answer [`ErrorCode::OllpRetryRequired`] without writing on
//! divergence — and each has the same reason for being leader-only: a follower
//! whose redb snapshot lags the leader's prediction window would compute a
//! different answer, poison the attempt, and exhaust the retry budget on a
//! dataset nobody is touching.
//!
//! They live here rather than duplicated in the update and delete handlers
//! because a check that exists in one and not the other is a hole in exactly one
//! statement shape — and because a fourth prediction should have one place to be
//! added.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use nodedb_physical::physical_plan::{OllpPredictedEdge, ResolvedSumTarget, UpdateValue};

/// The predictions one bulk statement carries.
pub(in crate::data::executor) struct BulkAdmission<'a> {
    pub collection: &'a str,
    pub predicted_surrogates: Option<&'a [u32]>,
    pub predicted_edges: Option<&'a [OllpPredictedEdge]>,
    /// The statement's `SET` assignments, so an update that rewrites a
    /// materialized-sum join column is verified against the target it moves rows
    /// ONTO as well. Empty for a delete.
    pub updates: &'a [(String, UpdateValue)],
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Settle the apply set and verify every carried prediction against it.
    ///
    /// Returns the doc-ids to mutate, or the error code the caller must return
    /// WITHOUT writing.
    ///
    /// The apply set is the carried surrogate prediction when there is one, NOT
    /// the local scan: that is the determinism anchor for multi-replica OLLP —
    /// every replica, leader and follower, mutates exactly the leader's verified
    /// set, so all replicas mutate identical state. With no prediction (the
    /// single-shard / non-OLLP path) the local scan stands as the apply set,
    /// unchanged.
    pub(in crate::data::executor) fn admit_bulk_predicate_write(
        &self,
        database_id: u64,
        tid: u64,
        matching_ids: Vec<String>,
        admission: &BulkAdmission<'_>,
    ) -> Result<Vec<String>, ErrorCode> {
        let apply_ids: Vec<String> = match admission.predicted_surrogates {
            Some(predicted) => {
                // The set comparison is deterministic: both sides are sorted.
                if self.ollp_is_group_leader
                    && !super::scan::ollp_surrogates_match(&matching_ids, predicted)
                {
                    return Err(ErrorCode::OllpRetryRequired);
                }
                super::scan::ollp_predicted_doc_ids(predicted)
            }
            None => matching_ids,
        };

        // Edge-content drift. The Control Plane derived the implicit-edge
        // reconciliation (retract the OLD edge, put the NEW one) from the recon
        // scan's `_from`/`_to`/`_type`. If a matched doc's edge fields changed —
        // or an edge appeared or disappeared among the matched docs — since then,
        // the wrong edge would be retracted and a stale one would dangle. The
        // surrogate-set check above cannot see this: the surrogate set is
        // unchanged. This runs BEFORE any write, so `sparse.get` still returns
        // pre-mutation content.
        if let Some(predicted) = admission.predicted_edges
            && self.ollp_is_group_leader
        {
            let actual = self.ollp_actual_edges(database_id, tid, admission.collection, &apply_ids);
            if !super::scan::ollp_edges_match(actual, predicted) {
                return Err(ErrorCode::OllpRetryRequired);
            }
        }

        // Materialized-sum coverage drift. A row that joined the match set since
        // the Control Plane resolved this statement's targets addresses a target
        // the plan holds no surrogate for. The fold's own
        // `MaterializedSumTargetNotFound` fires only once earlier rows are
        // already written, leaving a stored total that disagrees with the
        // `SUM(...)` over the source rows — so the shortfall is caught here,
        // before the first row moves.
        if self.sum_targets_diverged_for_ids(
            &SumTargetCheck {
                database_id,
                tid,
                collection: admission.collection,
                updates: admission.updates,
                resolved: admission.resolved_sum_targets,
            },
            &apply_ids,
        ) {
            return Err(ErrorCode::OllpRetryRequired);
        }

        Ok(apply_ids)
    }

    /// Compute the sorted ACTUAL implicit-edge set for the matched docs.
    ///
    /// For each matched `doc_id`, parse its surrogate (same `len()==8` hex
    /// parse as [`ollp_actual_surrogates`]), fetch the stored doc bytes via the
    /// SAME `sparse.get` path the delete loop uses, decode it, and — only when
    /// it carries BOTH `_from` and `_to` as strings — record an
    /// [`OllpPredictedEdge`] with the raw `_type` as `label`. A matched doc
    /// without both endpoints is not an edge and is skipped; if it gained an
    /// edge after recon it appears here and forces a set mismatch (correct).
    ///
    /// The output is sorted via `OllpPredictedEdge`'s derived `Ord` so it
    /// compares as a plain sorted-slice equality against the Control-Plane-sorted
    /// predicted set. Edge docs are schemaless (`_from`/`_to`), so `decode_document`
    /// (msgpack→JSON) is the field-extraction primitive — no hand-rolled
    /// msgpack. Bytes that don't decode (e.g. a strict Binary Tuple) yield no
    /// edge, matching the schemaless-only scope of implicit edges.
    pub(in crate::data::executor) fn ollp_actual_edges(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        matching_ids: &[String],
    ) -> Vec<OllpPredictedEdge> {
        // `decode_document` returns `serde_json::Value`, whose `get`/`as_str`
        // are inherent methods — no extra trait import needed.
        let mut edges: Vec<OllpPredictedEdge> = Vec::new();
        for doc_id in matching_ids {
            let Some(key) = crate::engine::document::store::StorageKey::parse(doc_id) else {
                continue;
            };
            let Ok(Some(bytes)) = self.sparse.get(database_id, tid, collection, &key) else {
                continue;
            };
            let Ok(doc) = doc_format::decode_document(&bytes) else {
                continue;
            };
            let from = doc.get("_from").and_then(|v| v.as_str());
            let to = doc.get("_to").and_then(|v| v.as_str());
            if let (Some(from), Some(to)) = (from, to) {
                let label = doc
                    .get("_type")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                edges.push(OllpPredictedEdge {
                    surrogate: key.surrogate().as_u32(),
                    from: from.to_string(),
                    to: to.to_string(),
                    label,
                });
            }
        }
        edges.sort_unstable();
        edges
    }
}

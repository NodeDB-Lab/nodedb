// SPDX-License-Identifier: BUSL-1.1

//! Shared parameter structs for join execution handlers.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::JoinProjection;

/// Common join configuration shared across join variants.
pub(crate) struct JoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    pub limit: usize,
    pub projection: &'a [JoinProjection],
    pub computed_projection_bytes: &'a [u8],
    pub join_filter_bytes: &'a [u8],
    pub post_filter_bytes: &'a [u8],
}

/// Hash join: scans both sides from storage or executes resolved child sub-plans.
///
/// When `left_input` or `right_input` is `Some`, the executor runs that sub-plan
/// (e.g. a `ProviderScan` after coordinator resolution) and uses the resulting
/// rows as the corresponding join side. When `None`, the side is scanned locally
/// by `left_collection` / `right_collection`.
///
/// `left_bitmap` / `right_bitmap`, when `Some`, are executed first to build a
/// surrogate prefilter that is injected into the local scan for the corresponding
/// side, pushing the filter into the document engine before any msgpack decode.
pub(crate) struct HashJoinParams<'a> {
    pub join: JoinParams<'a>,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub left_alias: Option<&'a str>,
    pub right_alias: Option<&'a str>,
    /// Resolved child plan for the left side (e.g. `ProviderScan`). `None` =
    /// scan locally by `left_collection`.
    pub left_input: Option<&'a PhysicalPlan>,
    /// Resolved child plan for the right side. Same semantics as `left_input`.
    pub right_input: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the left side. When `Some`, the executor
    /// runs this sub-plan first, collects surrogates, and injects the bitmap
    /// into the left side's scan prefilter.
    pub left_bitmap: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the right side. Same semantics as
    /// `left_bitmap` but applied to the right collection.
    pub right_bitmap: Option<&'a PhysicalPlan>,
}

/// Nested-loop join: O(N×M) fallback for non-equi, theta, and cross joins.
///
/// `condition` is a msgpack-encoded `Vec<ScanFilter>` (empty = cross join).
pub(crate) struct NestedLoopJoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub condition: &'a [u8],
    pub join_type: &'a str,
    pub limit: usize,
}

/// Sort-merge join: O((N+M)·log N) equi-join with optional pre-sorted inputs.
///
/// `on` is a slice of `(left_key, right_key)` column pairs. `pre_sorted`
/// skips the sort phase when the planner guarantees inputs arrive in key order.
pub(crate) struct SortMergeJoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    pub limit: usize,
    pub pre_sorted: bool,
}

// ── Test helpers ─────────────────────────────────────────────────────────────
//
// `filter_and_project` only reads `self.post_filter_bytes` and
// `self.projection`; it never dereferences `self.task`. The unit tests below
// construct a `JoinParams` with a minimal `ExecutionTask` so they can call the
// method directly without a live `CoreLoop`.
#[cfg(test)]
fn make_dummy_task() -> ExecutionTask {
    use crate::bridge::envelope::{PhysicalPlan, Priority};
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;
    use std::time::{Duration, Instant};

    let request = crate::bridge::envelope::Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(0),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "test".into(),
            document_id: "dummy".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        deadline: Instant::now() + Duration::from_secs(30),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ),
    };
    ExecutionTask::new(request)
}

impl JoinParams<'_> {
    /// Apply post-join WHERE filters and projection to result rows.
    ///
    /// Shared tail logic for hash joins and lateral joins:
    /// deserializes post-filter predicates, retains matching rows, then
    /// applies column projection — all on raw msgpack bytes.
    ///
    /// Returns `Ok(())` on success. Returns `Err` when `post_filter_bytes` is
    /// non-empty but fails to deserialize — a corrupt predicate payload must
    /// surface as an error rather than silently skipping the filter and
    /// returning rows that should have been excluded.
    ///
    /// An empty `post_filter_bytes` slice is a no-op and always returns `Ok(())`.
    pub fn filter_and_project(&self, results: &mut Vec<Vec<u8>>) -> crate::Result<()> {
        if !self.post_filter_bytes.is_empty() {
            let filters: Vec<ScanFilter> =
                zerompk::from_msgpack(self.post_filter_bytes).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("decode join post-filters: {e}"),
                    }
                })?;
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        if !self.computed_projection_bytes.is_empty() {
            let computed: Vec<crate::bridge::expr_eval::ComputedColumn> =
                zerompk::from_msgpack(self.computed_projection_bytes).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("decode join computed projection: {e}"),
                    }
                })?;
            for row in results.iter_mut() {
                *row = crate::data::executor::handlers::document::read::projection::apply_projection_msgpack(
                    row,
                    &computed,
                    &[],
                );
            }
        } else if !self.projection.is_empty() {
            for row in results.iter_mut() {
                *row = super::binary_row_project(row, self.projection);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A row containing a msgpack map `{"score": N}` — encoded via json_to_msgpack
    // so the byte layout matches what msgpack_scan::extract_field (used inside
    // matches_binary) expects: standard fixmap/map16/map32 headers, fixstr keys.
    fn row_with_score(score: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"score": score}))
            .expect("encode test row")
    }

    /// A valid msgpack-encoded `Vec<ScanFilter>` that keeps rows whose `score`
    /// field equals `99`.
    fn encode_eq99_filter() -> Vec<u8> {
        use crate::bridge::scan_filter::{FilterOp, ScanFilter};

        let filters = vec![ScanFilter {
            field: "score".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::Integer(99),
            clauses: Vec::new(),
            expr: None,
        }];
        zerompk::to_msgpack_vec(&filters).expect("encode filters")
    }

    // ── Core behaviour ────────────────────────────────────────────────────────

    /// Empty `post_filter_bytes` → `Ok(())` no-op: results unchanged.
    #[test]
    fn empty_post_filter_bytes_is_noop() {
        let task = make_dummy_task();
        let params = JoinParams {
            task: &task,
            on: &[],
            join_type: "inner",
            limit: usize::MAX,
            projection: &[],
            computed_projection_bytes: &[],
            join_filter_bytes: &[],
            post_filter_bytes: &[],
        };
        let mut results = vec![vec![1u8, 2, 3], vec![4u8, 5, 6]];
        assert!(params.filter_and_project(&mut results).is_ok());
        // Results are untouched.
        assert_eq!(results.len(), 2);
    }

    /// Non-empty but corrupt `post_filter_bytes` → `Err`, results NOT silently
    /// left unfiltered. This is the anti-pattern the fix addresses.
    #[test]
    fn corrupt_post_filter_bytes_returns_err_not_silent_noop() {
        let task = make_dummy_task();
        let corrupt: &[u8] = b"\xff\xfe\xfd this is not valid msgpack \x00";
        let params = JoinParams {
            task: &task,
            on: &[],
            join_type: "inner",
            limit: usize::MAX,
            projection: &[],
            computed_projection_bytes: &[],
            join_filter_bytes: &[],
            post_filter_bytes: corrupt,
        };
        let mut results = vec![vec![0u8; 8]]; // would be "leaked" under the old code
        let err = params.filter_and_project(&mut results);
        assert!(
            err.is_err(),
            "corrupt post-filter bytes must return Err, not silently skip the filter"
        );
        // Verify the error identifies the decode failure.
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("decode join post-filters") || msg.contains("serialization"),
            "error message should identify the decode failure, got: {msg}"
        );
    }

    /// Valid encoded filters → matching rows retained, non-matching rows dropped
    /// (happy path unchanged).
    #[test]
    fn valid_post_filter_retains_matching_rows() {
        let task = make_dummy_task();
        let filter_bytes = encode_eq99_filter();
        let params = JoinParams {
            task: &task,
            on: &[],
            join_type: "inner",
            limit: usize::MAX,
            projection: &[],
            computed_projection_bytes: &[],
            join_filter_bytes: &[],
            post_filter_bytes: &filter_bytes,
        };
        let mut results = vec![
            row_with_score(99), // should be kept
            row_with_score(42), // should be dropped
            row_with_score(99), // should be kept
        ];
        assert!(params.filter_and_project(&mut results).is_ok());
        assert_eq!(
            results.len(),
            2,
            "only the two score=99 rows should survive the filter"
        );
    }
}

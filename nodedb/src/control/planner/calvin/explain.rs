// SPDX-License-Identifier: BUSL-1.1

//! Calvin EXPLAIN preamble row generation.
//!
//! Prepends a Calvin-specific preamble row to EXPLAIN output when the task
//! set spans multiple vShards or when best-effort non-atomic dispatch is active.

use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
use crate::control::planner::calvin::dispatch::classify_dispatch;
use crate::control::planner::calvin::types::DispatchClass;
use nodedb_physical::physical_task::PhysicalTask;

/// Generate the Calvin dispatch preamble row for EXPLAIN output.
///
/// Returns `None` when:
/// - The task set is single-shard (standard dispatch, no Calvin preamble needed).
///
/// Returns `Some(String)` with a human-readable preamble for:
/// - `MultiShard` + `Strict` + static path
/// - `MultiShard` + `Strict` + dependent-read (OLLP)
/// - `MultiShard` + `BestEffortNonAtomic`
///
/// The `predicted_read_set_size` parameter is used only for the dependent-read
/// variant; pass `None` for the static path.
pub fn calvin_explain_preamble(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    predicted_read_set_size: Option<usize>,
) -> Option<String> {
    // EXPLAIN classifies by writes alone (no session read-set at plan time).
    let class = classify_dispatch(tasks, &std::collections::BTreeSet::new());
    match class {
        DispatchClass::SingleShard { .. } => None,
        DispatchClass::MultiShard { vshards } => {
            let vshard_list = format!(
                "[{}]",
                vshards
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            let sequencer_note = "epoch: <assigned by sequencer at admission> | \
                                  position: <assigned by sequencer>";

            let preamble = match mode {
                CrossShardTxnMode::Strict => {
                    if let Some(read_set_size) = predicted_read_set_size {
                        format!(
                            "Calvin dependent-read dispatch (OLLP) | vshards: {vshard_list} | \
                             predicted_read_set_size: {read_set_size} | mode: strict | \
                             {sequencer_note}"
                        )
                    } else {
                        format!(
                            "Calvin static dispatch | vshards: {vshard_list} | mode: strict | \
                             {sequencer_note}"
                        )
                    }
                }
                CrossShardTxnMode::BestEffortNonAtomic => {
                    format!(
                        "Best-effort multi-shard dispatch [NON-ATOMIC] | vshards: {vshard_list} | \
                         mode: best_effort_non_atomic | {sequencer_note}"
                    )
                }
            };
            Some(preamble)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TenantId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    fn doc_insert_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(
                    crate::types::DatabaseId::DEFAULT,
                    &format!("col_{vshard}"),
                ),
                document_id: "id1".to_owned(),
                surrogate: nodedb_types::Surrogate::new(1),
                value: vec![],
                if_absent: false,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn scan_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::Scan {
                collection: nodedb_types::QualifiedCollection::new(
                    crate::types::DatabaseId::DEFAULT,
                    &format!("col_{vshard}"),
                ),
                filters: vec![],
                limit: 0,
                offset: 0,
                sort_keys: vec![],
                distinct: false,
                projection: vec![],
                computed_columns: vec![],
                window_functions: vec![],
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn preamble_static_strict_format() {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let preamble = calvin_explain_preamble(&tasks, CrossShardTxnMode::Strict, None)
            .expect("multi-shard should produce preamble");
        assert!(
            preamble.starts_with("Calvin static dispatch"),
            "unexpected preamble: {preamble}"
        );
        assert!(
            preamble.contains("3"),
            "should contain vshard 3: {preamble}"
        );
        assert!(
            preamble.contains("7"),
            "should contain vshard 7: {preamble}"
        );
        assert!(preamble.contains("mode: strict"), "{preamble}");
        assert!(
            preamble.contains("epoch: <assigned by sequencer at admission>"),
            "preamble must include sequencer epoch placeholder: {preamble}"
        );
        assert!(
            preamble.contains("position: <assigned by sequencer>"),
            "preamble must include sequencer position placeholder: {preamble}"
        );
    }

    #[test]
    fn preamble_dependent_strict_format() {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let preamble = calvin_explain_preamble(&tasks, CrossShardTxnMode::Strict, Some(42))
            .expect("multi-shard should produce preamble");
        assert!(
            preamble.starts_with("Calvin dependent-read dispatch (OLLP)"),
            "unexpected preamble: {preamble}"
        );
        assert!(
            preamble.contains("predicted_read_set_size: 42"),
            "{preamble}"
        );
        assert!(preamble.contains("mode: strict"), "{preamble}");
        assert!(
            preamble.contains("epoch: <assigned by sequencer at admission>"),
            "preamble must include epoch placeholder: {preamble}"
        );
    }

    #[test]
    fn preamble_best_effort_marks_non_atomic() {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let preamble =
            calvin_explain_preamble(&tasks, CrossShardTxnMode::BestEffortNonAtomic, None)
                .expect("multi-shard should produce preamble");
        assert!(
            preamble.contains("[NON-ATOMIC]"),
            "best-effort preamble must contain [NON-ATOMIC]: {preamble}"
        );
        assert!(
            preamble.contains("mode: best_effort_non_atomic"),
            "{preamble}"
        );
        assert!(
            preamble.contains("epoch: <assigned by sequencer at admission>"),
            "preamble must include epoch placeholder: {preamble}"
        );
    }

    #[test]
    fn preamble_single_shard_returns_none() {
        let tasks = vec![doc_insert_task(5), scan_task(5)];
        let preamble = calvin_explain_preamble(&tasks, CrossShardTxnMode::Strict, None);
        assert!(
            preamble.is_none(),
            "single-shard should return None, got {preamble:?}"
        );
    }

    #[test]
    fn preamble_all_reads_returns_none() {
        let tasks = vec![scan_task(3), scan_task(7)];
        let preamble = calvin_explain_preamble(&tasks, CrossShardTxnMode::Strict, None);
        assert!(
            preamble.is_none(),
            "all-reads (no writes) should return None, got {preamble:?}"
        );
    }
}

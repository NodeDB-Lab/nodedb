// SPDX-License-Identifier: BUSL-1.1

//! Pin every `AS OF SYSTEM TIME` of one statement under one read cut.
//!
//! The planner lowers `AS OF SYSTEM TIME <ms>` to `SystemTimeScope::AsOf`.
//! Before the statement's tasks leave the Control Plane, this pass takes the
//! statement's read cut once (`read_cut::statement_cap`), sized by the
//! latest cut of the statement, and rewrites every `AsOf` to
//! `SystemTimeScope::Pinned`: the last ordinal of its millisecond, capped at
//! the cut. Every Data Plane read of the statement then reads one cut, on
//! every core and every node. A future time reads the state at the
//! statement and never a later write.
//!
//! A statement inside a transaction block reads the block's snapshot instead
//! ([`pin_txn_snapshot`]): every read that names no system time reads at
//! `SystemTimeScope::TxnSnapshot`, and an explicit `AS OF SYSTEM TIME` pins
//! under the snapshot, never past it.

use nodedb_physical::physical_plan::{GraphReadScope, GraphSystemCut};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::SystemTimeScope;

use crate::control::read_cut::{StatementCap, mirror_cap, statement_cap};
use crate::control::state::SharedState;

use super::sites::{for_each_cut_site, for_each_cut_site_ref};

/// What pinning a statement's cuts did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CutPinning {
    /// True when the read cut capped at least one cut. A capped cut depends
    /// on when the statement ran, so its plan never enters a plan cache. An
    /// uncapped cut reads the same rows whenever it runs.
    pub capped: bool,
}

/// Pin every unpinned `AS OF SYSTEM TIME` cut in `tasks` under one read cut
/// of `state`. A task set with no unpinned cut takes no read cut. The read
/// cut is cheaper when every cut is in the past (`statement_cap`).
pub async fn pin_statement_cuts(
    state: &SharedState,
    tasks: &mut [PhysicalTask],
) -> crate::Result<CutPinning> {
    let latest = tasks
        .iter()
        .filter_map(|task| latest_unpinned_ms(&task.plan))
        .max();
    let Some(latest_as_of_ms) = latest else {
        return Ok(CutPinning::default());
    };
    // A task on a read-only mirror pins under the mirror's cap. Any other task
    // shares one local read cut, taken only if one needs it.
    let mut local = None;
    let mut pinning = CutPinning::default();
    for task in tasks.iter_mut() {
        let cap = match mirror_cap(state, task.database_id)? {
            Some(cap) => cap,
            None => match local {
                Some(cap) => cap,
                None => {
                    let cap = statement_cap(state, latest_as_of_ms).await?;
                    local = Some(cap);
                    cap
                }
            },
        };
        for_each_cut_site(&mut task.plan, &mut |site| {
            pinning.capped |= pin_site(site, cap);
        });
    }
    Ok(pinning)
}

/// Whether `tasks` hold an `AS OF SYSTEM TIME` cut no statement pinned yet.
pub fn holds_unpinned_cut(tasks: &[PhysicalTask]) -> bool {
    tasks
        .iter()
        .any(|task| latest_unpinned_ms(&task.plan).is_some())
}

/// The latest millisecond of a cut in `plan` no statement pinned yet, or
/// `None` when `plan` holds no such cut.
fn latest_unpinned_ms(plan: &nodedb_physical::physical_plan::PhysicalPlan) -> Option<i64> {
    let mut latest: Option<i64> = None;
    for_each_cut_site_ref(plan, &mut |scope| {
        let ms = match *scope {
            SystemTimeScope::AsOf(ms) => Some(ms),
            SystemTimeScope::Current
            | SystemTimeScope::Pinned { .. }
            | SystemTimeScope::TxnSnapshot { .. }
            | SystemTimeScope::AllVersions => None,
        };
        latest = latest.max(ms);
    });
    latest
}

/// Pin one cut under `cap`. Returns true when the cap lowered it.
fn pin_site(scope: &mut SystemTimeScope, cap: StatementCap) -> bool {
    match *scope {
        SystemTimeScope::AsOf(ms) => {
            let (pinned, capped) = pin_ms(ms, cap);
            *scope = pinned;
            capped
        }
        SystemTimeScope::Current
        | SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. }
        | SystemTimeScope::AllVersions => false,
    }
}

/// Pin every read of `tasks` to a transaction block's snapshot, `snapshot`
/// as a cap: its ordinal and the cut marker that fixes it.
///
/// A read that names no system time reads at `TxnSnapshot`, which merges
/// the block's own staged writes. An `AS OF SYSTEM TIME` reads at its own
/// cut, capped at the snapshot, and merges none. An all-versions read keeps
/// its scope.
pub fn pin_txn_snapshot(tasks: &mut [PhysicalTask], snapshot: StatementCap) {
    for task in tasks.iter_mut() {
        pin_plan_txn_snapshot(&mut task.plan, snapshot);
    }
}

/// [`pin_txn_snapshot`] for one plan built outside the SQL planner.
pub fn pin_plan_txn_snapshot(
    plan: &mut nodedb_physical::physical_plan::PhysicalPlan,
    snapshot: StatementCap,
) {
    for_each_cut_site(plan, &mut |site| pin_snapshot_site(site, snapshot));
    pin_graph_snapshot(plan, snapshot);
}

/// Pin every graph read of `plan` and its child plans to the snapshot. A
/// live walk reads the snapshot with the transaction's staged edge writes
/// merged on top. A walk at a cut past the snapshot reads at the snapshot.
fn pin_graph_snapshot(
    plan: &mut nodedb_physical::physical_plan::PhysicalPlan,
    snapshot: StatementCap,
) {
    if let nodedb_physical::physical_plan::PhysicalPlan::Graph(op) = plan
        && let Some(scope) = op.read_scope_mut()
    {
        pin_graph_scope(scope, snapshot);
    }
    for child in crate::control::planner::plan_children::children_mut(plan) {
        pin_graph_snapshot(child, snapshot);
    }
}

/// Pin one graph read scope to the snapshot `snapshot`.
fn pin_graph_scope(scope: &mut GraphReadScope, snapshot: StatementCap) {
    match scope.system {
        GraphSystemCut::Live => {
            scope.system = GraphSystemCut::TxnSnapshot(snapshot.cap);
            scope.cut_marker = snapshot.marker;
        }
        GraphSystemCut::AtOrdinal(ordinal) if ordinal > snapshot.cap => {
            scope.system = GraphSystemCut::AtOrdinal(snapshot.cap);
            scope.cut_marker = snapshot.marker;
        }
        GraphSystemCut::AtOrdinal(_)
        | GraphSystemCut::TxnSnapshot(_)
        | GraphSystemCut::AllVersions => {}
    }
}

/// Pin one read to the snapshot `snapshot`.
fn pin_snapshot_site(scope: &mut SystemTimeScope, snapshot: StatementCap) {
    match *scope {
        SystemTimeScope::Current => {
            *scope = SystemTimeScope::TxnSnapshot {
                ordinal: snapshot.cap,
                cut_marker: snapshot.marker,
            };
        }
        SystemTimeScope::AsOf(ms) => {
            *scope = pin_ms(ms, snapshot).0;
        }
        SystemTimeScope::Pinned { ordinal, .. } if ordinal > snapshot.cap => {
            *scope = SystemTimeScope::Pinned {
                ordinal: snapshot.cap,
                cut_marker: snapshot.marker,
            };
        }
        SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. }
        | SystemTimeScope::AllVersions => {}
    }
}

/// The pinned scope of a cut at `ms` under `cap`, and whether the cap
/// lowered it.
fn pin_ms(ms: i64, cap: StatementCap) -> (SystemTimeScope, bool) {
    let cut = cap.pin(ms);
    let pinned = SystemTimeScope::Pinned {
        ordinal: cut.ordinal,
        cut_marker: cut.marker,
    };
    let capped = cut.ordinal < nodedb_types::ms_to_ordinal_upper(ms).max(0);
    (pinned, capped)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: StatementCap = StatementCap {
        cap: 1_700_000_000_500 * nodedb_types::NANOS_PER_MS + 7,
        marker: 3,
    };

    #[test]
    fn a_past_cut_keeps_its_millisecond_and_takes_the_marker() {
        let mut scope = SystemTimeScope::AsOf(1_700_000_000_000);
        assert!(!pin_site(&mut scope, CAP));
        assert_eq!(
            scope,
            SystemTimeScope::Pinned {
                ordinal: nodedb_types::ms_to_ordinal_upper(1_700_000_000_000),
                cut_marker: 3,
            }
        );
    }

    #[test]
    fn a_future_cut_reads_at_the_cap() {
        let mut scope = SystemTimeScope::AsOf(1_800_000_000_000);
        assert!(pin_site(&mut scope, CAP));
        assert_eq!(
            scope,
            SystemTimeScope::Pinned {
                ordinal: CAP.cap,
                cut_marker: 3,
            }
        );
    }

    /// An array aggregate's cut pins like any scan's: it takes the cap's
    /// ordinal and the marker, so every serving node waits for the cut.
    #[test]
    fn an_array_aggregate_cut_pins_with_the_marker() {
        use nodedb_physical::physical_plan::{ArrayOp, ArrayReducer, PhysicalPlan};
        let mut plan = PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: nodedb_array::types::ArrayId::new(nodedb_types::TenantId::new(1), "a"),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: false,
            shard_bucket: None,
            system_time: SystemTimeScope::AsOf(1_800_000_000_000),
            valid_at_ms: None,
            region_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(latest_unpinned_ms(&plan), Some(1_800_000_000_000));
        let mut capped = false;
        for_each_cut_site(&mut plan, &mut |scope| capped |= pin_site(scope, CAP));
        assert!(capped);
        let PhysicalPlan::Array(ArrayOp::Aggregate { system_time, .. }) = plan else {
            panic!("the plan stays an array aggregate");
        };
        assert_eq!(
            system_time,
            SystemTimeScope::Pinned {
                ordinal: CAP.cap,
                cut_marker: 3,
            }
        );
        assert_eq!(system_time.marked_cut(), Some((CAP.cap, 3)));
    }

    /// A LATERAL join's inner cut pins with the statement's. It sizes the read
    /// cut and takes the marker every serving node waits for.
    #[test]
    fn a_lateral_inner_cut_pins_with_the_marker() {
        use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};
        let mut plan = PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan: Box::new(PhysicalPlan::Query(QueryOp::FacetCounts {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "outer",
                ),
                filters: Vec::new(),
                fields: Vec::new(),
                limit_per_facet: 0,
                system_time: SystemTimeScope::AsOf(1_700_000_000_000),
            })),
            outer_alias: "o".to_string(),
            inner_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "inner",
            ),
            inner_filters: Vec::new(),
            inner_order_by: Vec::new(),
            inner_limit: 1,
            correlation_keys: Vec::new(),
            lateral_alias: "l".to_string(),
            projection: Vec::new(),
            left_join: false,
            inner_system_time: SystemTimeScope::AsOf(1_800_000_000_000),
        });
        assert_eq!(latest_unpinned_ms(&plan), Some(1_800_000_000_000));
        for_each_cut_site(&mut plan, &mut |scope| {
            pin_site(scope, CAP);
        });
        for scope in super::super::plan_scopes(&plan) {
            assert!(
                matches!(scope, SystemTimeScope::Pinned { cut_marker: 3, .. }),
                "{scope:?}"
            );
        }
    }

    #[test]
    fn a_live_pinned_or_audit_scope_stays() {
        for scope in [
            SystemTimeScope::Current,
            SystemTimeScope::AllVersions,
            SystemTimeScope::Pinned {
                ordinal: 9,
                cut_marker: 0,
            },
        ] {
            let mut site = scope;
            assert!(!pin_site(&mut site, CAP));
            assert_eq!(site, scope);
        }
    }

    /// Inside a block, a live read reads the snapshot with its staged writes,
    /// and an `AS OF SYSTEM TIME` reads its own cut under the snapshot.
    #[test]
    fn a_block_read_pins_to_the_snapshot() {
        let mut live = SystemTimeScope::Current;
        pin_snapshot_site(&mut live, CAP);
        assert_eq!(
            live,
            SystemTimeScope::TxnSnapshot {
                ordinal: CAP.cap,
                cut_marker: 3,
            }
        );
        assert_eq!(live.marked_cut(), Some((CAP.cap, 3)));

        let mut past = SystemTimeScope::AsOf(1_700_000_000_000);
        pin_snapshot_site(&mut past, CAP);
        assert_eq!(
            past,
            SystemTimeScope::Pinned {
                ordinal: nodedb_types::ms_to_ordinal_upper(1_700_000_000_000),
                cut_marker: 3,
            }
        );

        let mut future = SystemTimeScope::AsOf(1_800_000_000_000);
        pin_snapshot_site(&mut future, CAP);
        assert_eq!(
            future,
            SystemTimeScope::Pinned {
                ordinal: CAP.cap,
                cut_marker: 3,
            },
            "a time past the snapshot reads at the snapshot"
        );

        let mut audit = SystemTimeScope::AllVersions;
        pin_snapshot_site(&mut audit, CAP);
        assert_eq!(audit, SystemTimeScope::AllVersions);
    }

    /// A graph walk of a block reads the snapshot. A walk at a cut past the
    /// snapshot reads at the snapshot. An earlier cut stays.
    #[test]
    fn a_block_graph_walk_pins_to_the_snapshot() {
        let mut live = GraphReadScope::default();
        pin_graph_scope(&mut live, CAP);
        assert_eq!(live.system, GraphSystemCut::TxnSnapshot(CAP.cap));
        assert_eq!(live.cut_marker, 3);

        let mut past = GraphReadScope::at_installed_cut(5, None);
        pin_graph_scope(&mut past, CAP);
        assert_eq!(past, GraphReadScope::at_installed_cut(5, None));

        let mut future = GraphReadScope::at_installed_cut(CAP.cap + 1, None);
        pin_graph_scope(&mut future, CAP);
        assert_eq!(future.system, GraphSystemCut::AtOrdinal(CAP.cap));
        assert_eq!(future.cut_marker, 3);
    }

    /// Every read of a block's planned statement, a KV get and an aggregate
    /// among them, carries the snapshot.
    #[test]
    fn every_read_of_a_block_statement_carries_the_snapshot() {
        use nodedb_physical::physical_plan::{KvOp, PhysicalPlan, QueryOp};
        let collection =
            nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "c");
        let task = |plan| PhysicalTask {
            tenant_id: nodedb_types::TenantId::new(1),
            database_id: nodedb_types::DatabaseId::DEFAULT,
            vshard_id: nodedb_types::id::VShardId::new(0),
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let mut tasks = vec![
            task(PhysicalPlan::Kv(KvOp::Get {
                collection: collection.clone(),
                key: b"k".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
                system_time: SystemTimeScope::Current,
            })),
            task(PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                input: None,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                having: Vec::new(),
                limit: usize::MAX,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
                grouping_sets: Vec::new(),
                sort_keys: Vec::new(),
                group_key_aliases: Vec::new(),
            })),
        ];
        pin_txn_snapshot(&mut tasks, CAP);
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: CAP.cap,
            cut_marker: 3,
        };
        for task in &tasks {
            assert_eq!(super::super::plan_scopes(&task.plan), vec![snapshot]);
        }
    }

    /// Inside a block a predicate write matches at the snapshot, and every
    /// read that scans a collection by name reads at it: a facet count, a
    /// recursive scan, a LATERAL join's inner side, and a by-name join.
    #[test]
    fn a_block_predicate_write_and_by_name_read_take_the_snapshot() {
        use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, QueryOp};
        let collection =
            || nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "c");
        let current = SystemTimeScope::Current;
        let mut plans = vec![
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection(),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
                system_time: current,
            }),
            PhysicalPlan::Kv(KvOp::PredicateUpdate {
                collection: collection(),
                filters: Vec::new(),
                updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
                system_time: current,
            }),
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: collection(),
                filters: Vec::new(),
                system_from: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                system_time: current,
            }),
            PhysicalPlan::Query(QueryOp::FacetCounts {
                collection: collection(),
                filters: Vec::new(),
                fields: Vec::new(),
                limit_per_facet: 0,
                system_time: current,
            }),
            PhysicalPlan::Query(QueryOp::RecursiveScan {
                collection: collection(),
                base_filters: Vec::new(),
                recursive_filters: Vec::new(),
                join_link: None,
                max_iterations: 1,
                distinct: false,
                limit: 1,
                system_time: current,
            }),
            PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection: collection(),
                right_collection: collection(),
                condition: Vec::new(),
                join_type: "inner".into(),
                limit: 1,
                left_rls_filters: Vec::new(),
                right_rls_filters: Vec::new(),
                left_input: None,
                right_input: None,
                system_time: current,
            }),
        ];
        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: CAP.cap,
            cut_marker: 3,
        };
        for plan in &mut plans {
            pin_plan_txn_snapshot(plan, CAP);
            assert_eq!(super::super::plan_scopes(plan), vec![snapshot], "{plan:?}");
        }
    }
}

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

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::SystemTimeScope;

use crate::control::read_cut::{StatementCap, statement_cap};
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
    let cap = statement_cap(state, latest_as_of_ms).await?;
    let mut pinning = CutPinning::default();
    for task in tasks.iter_mut() {
        for_each_cut_site(&mut task.plan, &mut |site| {
            pinning.capped |= pin_site(site, cap);
        });
    }
    Ok(pinning)
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
        | SystemTimeScope::AllVersions => false,
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
}

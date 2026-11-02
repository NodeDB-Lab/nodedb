// SPDX-License-Identifier: BUSL-1.1

//! Fix the system-time ordinal a write stamps before its WAL record exists.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{CalvinInstall, ColumnarOp, MetaOp};
use nodedb_types::OrdinalClock;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::write_admission::plan_writes_user_data;
use crate::control::wal_replication::columnar_rows;

/// Fix the system-time ordinal `plan`'s write stamps, and return it. The
/// funnel writes it into every WAL record of the write and hands it to the
/// Data Plane apply, so the live apply and every replay of the records stamp
/// the same versions.
///
/// - A columnar insert fixes the run of ordinals its rows stamp, and writes
///   the run's first ordinal into the plan (`ColumnarOp::Insert::system_from`).
///   An UPDATE fixes the one ordinal its new versions stamp
///   (`ColumnarOp::Update::system_from`,
///   `ColumnarOp::ResolvedUpdate::system_from`). A DELETE fixes the one
///   ordinal the tombstones that close its keys stamp
///   (`ColumnarOp::Delete::system_from`,
///   `ColumnarOp::ResolvedDelete::system_from`), and so does a TRUNCATE
///   (`ColumnarOp::Truncate::system_from`). The returned ordinal is the
///   run's first, or the one ordinal.
/// - The install of a Calvin slice stamps the transaction's ordinal, which
///   every replica derives from the epoch instant and position.
/// - Every other write of user data (KV, timeseries, array, document, and
///   the rest) stamps one ordinal for the write.
/// - A plan that writes no user data returns `None`.
///
/// `carried` is the ordinal the leader of the applied replicated entry
/// reserved, which every replica stamps. It folds into `clock`. Without one,
/// the funnel reserves from `clock`.
///
/// The funnel calls it under the write-admission guard, right before it
/// appends the write's WAL record, so a later write to the same key reserves
/// a later ordinal. A plan that already carries a columnar run keeps it.
///
/// The match over [`PhysicalPlan`] is exhaustive (`wildcard_enum_match_arm`
/// is denied), so a future plan variant decides by name how the funnel fixes
/// its ordinal.
pub fn stamp_system_ordinals(
    plan: &mut PhysicalPlan,
    clock: &OrdinalClock,
    carried: Option<i64>,
) -> Option<i64> {
    match plan {
        PhysicalPlan::Columnar(op) => return stamp_columnar_op(op, clock, carried),
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            calvin: Some(install),
            ..
        }) => return Some(calvin_install_ordinal(install, clock)),
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => {}
    }
    if !plan_writes_user_data(plan) {
        return None;
    }
    Some(one_ordinal(clock, carried))
}

/// The one ordinal a write stamps: `carried`, folded into `clock`, else a
/// fresh ordinal of `clock`.
fn one_ordinal(clock: &OrdinalClock, carried: Option<i64>) -> i64 {
    match carried {
        Some(ordinal) => {
            clock.update_from_remote(ordinal);
            ordinal
        }
        None => clock.next_ordinal(),
    }
}

/// The ordinal of the Calvin slice `install` installs, folded into `clock`.
fn calvin_install_ordinal(install: &CalvinInstall, clock: &OrdinalClock) -> i64 {
    let ordinal = nodedb_types::calvin_txn_ordinal(install.epoch_system_ms, install.position);
    clock.update_from_remote(ordinal);
    ordinal
}

fn stamp_columnar_op(
    op: &mut ColumnarOp,
    clock: &OrdinalClock,
    carried: Option<i64>,
) -> Option<i64> {
    match op {
        ColumnarOp::Insert {
            payload,
            surrogates,
            system_from: system_from @ None,
            ..
        } => {
            let rows = columnar_rows(payload, surrogates.len());
            let first = match carried {
                Some(first) => {
                    let last = i64::try_from(rows.saturating_sub(1)).unwrap_or(i64::MAX);
                    clock.update_from_remote(first.saturating_add(last));
                    first
                }
                None => clock.reserve(rows),
            };
            *system_from = Some(first);
            Some(first)
        }
        // An update's new versions, and the tombstones of a delete or a
        // truncate, stamp one ordinal for the statement.
        ColumnarOp::Update {
            system_from: system_from @ None,
            ..
        }
        | ColumnarOp::ResolvedUpdate {
            system_from: system_from @ None,
            ..
        }
        | ColumnarOp::Delete {
            system_from: system_from @ None,
            ..
        }
        | ColumnarOp::ResolvedDelete {
            system_from: system_from @ None,
            ..
        }
        | ColumnarOp::Truncate {
            system_from: system_from @ None,
            ..
        } => {
            let ordinal = one_ordinal(clock, carried);
            *system_from = Some(ordinal);
            Some(ordinal)
        }
        // A plan that carries its ordinal keeps it.
        ColumnarOp::Insert { system_from, .. }
        | ColumnarOp::Update { system_from, .. }
        | ColumnarOp::ResolvedUpdate { system_from, .. }
        | ColumnarOp::Delete { system_from, .. }
        | ColumnarOp::ResolvedDelete { system_from, .. }
        | ColumnarOp::Truncate { system_from, .. } => *system_from,
        ColumnarOp::Scan { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::ColumnarInsertIntent;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, Value};

    use super::*;

    fn insert(
        payload: Vec<u8>,
        surrogates: Vec<Surrogate>,
        system_from: Option<i64>,
    ) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates,
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from,
            restored_versions: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn carried(plan: &PhysicalPlan) -> Option<i64> {
        let PhysicalPlan::Columnar(ColumnarOp::Insert { system_from, .. }) = plan else {
            panic!("not a columnar insert: {plan:?}");
        };
        *system_from
    }

    fn rows(n: usize) -> Vec<u8> {
        let row = Value::Object(std::collections::HashMap::new());
        nodedb_types::value_to_msgpack(&Value::Array(vec![row; n])).expect("encode rows")
    }

    /// An insert reserves one ordinal per row, and the next write stamps
    /// past the run.
    #[test]
    fn an_insert_reserves_one_ordinal_per_row() {
        let clock = OrdinalClock::new();
        let mut plan = insert(rows(3), Vec::new(), None);
        stamp_system_ordinals(&mut plan, &clock, None);
        let first = carried(&plan).expect("stamped");
        assert_eq!(clock.peek(), first + 2);

        let mut by_surrogates = insert(rows(1), vec![Surrogate::new(1), Surrogate::new(2)], None);
        stamp_system_ordinals(&mut by_surrogates, &clock, None);
        let second = carried(&by_surrogates).expect("stamped");
        assert!(second > first + 2, "a later write reserves a later run");
        assert_eq!(clock.peek(), second + 1, "surrogates count the rows");
    }

    /// An update fixes one ordinal for its new versions: the leader's when
    /// the entry carries one, else a fresh one of the clock.
    #[test]
    fn an_update_fixes_one_ordinal() {
        let update = |system_from| {
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
                filters: Vec::new(),
                updates: Vec::new(),
                system_from,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                system_time: nodedb_types::SystemTimeScope::Current,
            })
        };
        let stamp = |plan: &PhysicalPlan| {
            let PhysicalPlan::Columnar(ColumnarOp::Update { system_from, .. }) = plan else {
                panic!("not a columnar update: {plan:?}");
            };
            *system_from
        };
        let clock = OrdinalClock::new();
        let mut local = update(None);
        stamp_system_ordinals(&mut local, &clock, None);
        let fixed = stamp(&local).expect("stamped");
        assert_eq!(clock.peek(), fixed);

        let leader = fixed.saturating_add(3_000_000_000_000);
        let mut replicated = update(None);
        stamp_system_ordinals(&mut replicated, &clock, Some(leader));
        assert_eq!(stamp(&replicated), Some(leader));
        assert_eq!(clock.peek(), leader, "the leader's ordinal folds");

        let mut replayed = update(Some(77));
        stamp_system_ordinals(&mut replayed, &clock, Some(leader));
        assert_eq!(stamp(&replayed), Some(77), "a carried stamp stays");
    }

    /// A delete fixes one ordinal for its tombstones, as an update does.
    #[test]
    fn a_delete_fixes_one_ordinal() {
        let stamp = |plan: &PhysicalPlan| {
            let PhysicalPlan::Columnar(
                ColumnarOp::Delete { system_from, .. }
                | ColumnarOp::ResolvedDelete { system_from, .. },
            ) = plan
            else {
                panic!("not a columnar delete: {plan:?}");
            };
            *system_from
        };
        let clock = OrdinalClock::new();
        let mut local = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            filters: Vec::new(),
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        stamp_system_ordinals(&mut local, &clock, None);
        let fixed = stamp(&local).expect("stamped");
        assert_eq!(clock.peek(), fixed);

        let leader = fixed.saturating_add(2_000_000_000_000);
        let mut replicated = PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            pks: vec![Value::Integer(1)],
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        stamp_system_ordinals(&mut replicated, &clock, Some(leader));
        assert_eq!(stamp(&replicated), Some(leader));
        assert_eq!(clock.peek(), leader, "the leader's ordinal folds");
    }

    /// A replicated insert stamps the run its leader reserved, and the run
    /// folds into the clock, so a later local write stamps past it.
    #[test]
    fn a_replicated_insert_stamps_the_leaders_run() {
        let clock = OrdinalClock::new();
        let leader_first = clock.peek().saturating_add(4_000_000_000_000);
        let mut plan = insert(rows(3), Vec::new(), None);
        stamp_system_ordinals(&mut plan, &clock, Some(leader_first));
        assert_eq!(carried(&plan), Some(leader_first));
        assert_eq!(clock.peek(), leader_first + 2, "the whole run folds");
        assert!(clock.next_ordinal() > leader_first + 2);
    }

    /// A plan that carries a run keeps it: a replayed or replicated record
    /// stamps what its writer stamped.
    #[test]
    fn a_carried_run_stays() {
        let clock = OrdinalClock::new();
        let mut plan = insert(rows(2), Vec::new(), Some(77));
        stamp_system_ordinals(&mut plan, &clock, None);
        assert_eq!(carried(&plan), Some(77));
    }

    fn kv_delete() -> PhysicalPlan {
        PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            keys: vec![b"k".to_vec()],
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    /// A KV write fixes one ordinal: a fresh one of the clock for a local
    /// write, the leader's for a replicated one. A later write fixes a later
    /// one.
    #[test]
    fn a_kv_write_fixes_one_ordinal() {
        let clock = OrdinalClock::new();
        let local = stamp_system_ordinals(&mut kv_delete(), &clock, None).expect("stamped");
        assert_eq!(clock.peek(), local);
        let later = stamp_system_ordinals(&mut kv_delete(), &clock, None).expect("stamped");
        assert!(later > local, "a later write fixes a later ordinal");

        let leader = later.saturating_add(1_000_000_000_000);
        assert_eq!(
            stamp_system_ordinals(&mut kv_delete(), &clock, Some(leader)),
            Some(leader)
        );
        assert_eq!(clock.peek(), leader, "the leader's ordinal folds");
    }

    /// A plan that writes no row stamps no ordinal.
    #[test]
    fn a_schema_change_fixes_no_ordinal() {
        let clock = OrdinalClock::new();
        let mut plan = PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::RegisterIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            field: "f".into(),
            field_position: 0,
            backfill: true,
        });
        assert_eq!(stamp_system_ordinals(&mut plan, &clock, None), None);
    }

    /// A columnar insert answers the first ordinal of its run.
    #[test]
    fn a_columnar_insert_answers_its_runs_first_ordinal() {
        let clock = OrdinalClock::new();
        let mut plan = insert(rows(3), Vec::new(), None);
        let ordinal = stamp_system_ordinals(&mut plan, &clock, None);
        assert_eq!(ordinal, carried(&plan));
        let mut kept = insert(rows(2), Vec::new(), Some(77));
        assert_eq!(stamp_system_ordinals(&mut kept, &clock, None), Some(77));
    }
}

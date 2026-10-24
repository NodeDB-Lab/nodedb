// SPDX-License-Identifier: BUSL-1.1

//! Fix the system-time ordinals a columnar insert, update or delete stamps
//! before its WAL record exists.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::ColumnarOp;
use nodedb_types::OrdinalClock;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::wal_replication::columnar_rows;

/// Fix the run of system-time ordinals a columnar insert's rows stamp, and
/// write the run's first ordinal into the plan
/// (`ColumnarOp::Insert::system_from`). An UPDATE fixes the one ordinal its
/// new versions stamp (`ColumnarOp::Update::system_from`,
/// `ColumnarOp::ResolvedUpdate::system_from`). A DELETE fixes the one
/// ordinal the tombstones that close its keys stamp
/// (`ColumnarOp::Delete::system_from`,
/// `ColumnarOp::ResolvedDelete::system_from`), and so does a TRUNCATE
/// (`ColumnarOp::Truncate::system_from`).
///
/// `carried` is the run's first ordinal when the write applies a replicated
/// entry: the run the entry's leader reserved, which every replica stamps.
/// It folds into `clock`. Without one, the funnel reserves a run of `clock`.
///
/// The funnel calls it under the write-admission guard, right before it
/// appends the write's WAL record, so a later write to the same key reserves
/// a later run and its record carries the run. The live apply and every
/// replay of the record then stamp the same `_ts_system` ordinals. A plan
/// that already carries a run keeps it. A collection that is not bitemporal
/// ignores the run.
///
/// The match over [`PhysicalPlan`] is exhaustive (`wildcard_enum_match_arm`
/// is denied), so a future plan variant that stamps system time decides by
/// name whether the funnel fixes its ordinals.
pub fn stamp_system_ordinals(plan: &mut PhysicalPlan, clock: &OrdinalClock, carried: Option<i64>) {
    match plan {
        PhysicalPlan::Columnar(op) => stamp_columnar_op(op, clock, carried),
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
}

fn stamp_columnar_op(op: &mut ColumnarOp, clock: &OrdinalClock, carried: Option<i64>) {
    match op {
        ColumnarOp::Insert {
            payload,
            surrogates,
            system_from: system_from @ None,
            ..
        } => {
            let rows = columnar_rows(payload, surrogates.len());
            *system_from = Some(match carried {
                Some(first) => {
                    let last = i64::try_from(rows.saturating_sub(1)).unwrap_or(i64::MAX);
                    clock.update_from_remote(first.saturating_add(last));
                    first
                }
                None => clock.reserve(rows),
            });
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
            *system_from = Some(match carried {
                Some(ordinal) => {
                    clock.update_from_remote(ordinal);
                    ordinal
                }
                None => clock.next_ordinal(),
            });
        }
        ColumnarOp::Insert { .. }
        | ColumnarOp::Update { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::Scan { .. }
        | ColumnarOp::Delete { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::Truncate { .. } => {}
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
}

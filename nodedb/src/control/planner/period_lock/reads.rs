// SPDX-License-Identifier: BUSL-1.1

//! The period-lock judgement of a Calvin transaction, made before it is
//! sequenced.
//!
//! A period-locked write names a reference row on another vShard. The install
//! of a Calvin slice does not read that row: each replica would read it at a
//! different point of that vShard's log. The coordinator judges the row here
//! instead. It takes a read cut, reads each row through the gateway, and
//! refuses a write into a closed or unknown period.
//!
//! The transaction then carries one point read per row. The read locks the
//! row `Shared`, so the transaction orders against every Calvin close. The
//! row's vShard joins as a participant and validates the read at the
//! snapshot, so a close committed after the cut aborts with `40001`. The cut
//! comes before the read, so a close between them aborts too.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, ResolvedSumTarget};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{CollectionKey, QualifiedCollection, StorageKey};

use super::gate::period_lock_targets;
use crate::control::planner::calvin::TxnReads;
use crate::control::planner::materialized_sum::join_value_from_body;
use crate::control::planner::materialized_sum::recon::{read_cut, recon_point_row};
use crate::control::security::catalog::PeriodLockDef;
use crate::control::server::shared::session::read_set::{
    EngineTag, ReadKey, ReadPoint, ReadSetEntry,
};
use crate::control::state::SharedState;

/// A transaction's read set with its period-lock reference reads joined.
pub(crate) struct JudgedReads {
    entries: Vec<ReadSetEntry>,
    snapshot_ordinal: i64,
}

impl JudgedReads {
    pub(crate) fn txn_reads(&self) -> TxnReads<'_> {
        TxnReads {
            entries: &self.entries,
            snapshot_ordinal: Some(self.snapshot_ordinal),
        }
    }

    pub(crate) fn entries(&self) -> &[ReadSetEntry] {
        &self.entries
    }
}

/// One period-locked write of a task set.
struct GatedWrite<'a> {
    task: &'a PhysicalTask,
    op: &'a DocumentOp,
    /// The bare name of the written collection.
    collection: String,
    reference: QualifiedCollection,
    def: PeriodLockDef,
    targets: &'a [ResolvedSumTarget],
}

impl GatedWrite<'_> {
    fn period_locked(&self) -> crate::Error {
        crate::Error::PeriodLocked {
            collection: self.collection.clone(),
            detail: "writes rejected".to_owned(),
        }
    }

    /// Refuse a submitted body whose period the plan resolved to no row.
    fn refuse_unknown_periods(&self) -> crate::Result<()> {
        for body in submitted_bodies(self.op) {
            let Some(period) = join_value_from_body(body, &self.def.period_column) else {
                continue;
            };
            let known = self
                .targets
                .iter()
                .any(|target| target.addresses(&self.def.ref_table, &period));
            if !known {
                return Err(self.period_locked());
            }
        }
        Ok(())
    }

    /// The targets of this write that name a reference row. A target of
    /// another collection is a materialized-sum target.
    fn reference_targets(&self) -> impl Iterator<Item = &ResolvedSumTarget> {
        self.targets
            .iter()
            .filter(|target| target.target_collection == self.def.ref_table)
    }

    fn reference_read(&self, target: &ResolvedSumTarget) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: self.task.database_id,
            tenant_id: self.task.tenant_id,
            collection: self.reference.as_str().to_owned(),
            key: ReadKey::Point {
                repr: ReadPoint::Surrogate(target.surrogate.as_u32()),
            },
            observed_at: None,
            home: None,
        }
    }

    /// Refuse the write when the reference row of `target` is absent or its
    /// status is not an allowed one.
    async fn judge_reference_row(
        &self,
        state: &SharedState,
        target: &ResolvedSumTarget,
    ) -> crate::Result<()> {
        let row = recon_point_row(
            state,
            self.task.tenant_id,
            self.task.database_id,
            None,
            self.reference.as_str(),
            &target.join_value,
            target.surrogate,
        )
        .await?;
        let Some(row) = row else {
            return Err(self.period_locked());
        };
        let Some(status) = row
            .get(self.def.status_column.as_str())
            .and_then(|status| status.as_str())
        else {
            return Err(crate::Error::PeriodLockMisconfigured {
                collection: self.collection.clone(),
                ref_table: self.def.ref_table.clone(),
                status_column: self.def.status_column.clone(),
                row_identity: StorageKey::for_surrogate(target.surrogate)
                    .to_identity()
                    .to_string(),
            });
        };
        if self
            .def
            .allowed_statuses
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(status))
        {
            Ok(())
        } else {
            Err(self.period_locked())
        }
    }
}

/// The row bodies `op` submits. An op that names its rows by key or predicate
/// submits none: its period values come off the stored rows.
fn submitted_bodies(op: &DocumentOp) -> Vec<&[u8]> {
    match op {
        DocumentOp::PointPut { value, .. }
        | DocumentOp::PointInsert { value, .. }
        | DocumentOp::Upsert { value, .. } => vec![value.as_slice()],
        DocumentOp::BatchInsert { documents, .. } | DocumentOp::BatchUpsert { documents, .. } => {
            documents.iter().map(|(_, body)| body.as_slice()).collect()
        }
        _ => Vec::new(),
    }
}

/// The period-locked writes of `tasks`.
fn gated_writes<'a>(
    state: &SharedState,
    tasks: &'a [PhysicalTask],
) -> crate::Result<Vec<GatedWrite<'a>>> {
    let catalog = state.credentials.catalog();
    let mut gated = Vec::new();
    for task in tasks {
        let PhysicalPlan::Document(op) = &task.plan else {
            continue;
        };
        let Some((collection, targets)) = period_lock_targets(op) else {
            continue;
        };
        let source = CollectionKey::from_qualified_str(task.database_id, collection)?;
        let Some(def) = catalog
            .get_collection(task.database_id, task.tenant_id.as_u64(), source.name())?
            .and_then(|coll| coll.period_lock)
        else {
            continue;
        };
        gated.push(GatedWrite {
            task,
            op,
            collection: source.name().to_owned(),
            reference: QualifiedCollection::new(task.database_id, &def.ref_table),
            def,
            targets,
        });
    }
    Ok(gated)
}

/// Judge the period-locked writes of `tasks` and join one point read per
/// reference row to `reads`. `None` when no read joins.
///
/// A transaction with no snapshot takes a read cut, and the cut becomes its
/// snapshot. A row two writes name is judged and read once.
pub(crate) async fn judge_period_references(
    state: &SharedState,
    tasks: &[PhysicalTask],
    reads: TxnReads<'_>,
) -> crate::Result<Option<JudgedReads>> {
    let gated = gated_writes(state, tasks)?;
    let mut rows: Vec<(&GatedWrite<'_>, &ResolvedSumTarget, ReadSetEntry)> = Vec::new();
    for write in &gated {
        write.refuse_unknown_periods()?;
        for target in write.reference_targets() {
            let read = write.reference_read(target);
            if !rows.iter().any(|(_, _, seen)| *seen == read) {
                rows.push((write, target, read));
            }
        }
    }
    if rows.is_empty() {
        return Ok(None);
    }
    let snapshot_ordinal = match reads.snapshot_ordinal {
        Some(ordinal) => ordinal,
        None => read_cut(state).await?,
    };
    let mut entries = reads.entries.to_vec();
    for (write, target, read) in rows {
        write.judge_reference_row(state, target).await?;
        entries.push(ReadSetEntry {
            observed_at: Some(snapshot_ordinal),
            ..read
        });
    }
    Ok(Some(JudgedReads {
        entries,
        snapshot_ordinal,
    }))
}

// SPDX-License-Identifier: BUSL-1.1

//! Attaching a planned DML statement's `RETURNING` spec to its plans.
//!
//! Every engine carries the clause on its write ops: document (schemaless
//! and strict), key-value, columnar, spatial, timeseries, and
//! vector-primary each own a `returning` slot paired with an `rls_filters`
//! read gate, so the statement returns the stored post-image bounded by the
//! read policy. An `INSERT ... SELECT` carries it too, and answers every
//! copied row.

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_physical::physical_task::PhysicalTask;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;

/// Attach `spec` to every planned task. A plan with no slot is left
/// unchanged.
pub fn attach_returning_spec(tasks: &mut [PhysicalTask], spec: &ReturningSpec) {
    for task in tasks.iter_mut() {
        inject_returning_spec(&mut task.plan, spec.clone());
    }
}

/// The error for a row-returning write that its transaction buffers for
/// COMMIT or stages with no row image, so it has no row to answer at the
/// statement. A Document point write, `UPSERT`, predicate `UPDATE` /
/// `DELETE`, and the point writes an `INSERT ... SELECT`, `UPDATE ... FROM`
/// or `MERGE` expands to answer from the rows they stage. COMMIT answers one
/// tag for the whole transaction, so the rows of any other write cannot be
/// answered later either, and the statement is refused.
pub fn in_transaction_returning_unsupported() -> Error {
    Error::BadRequest {
        detail: "RETURNING is not supported inside an explicit transaction: the write is staged \
                 or buffered until COMMIT, so it has no stored row to project at this point. Run \
                 the statement in autocommit, or follow the write with a SELECT after COMMIT."
            .to_string(),
    }
}

/// Inject a RETURNING spec into the `returning` slot of a DML physical plan.
///
/// A plan whose op has no slot is left unchanged. The slotted ops are listed
/// once, in `PhysicalPlan::returning_slot_mut`.
pub fn inject_returning_spec(plan: &mut PhysicalPlan, spec: ReturningSpec) {
    if let Some(slot) = plan.returning_slot_mut() {
        *slot = Some(spec);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, ReturningColumns};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    /// An `INSERT ... SELECT` carries the clause, so the attach step places
    /// it on the plan instead of refusing the statement.
    #[test]
    fn an_insert_select_plan_carries_the_clause() {
        let mut plan = PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "dst"),
            source: nodedb_physical::physical_plan::RowSource::whole_collection(
                QualifiedCollection::new(DatabaseId::DEFAULT, "src"),
            ),
            returning: None,
            rls_filters: Vec::new(),
        });
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        };
        inject_returning_spec(&mut plan, spec.clone());
        assert_eq!(
            plan.returning_target().map(|(_, carried)| carried.clone()),
            Some(spec)
        );
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! What a settled request needs from its route, and which requests no route
//! can settle.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::edge_truncate::is_truncate;
use super::is_dependent_predicate;
use crate::Error;
use crate::control::server::shared::session::read_set::{ReadKey, ReadPoint, ReadSetEntry};

/// The refusal of a request in which a predicate write or a TRUNCATE moves one
/// balance beside another write. The wire tests match this text.
pub const SEVERAL_WRITES_REFUSAL: &str = "several writes of one request move the same \
materialized-sum balance and one is a predicate write or a TRUNCATE; run them as separate \
statements.";

/// The refusal of a request that writes one settled source row twice. The wire
/// tests match this text.
pub const SAME_ROW_WRITES_REFUSAL: &str = "several writes of one request rewrite the same source \
row of a materialized sum; run them as separate statements.";

/// The refusal of a Calvin task set with several `RETURNING` tasks.
pub const SEVERAL_RETURNING_REFUSAL: &str = "several writes of one request carry RETURNING and \
commit through one Calvin transaction, which answers one write's rows; run them as separate \
statements.";

/// Refuse a Calvin task set with several `RETURNING` tasks. One transaction
/// answers one set of rows: a slice keeps its last task's, and two slices with
/// rows are a union no shape holds. Refused before anything commits, so no
/// client gets one write's rows in place of two.
pub fn refuse_several_returning_tasks(tasks: &[PhysicalTask]) -> crate::Result<()> {
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};
    let returning = tasks
        .iter()
        .filter(|task| matches!(describe_plan(&task.plan), PlanKind::ReturningRows))
        .count();
    if returning > 1 {
        return Err(Error::FeatureNotSupported {
            detail: format!("{SEVERAL_RETURNING_REFUSAL} RETURNING writes: {returning}."),
        });
    }
    Ok(())
}

/// Refuse a task set that settles one source row twice.
///
/// Every write of a task set settles before any write stages, so a later write
/// of a row folds its delta from an image the earlier write changes. `reads`
/// names each settled point write by its source row, so a row that ships no
/// delta does not count. Two writes of different rows each fold their own image
/// and pass. The rows of one `BatchUpsert` page settle the same way, so a page
/// that names a row twice is refused too.
pub fn refuse_repeated_settled_rows(reads: &[ReadSetEntry]) -> crate::Result<()> {
    let mut settled_rows = BTreeSet::new();
    for read in reads {
        if let ReadKey::Point {
            repr: ReadPoint::Surrogate(surrogate),
        } = &read.key
            && !settled_rows.insert((read.collection.as_str(), *surrogate))
        {
            return Err(Error::FeatureNotSupported {
                detail: format!(
                    "{SAME_ROW_WRITES_REFUSAL} Collection: {}, row surrogate: {surrogate}.",
                    read.collection
                ),
            });
        }
    }
    Ok(())
}

/// Refuse a request in which a predicate write or a TRUNCATE moves one balance
/// beside another write.
///
/// A request plans as one task set, and every write settles before any write
/// stages. So a later write folds its delta from rows the earlier write changes,
/// and the balance drifts from the source sum. A transaction block does not
/// help: its overlay holds neither write yet. A balance is one column of a target
/// row, so two sum columns of one row pass.
pub fn refuse_several_settling_writes(tasks: &[PhysicalTask]) -> crate::Result<()> {
    if !tasks
        .iter()
        .any(|task| is_dependent_predicate(&task.plan) || is_truncate(&task.plan))
    {
        return Ok(());
    }
    let mut balances = BTreeSet::new();
    for task in tasks {
        if let PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection,
            document_id,
            column,
            ..
        }) = &task.plan
            && !balances.insert((collection.as_str(), document_id.as_str(), column.as_str()))
        {
            return Err(Error::FeatureNotSupported {
                detail: format!(
                    "{SEVERAL_WRITES_REFUSAL} Balance: column '{column}' of target '{collection}'."
                ),
            });
        }
    }
    Ok(())
}

/// Whether a settled request ships a materialized-sum balance delta, or holds
/// the source reads its deltas were folded from. Such a request commits through
/// the deterministic scheduler. A gateway forward applies tasks one by one and
/// checks no read.
pub fn ships_balance_delta(tasks: &[PhysicalTask], reads: &[ReadSetEntry]) -> bool {
    !reads.is_empty()
        || tasks.iter().any(|task| {
            matches!(
                task.plan,
                PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
            )
        })
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_task::PostSetOp;

    use super::*;
    use crate::control::server::shared::session::read_set::EngineTag;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn truncate() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "postings"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }))
    }

    fn balance_delta() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "0a".to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            column: "balance".to_owned(),
            delta: "5".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc".to_owned(),
            declared_primary_key: None,
        }))
    }

    fn bulk_delete() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "postings"),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        }))
    }

    fn fees_delta() -> PhysicalTask {
        task(PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "0a".to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            column: "fees".to_owned(),
            delta: "5".to_owned(),
            join_column: "account_id".to_owned(),
            join_value: "acc".to_owned(),
            declared_primary_key: None,
        }))
    }

    fn row_read(surrogate: u32) -> ReadSetEntry {
        ReadSetEntry {
            key: ReadKey::Point {
                repr: ReadPoint::Surrogate(surrogate),
            },
            ..settlement_read()
        }
    }

    fn message(result: crate::Result<()>) -> String {
        match result {
            Err(Error::FeatureNotSupported { detail }) => detail,
            other => panic!("a FeatureNotSupported refusal, got {other:?}"),
        }
    }

    #[test]
    fn two_deltas_of_one_balance_beside_a_predicate_write_are_refused() {
        let tasks = [bulk_delete(), balance_delta(), balance_delta()];
        assert!(message(refuse_several_settling_writes(&tasks)).contains(SEVERAL_WRITES_REFUSAL));
    }

    #[test]
    fn two_deltas_of_one_balance_beside_a_truncate_are_refused() {
        let tasks = [truncate(), balance_delta(), balance_delta()];
        assert!(message(refuse_several_settling_writes(&tasks)).contains(SEVERAL_WRITES_REFUSAL));
    }

    #[test]
    fn two_sum_columns_of_one_row_are_not_refused() {
        let tasks = [bulk_delete(), balance_delta(), fees_delta()];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }

    #[test]
    fn two_inserts_of_one_balance_are_not_refused() {
        let tasks = [balance_delta(), balance_delta()];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }

    #[test]
    fn plain_predicate_writes_are_not_refused() {
        let tasks = [bulk_delete(), bulk_delete()];
        assert!(refuse_several_settling_writes(&tasks).is_ok());
    }

    #[test]
    fn one_returning_task_is_not_refused() {
        assert!(refuse_several_returning_tasks(&[bulk_delete()]).is_ok());
    }

    #[test]
    fn a_row_settled_twice_is_refused() {
        let reads = [row_read(7), row_read(7)];
        assert!(message(refuse_repeated_settled_rows(&reads)).contains(SAME_ROW_WRITES_REFUSAL));
    }

    #[test]
    fn two_settled_rows_are_not_refused() {
        let reads = [row_read(7), row_read(8)];
        assert!(refuse_repeated_settled_rows(&reads).is_ok());
    }

    #[test]
    fn two_predicate_reads_of_one_collection_are_not_refused() {
        let reads = [settlement_read(), settlement_read()];
        assert!(refuse_repeated_settled_rows(&reads).is_ok());
    }

    fn settlement_read() -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "postings".to_owned(),
            key: ReadKey::Predicate,
            observed_at: Some(42),
            home: None,
        }
    }

    #[test]
    fn a_shipped_delta_needs_the_scheduler() {
        assert!(ships_balance_delta(&[truncate(), balance_delta()], &[]));
    }

    #[test]
    fn a_zero_net_settlement_ships_no_task_and_still_needs_the_scheduler() {
        assert!(ships_balance_delta(&[truncate()], &[settlement_read()]));
    }

    #[test]
    fn an_unsettled_request_can_forward() {
        assert!(!ships_balance_delta(&[truncate()], &[]));
    }
}

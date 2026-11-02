// SPDX-License-Identifier: BUSL-1.1

//! The per-statement `AS OF SYSTEM TIME` pin of [`QueryContext`].

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::types::PlanCacheEligibility;

use super::QueryContext;
use crate::control::planner::sql_plan_convert::PlanningPurpose;
use crate::control::planner::system_cut::{pin_statement_cuts, pin_txn_snapshot};
use crate::control::version_cleanup::check_history_window;

impl QueryContext {
    /// Pin every `AS OF SYSTEM TIME` cut of one statement's `tasks` under
    /// one read cut, and return the statement's cache verdict: a capped cut
    /// depends on when the statement ran, so the statement is
    /// `DataDependent`.
    ///
    /// Inside a transaction block every read reads the block's snapshot
    /// instead: a read that names no system time at `TxnSnapshot`, an
    /// `AS OF SYSTEM TIME` under the snapshot. The plan then depends on the
    /// block, so it is `DataDependent`.
    ///
    /// A read of a collection that is not bitemporal at a cut older than the
    /// snapshot age limit is refused: the cleanup horizon purges its history
    /// (`version_cleanup::check_history_window`).
    ///
    /// Metadata tasks are never dispatched and take no cut. A context with
    /// no booted node leaves the cuts unpinned. The Data Plane then reads
    /// each at the last ordinal of its millisecond.
    pub(super) async fn pin_statement_cut(
        &self,
        tasks: &mut [PhysicalTask],
        purpose: PlanningPurpose,
        eligibility: PlanCacheEligibility,
    ) -> crate::Result<PlanCacheEligibility> {
        if purpose == PlanningPurpose::Metadata {
            return Ok(eligibility);
        }
        let state = self.cut_state.as_ref().and_then(|weak| weak.upgrade());
        if let Some(snapshot) = self.txn_snapshot() {
            pin_txn_snapshot(tasks, snapshot.statement_cap());
            if let Some(state) = &state {
                check_history_window(state, tasks)?;
            }
            return Ok(PlanCacheEligibility::DataDependent);
        }
        let Some(state) = state else {
            return Ok(eligibility);
        };
        let pinning = pin_statement_cuts(&state, tasks).await?;
        // A cut on a collection whose history the cleanup horizon purges
        // reads one answer only while it is recent.
        let time_bound = check_history_window(&state, tasks)?;
        Ok(if pinning.capped || time_bound {
            PlanCacheEligibility::DataDependent
        } else {
            eligibility
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::SystemTimeScope;

    use crate::control::server::shared::session::TxnSnapshotCut;

    fn scan_task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: crate::types::TenantId::new(1),
            database_id: crate::types::DatabaseId::DEFAULT,
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::Scan {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                limit: usize::MAX,
                offset: 0,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn scope_of(task: &PhysicalTask) -> Vec<SystemTimeScope> {
        crate::control::planner::system_cut::plan_scopes(&task.plan)
    }

    /// A SELECT planned inside a transaction block reads the block's
    /// snapshot, and its plan never enters a plan cache. Outside a block it
    /// reads the current state.
    #[tokio::test]
    async fn a_block_select_reads_the_txn_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = crate::control::state::SharedState::new(dispatcher, wal).expect("state");
        let ctx = QueryContext::for_state(&state);

        let mut autocommit = vec![scan_task()];
        let verdict = ctx
            .pin_statement_cut(
                &mut autocommit,
                PlanningPurpose::Execute,
                PlanCacheEligibility::Cacheable,
            )
            .await
            .expect("pin");
        assert_eq!(verdict, PlanCacheEligibility::Cacheable);
        assert_eq!(scope_of(&autocommit[0]), vec![SystemTimeScope::Current]);

        let snapshot = TxnSnapshotCut {
            ordinal: 42,
            cut_marker: 0,
        };
        ctx.set_txn_snapshot(Some(snapshot));
        let mut in_block = vec![scan_task()];
        let verdict = ctx
            .pin_statement_cut(
                &mut in_block,
                PlanningPurpose::Execute,
                PlanCacheEligibility::Cacheable,
            )
            .await
            .expect("pin");
        assert_eq!(verdict, PlanCacheEligibility::DataDependent);
        assert_eq!(scope_of(&in_block[0]), vec![snapshot.scope()]);

        let mut metadata = vec![scan_task()];
        ctx.pin_statement_cut(
            &mut metadata,
            PlanningPurpose::Metadata,
            PlanCacheEligibility::Cacheable,
        )
        .await
        .expect("pin");
        assert_eq!(scope_of(&metadata[0]), vec![SystemTimeScope::Current]);
        ctx.set_txn_snapshot(None);
    }
}

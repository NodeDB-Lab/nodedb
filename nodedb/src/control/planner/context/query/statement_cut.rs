// SPDX-License-Identifier: BUSL-1.1

//! The per-statement `AS OF SYSTEM TIME` pin of [`QueryContext`].

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::types::PlanCacheEligibility;

use super::QueryContext;
use crate::control::planner::sql_plan_convert::PlanningPurpose;
use crate::control::planner::system_cut::pin_statement_cuts;

impl QueryContext {
    /// Pin every `AS OF SYSTEM TIME` cut of one statement's `tasks` under
    /// one read cut, and return the statement's cache verdict: a capped cut
    /// depends on when the statement ran, so the statement is
    /// `DataDependent`.
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
        let Some(state) = self.cut_state.as_ref().and_then(|weak| weak.upgrade()) else {
            return Ok(eligibility);
        };
        let pinning = pin_statement_cuts(&state, tasks).await?;
        Ok(if pinning.capped {
            PlanCacheEligibility::DataDependent
        } else {
            eligibility
        })
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop::execute` / `execute_plan`: matches on `PhysicalPlan` variant
//! and delegates to the appropriate per-engine sub-dispatcher.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::super::core_loop::CoreLoop;
use super::super::task::ExecutionTask;
use super::visitor;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate sub-dispatcher.
    ///
    /// A write task carries the system-time ordinal its WAL records carry:
    /// the one the leader of the applied replicated entry assigned, or the
    /// one the local append reserved. It scopes the apply
    /// (`ApplyScope::replicated_ordinal`) and folds into the shared clock, so
    /// the write's versions take it, a replay of its records stamps the same
    /// versions, and a later local write stamps above it.
    ///
    /// A fresh write that loads a vector or R-tree index under emergency
    /// pressure is refused before it changes anything. Every write recharges
    /// its tenant's index charges before it answers, so the governor holds
    /// each index's exact bytes after every write. A task whose install
    /// parked recharges again when the install ends.
    #[cfg(test)]
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        self.execute_with_journal(task, None)
    }

    pub(in crate::data::executor) fn execute_with_journal(
        &mut self,
        task: &ExecutionTask,
        permit: Option<RootJournalPermit>,
    ) -> Response {
        if let Some(refusal) = self.index_pressure_refusal(task) {
            return refusal;
        }
        let carried = task.request.system_ordinal;
        if let Some(ordinal) = carried {
            self.observe_bitemporal_ordinal(ordinal);
        }
        let previous = std::mem::replace(&mut self.apply_scope.replicated_ordinal, carried);
        // A task that carries a WAL LSN applies a journaled record.
        let journaled = self.apply_scope.journaled || task.wal_lsn.is_some();
        let previous_journaled = std::mem::replace(&mut self.apply_scope.journaled, journaled);
        let previous_schemas = std::mem::take(self.apply_scope.storage_schemas.get_mut());
        let response = self.execute_plan_with_journal(task, task.plan(), permit);
        *self.apply_scope.storage_schemas.get_mut() = previous_schemas;
        self.apply_scope.journaled = previous_journaled;
        self.apply_scope.replicated_ordinal = previous;
        self.recharge_written_tenant(task);
        response
    }

    /// End the execution of `task`, whose install parked inside it and
    /// answered in a later tick. The apply scope stands as it stands
    /// between tasks, and the tenant's index charges follow every write
    /// the install applied.
    pub(in crate::data::executor) fn end_parked_execution(&mut self, task: &ExecutionTask) {
        self.apply_scope = Default::default();
        self.recharge_written_tenant(task);
    }

    /// Recharge the index charges of `task`'s tenant, unless the task is
    /// exempt as a read.
    fn recharge_written_tenant(&mut self, task: &ExecutionTask) {
        if !task.request.admission.is_exempt_as_read() {
            self.recharge_tenant_indexes(task.request.database_id, task.request.tenant_id);
        }
    }

    /// Execute an arbitrary physical plan (used for inline sub-plans in multi-way joins).
    pub(in crate::data::executor) fn execute_plan(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
    ) -> Response {
        self.execute_plan_with_journal(task, plan, None)
    }

    fn execute_plan_with_journal(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
        permit: Option<RootJournalPermit>,
    ) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let mut v = visitor::DataPlaneVisitor {
            core_loop: self,
            task,
            tid,
            permit,
        };
        match nodedb_physical::dispatch(&mut v, plan) {
            Ok(response) => response,
            Err(never) => match never {},
        }
    }
}

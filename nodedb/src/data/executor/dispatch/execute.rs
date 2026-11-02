// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop::execute` / `execute_plan`: matches on `PhysicalPlan` variant
//! and delegates to the appropriate per-engine sub-dispatcher.

use crate::bridge::envelope::Response;
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
    /// each index's exact bytes after every write.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        if let Some(refusal) = self.index_pressure_refusal(task) {
            return refusal;
        }
        let carried = task.request.system_ordinal;
        if let Some(ordinal) = carried {
            self.observe_bitemporal_ordinal(ordinal);
        }
        let previous = std::mem::replace(&mut self.apply_scope.replicated_ordinal, carried);
        let response = self.execute_plan(task, task.plan());
        self.apply_scope.replicated_ordinal = previous;
        if !task.request.admission.is_exempt_as_read() {
            self.recharge_tenant_indexes(task.request.database_id, task.request.tenant_id);
        }
        response
    }

    /// Execute an arbitrary physical plan (used for inline sub-plans in multi-way joins).
    pub(in crate::data::executor) fn execute_plan(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
    ) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let mut v = visitor::DataPlaneVisitor {
            core_loop: self,
            task,
            tid,
        };
        match nodedb_physical::dispatch(&mut v, plan) {
            Ok(response) => response,
            Err(never) => match never {},
        }
    }
}

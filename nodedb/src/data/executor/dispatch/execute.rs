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
    /// A task that applies a replicated data-group entry carries the
    /// system-time ordinal the entry's leader assigned. It scopes the apply
    /// (`ApplyScope::replicated_ordinal`) and folds into the shared clock, so
    /// the write's bitemporal versions take it and a later local write
    /// stamps above it.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        let carried = task
            .request
            .entry_version
            .and_then(|entry| entry.system_ordinal);
        if let Some(ordinal) = carried {
            self.observe_bitemporal_ordinal(ordinal);
        }
        let previous = std::mem::replace(&mut self.apply_scope.replicated_ordinal, carried);
        let response = self.execute_plan(task, task.plan());
        self.apply_scope.replicated_ordinal = previous;
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

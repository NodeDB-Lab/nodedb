//! Main execute() dispatch: matches on PhysicalPlan variant and delegates
//! to the appropriate sub-dispatcher.

pub mod crdt;
pub mod document;
pub mod graph;
pub mod meta_retention;
pub mod other;
pub mod text;
pub mod vector;

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::PhysicalPlan;

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate sub-dispatcher.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        self.execute_plan(task, task.plan())
    }

    /// Execute an arbitrary physical plan (used for inline sub-plans in multi-way joins).
    pub(in crate::data::executor) fn execute_plan(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
    ) -> Response {
        match plan {
            PhysicalPlan::Document(op) => self.dispatch_document(task, op),
            PhysicalPlan::Vector(op) => self.dispatch_vector(task, op),
            PhysicalPlan::Crdt(op) => self.dispatch_crdt(task, op),
            PhysicalPlan::Graph(op) => self.dispatch_graph(task, op),
            PhysicalPlan::Text(op) => self.dispatch_text(task, op),
            plan => self.dispatch_other(task, plan),
        }
    }
}

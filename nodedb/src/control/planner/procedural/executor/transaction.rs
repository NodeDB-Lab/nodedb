//! Procedure transaction context: buffers DML tasks for COMMIT/ROLLBACK/SAVEPOINT.
//!
//! Stored procedures can execute COMMIT mid-body to finalize buffered DML,
//! ROLLBACK to discard it, and SAVEPOINT/ROLLBACK TO for partial rollback.
//!
//! Triggers do NOT use this — they dispatch DML immediately.

use crate::control::planner::physical::PhysicalTask;

/// Buffered transaction context for stored procedure execution.
///
/// DML statements inside a procedure body are collected here until
/// an explicit COMMIT flushes them as a TransactionBatch, or ROLLBACK
/// discards them. An implicit COMMIT occurs at the end of the procedure.
#[derive(Default)]
pub struct ProcedureTransactionCtx {
    /// Buffered DML tasks awaiting COMMIT.
    buffer: Vec<PhysicalTask>,
    /// Savepoint stack: (name, buffer_position_at_savepoint_time).
    savepoints: Vec<(String, usize)>,
}

impl ProcedureTransactionCtx {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            savepoints: Vec::new(),
        }
    }

    /// Buffer a DML task for later COMMIT.
    pub fn buffer_task(&mut self, task: PhysicalTask) {
        self.buffer.push(task);
    }

    /// Take all buffered tasks (on COMMIT). Clears the buffer and savepoint stack.
    pub fn take_buffered_tasks(&mut self) -> Vec<PhysicalTask> {
        self.savepoints.clear();
        std::mem::take(&mut self.buffer)
    }

    /// Discard all buffered tasks (on ROLLBACK). Clears the savepoint stack.
    pub fn rollback(&mut self) {
        self.buffer.clear();
        self.savepoints.clear();
    }

    /// Record a savepoint at the current buffer position.
    pub fn savepoint(&mut self, name: &str) {
        let pos = self.buffer.len();
        // Remove any existing savepoint with the same name (redefine).
        self.savepoints.retain(|(n, _)| n != name);
        self.savepoints.push((name.to_string(), pos));
    }

    /// Rollback to a named savepoint: discard tasks buffered after it.
    pub fn rollback_to(&mut self, name: &str) -> crate::Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, p)| *p);

        match pos {
            Some(p) => {
                self.buffer.truncate(p);
                // Remove savepoints created after this one.
                if let Some(idx) = self.savepoints.iter().position(|(n, _)| n == name) {
                    self.savepoints.truncate(idx + 1);
                }
                Ok(())
            }
            None => Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            }),
        }
    }

    /// Release a savepoint without rolling back (keeps buffered tasks).
    pub fn release_savepoint(&mut self, name: &str) -> crate::Result<()> {
        let existed = self.savepoints.iter().any(|(n, _)| n == name);
        if !existed {
            return Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            });
        }
        self.savepoints.retain(|(n, _)| n != name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::bridge::physical_plan::DocumentOp;
    use crate::control::planner::physical::PostSetOp;
    use crate::types::{TenantId, VShardId};

    fn dummy_task(id: &str) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "test".into(),
                document_id: id.into(),
                value: vec![],
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
        }
    }

    #[test]
    fn buffer_and_take() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.buffer_task(dummy_task("b"));
        let tasks = ctx.take_buffered_tasks();
        assert_eq!(tasks.len(), 2);
        assert!(ctx.take_buffered_tasks().is_empty());
    }

    #[test]
    fn rollback_clears_buffer() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.rollback();
        assert!(ctx.take_buffered_tasks().is_empty());
    }

    #[test]
    fn savepoint_and_rollback_to() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.savepoint("sp1");
        ctx.buffer_task(dummy_task("b"));
        ctx.buffer_task(dummy_task("c"));

        ctx.rollback_to("sp1").unwrap();
        let tasks = ctx.take_buffered_tasks();
        assert_eq!(tasks.len(), 1); // Only "a" remains
    }

    #[test]
    fn rollback_to_nonexistent_fails() {
        let mut ctx = ProcedureTransactionCtx::new();
        assert!(ctx.rollback_to("nope").is_err());
    }

    #[test]
    fn release_savepoint() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.savepoint("sp1");
        ctx.buffer_task(dummy_task("b"));
        ctx.release_savepoint("sp1").unwrap();

        // Rollback to released savepoint should fail.
        assert!(ctx.rollback_to("sp1").is_err());

        // But tasks are still there.
        let tasks = ctx.take_buffered_tasks();
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn nested_savepoints() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.savepoint("sp1");
        ctx.buffer_task(dummy_task("b"));
        ctx.savepoint("sp2");
        ctx.buffer_task(dummy_task("c"));

        // Rollback to sp2: discard "c" only.
        ctx.rollback_to("sp2").unwrap();
        assert_eq!(ctx.buffer.len(), 2); // a + b

        // Rollback to sp1: discard "b".
        ctx.rollback_to("sp1").unwrap();
        assert_eq!(ctx.buffer.len(), 1); // a only

        let tasks = ctx.take_buffered_tasks();
        assert_eq!(tasks.len(), 1);
    }

    #[test]
    fn release_nonexistent_fails() {
        let mut ctx = ProcedureTransactionCtx::new();
        assert!(ctx.release_savepoint("nope").is_err());
    }

    #[test]
    fn savepoint_redefine() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.savepoint("sp1");
        ctx.buffer_task(dummy_task("b"));
        ctx.savepoint("sp1"); // Redefine — now at position 2
        ctx.buffer_task(dummy_task("c"));

        ctx.rollback_to("sp1").unwrap();
        assert_eq!(ctx.buffer.len(), 2); // a + b (redefined position)
    }
}

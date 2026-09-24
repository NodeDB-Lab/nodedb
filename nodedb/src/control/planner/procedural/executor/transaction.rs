// SPDX-License-Identifier: BUSL-1.1

//! Procedure transaction context: buffers DML tasks for COMMIT/ROLLBACK/SAVEPOINT.
//!
//! Stored procedures can execute COMMIT mid-body to finalize buffered DML,
//! ROLLBACK to discard it, and SAVEPOINT/ROLLBACK TO for partial rollback.
//!
//! Triggers do NOT use this — they dispatch DML immediately.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;

/// Lease scope retained for one planned statement in a procedure transaction.
///
/// `task_start` lets savepoint rollback drop scopes for every statement it
/// discards without requiring individual tasks to own a scope.
struct BufferedStatementScope {
    task_start: usize,
    scope: QueryLeaseScope,
}

/// Buffered transaction context for stored procedure execution.
///
/// DML statements inside a procedure body are collected here until
/// an explicit COMMIT flushes them as one system transaction, or ROLLBACK
/// discards them. An implicit COMMIT occurs at the end of the procedure.
#[derive(Default)]
pub struct ProcedureTransactionCtx {
    /// Buffered DML tasks awaiting COMMIT.
    buffer: Vec<PhysicalTask>,
    /// Descriptor lease scopes, one per planned statement, retained until its
    /// tasks have finished COMMIT dispatch or the statement is discarded.
    statement_scopes: Vec<BufferedStatementScope>,
    /// Savepoint stack: (name, buffer_position_at_savepoint_time).
    savepoints: Vec<(String, usize)>,
}

impl ProcedureTransactionCtx {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            statement_scopes: Vec::new(),
            savepoints: Vec::new(),
        }
    }

    /// Buffer all tasks planned for one statement and retain its descriptor
    /// leases until those tasks are dispatched at COMMIT.
    pub fn buffer_statement(&mut self, tasks: Vec<PhysicalTask>, scope: QueryLeaseScope) {
        let task_start = self.buffer.len();
        self.buffer.extend(tasks);
        self.statement_scopes
            .push(BufferedStatementScope { task_start, scope });
    }

    /// Buffer a task without a lease scope. Kept as a task-only test helper.
    pub fn buffer_task(&mut self, task: PhysicalTask) {
        self.buffer_task_with_empty_scope(task);
    }

    fn buffer_task_with_empty_scope(&mut self, task: PhysicalTask) {
        self.buffer_statement(vec![task], QueryLeaseScope::empty());
    }

    /// Take all buffered tasks and their owned scopes (on COMMIT). The caller
    /// must keep the scopes alive until every WAL append and dispatch finishes.
    pub fn take_buffered(&mut self) -> (Vec<PhysicalTask>, Vec<QueryLeaseScope>) {
        self.savepoints.clear();
        let scopes = std::mem::take(&mut self.statement_scopes)
            .into_iter()
            .map(|statement| statement.scope)
            .collect();
        (std::mem::take(&mut self.buffer), scopes)
    }

    /// Take every buffered statement with its lease scope, in buffer order
    /// (on COMMIT). Clears the savepoint stack.
    pub fn take_statements(&mut self) -> Vec<(Vec<PhysicalTask>, QueryLeaseScope)> {
        self.savepoints.clear();
        let mut tasks = std::mem::take(&mut self.buffer);
        let scopes = std::mem::take(&mut self.statement_scopes);
        let mut statements = Vec::with_capacity(scopes.len() + 1);
        // Each statement owns the tasks from its start to the next start.
        for statement in scopes.into_iter().rev() {
            let own = tasks.split_off(statement.task_start.min(tasks.len()));
            statements.push((own, statement.scope));
        }
        // Tasks buffered ahead of the first statement carry no lease.
        if !tasks.is_empty() {
            statements.push((tasks, QueryLeaseScope::empty()));
        }
        statements.reverse();
        statements
    }

    /// Take tasks only. Kept for existing task-oriented tests; it intentionally
    /// drops the associated scopes when the returned tasks are taken.
    pub fn take_buffered_tasks(&mut self) -> Vec<PhysicalTask> {
        self.take_buffered().0
    }

    /// Discard all buffered tasks and their descriptor lease scopes (on
    /// ROLLBACK). Clears the savepoint stack.
    pub fn rollback(&mut self) {
        self.buffer.clear();
        self.statement_scopes.clear();
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
                // Savepoints occur between statements, so every discarded
                // statement starts at or after the saved task position.
                self.statement_scopes
                    .retain(|statement| statement.task_start < p);
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
    use crate::types::{TenantId, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_physical::physical_task::PostSetOp;

    fn dummy_task(id: &str) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    crate::types::DatabaseId::DEFAULT,
                    "test",
                ),
                document_id: id.into(),
                value: vec![],
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
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
    fn commit_takes_each_statement_with_its_own_tasks() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_statement(
            vec![dummy_task("a"), dummy_task("b")],
            QueryLeaseScope::empty(),
        );
        ctx.buffer_statement(vec![dummy_task("c")], QueryLeaseScope::empty());
        let statements = ctx.take_statements();
        let sizes: Vec<usize> = statements.iter().map(|(tasks, _)| tasks.len()).collect();
        assert_eq!(sizes, vec![2, 1]);
        assert!(ctx.take_statements().is_empty());
    }

    #[test]
    fn rollback_and_commit_take_owned_statement_scopes() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_statement(vec![dummy_task("a")], QueryLeaseScope::empty());
        assert_eq!(ctx.statement_scopes.len(), 1);

        ctx.rollback();
        assert!(ctx.statement_scopes.is_empty());

        ctx.buffer_statement(vec![dummy_task("b")], QueryLeaseScope::empty());
        let (tasks, scopes) = ctx.take_buffered();
        assert_eq!(tasks.len(), 1);
        assert_eq!(scopes.len(), 1);
        assert!(ctx.statement_scopes.is_empty());
    }

    #[test]
    fn savepoint_and_rollback_to() {
        let mut ctx = ProcedureTransactionCtx::new();
        ctx.buffer_task(dummy_task("a"));
        ctx.savepoint("sp1");
        ctx.buffer_task(dummy_task("b"));
        ctx.buffer_task(dummy_task("c"));

        ctx.rollback_to("sp1").unwrap();
        assert_eq!(ctx.statement_scopes.len(), 1);
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

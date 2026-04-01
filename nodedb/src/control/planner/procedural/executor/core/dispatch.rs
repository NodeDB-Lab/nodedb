//! DML dispatch and transaction control for the statement executor.

use super::super::transaction::ProcedureTransactionCtx;
use super::StatementExecutor;
use crate::control::planner::procedural::ast::SqlExpr;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::eval;

impl<'a> StatementExecutor<'a> {
    // ── ASSIGN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_assign(
        &self,
        target: &str,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let target_upper = target.to_uppercase();
        if let Some(field_name) = target_upper.strip_prefix("NEW.") {
            let bound_expr = bindings.substitute(&expr.sql);
            let value = eval::evaluate_to_json(self.state, self.tenant_id, &bound_expr).await?;
            let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
            guard.insert(field_name.to_lowercase(), value);
        }
        Ok(())
    }

    // ── RETURN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_return(
        &self,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let bound_expr = bindings.substitute(&expr.sql);
        let value = eval::evaluate_to_json(self.state, self.tenant_id, &bound_expr).await?;
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert("__return".to_string(), value);
        Ok(())
    }

    // ── DML dispatch ────────────────────────────────────────────────────

    pub(super) async fn execute_dml(&self, sql: &str, bindings: &RowBindings) -> crate::Result<()> {
        let bound_sql = bindings.substitute(sql);

        let ctx = crate::control::planner::context::QueryContext::for_state(
            self.state,
            self.tenant_id.as_u32(),
        );
        let tasks = ctx.plan_sql(&bound_sql, self.tenant_id).await?;

        if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            for task in tasks {
                guard.buffer_task(task);
            }
        } else {
            for task in tasks {
                crate::control::server::dispatch_utils::wal_append_if_write(
                    &self.state.wal,
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                )?;

                crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                    self.state,
                    task.tenant_id,
                    task.vshard_id,
                    task.plan,
                    0,
                    self.event_source,
                )
                .await?;
            }
        }

        Ok(())
    }

    // ── Transaction control ─────────────────────────────────────────────

    pub(super) async fn execute_commit(&self) -> crate::Result<()> {
        self.flush_transaction_buffer().await
    }

    pub(super) fn execute_rollback(&self) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK", |ctx| {
            ctx.rollback();
            Ok(())
        })
    }

    pub(super) fn execute_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("SAVEPOINT", |ctx| {
            ctx.savepoint(name);
            Ok(())
        })
    }

    pub(super) fn execute_rollback_to(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK TO", |ctx| ctx.rollback_to(name))
    }

    pub(super) fn execute_release_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("RELEASE SAVEPOINT", |ctx| ctx.release_savepoint(name))
    }

    fn with_tx_ctx(
        &self,
        stmt_name: &str,
        f: impl FnOnce(&mut ProcedureTransactionCtx) -> crate::Result<()>,
    ) -> crate::Result<()> {
        match self.tx_ctx {
            Some(ref tx_ctx) => {
                let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
                f(&mut guard)
            }
            None => Err(crate::Error::BadRequest {
                detail: format!("{stmt_name} is only valid inside stored procedures"),
            }),
        }
    }

    /// Flush the procedure transaction buffer: WAL append + dispatch as batch.
    pub(super) async fn flush_transaction_buffer(&self) -> crate::Result<()> {
        let tasks = if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.take_buffered_tasks()
        } else {
            return Ok(());
        };

        if tasks.is_empty() {
            return Ok(());
        }

        for task in &tasks {
            crate::control::server::dispatch_utils::wal_append_if_write(
                &self.state.wal,
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )?;
        }

        if tasks.len() == 1 {
            let task = tasks.into_iter().next().unwrap();
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                self.state,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                0,
                self.event_source,
            )
            .await?;
        } else {
            let tenant_id = tasks[0].tenant_id;
            let vshard_id = tasks[0].vshard_id;
            let plans: Vec<_> = tasks.into_iter().map(|t| t.plan).collect();
            let batch_plan = crate::bridge::envelope::PhysicalPlan::Meta(
                crate::bridge::physical_plan::MetaOp::TransactionBatch { plans },
            );
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
                self.state,
                tenant_id,
                vshard_id,
                batch_plan,
                0,
                self.event_source,
            )
            .await?;
        }

        Ok(())
    }
}

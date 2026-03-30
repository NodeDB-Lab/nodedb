//! Statement executor for procedural SQL blocks with DML.
//!
//! Steps through procedural statements sequentially, dispatching each
//! embedded DML statement through the normal plan+SPSC path. Used by
//! triggers (Tier 3) and stored procedures (Tier 4).
//!
//! Runs on the **Control Plane** (Tokio async). Each DML in the body
//! is planned via DataFusion and dispatched to the Data Plane through
//! the existing SPSC bridge.

use crate::control::planner::procedural::ast::*;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::bindings::RowBindings;
use super::fuel::ExecutionBudget;

/// Maximum trigger cascade depth (trigger A fires trigger B fires trigger A).
pub const MAX_CASCADE_DEPTH: u32 = 16;

/// Statement executor: steps through procedural SQL blocks with DML.
///
/// Each instance tracks the current cascade depth to prevent infinite loops.
pub struct StatementExecutor<'a> {
    state: &'a SharedState,
    /// Stored for SECURITY INVOKER enforcement on trigger body DML.
    #[allow(dead_code)]
    identity: AuthenticatedIdentity,
    tenant_id: TenantId,
    cascade_depth: u32,
}

/// Control flow signal from statement execution.
enum Flow {
    /// Continue to next statement.
    Continue,
    /// Break out of innermost loop.
    Break,
    /// Skip to next iteration of innermost loop.
    LoopContinue,
}

impl<'a> StatementExecutor<'a> {
    pub fn new(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
    ) -> Self {
        Self {
            state,
            identity,
            tenant_id,
            cascade_depth,
        }
    }

    /// Execute a procedural block with the given row bindings.
    ///
    /// Uses an unlimited execution budget (for triggers). Stored procedures
    /// should call `execute_block_with_budget` instead.
    pub async fn execute_block(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let mut budget = ExecutionBudget::unlimited();
        self.execute_statements(&block.statements, bindings, &mut budget)
            .await
    }

    /// Execute a procedural block with an explicit execution budget.
    ///
    /// Used by stored procedures with configured max_iterations and timeout.
    pub async fn execute_block_with_budget(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        self.execute_statements(&block.statements, bindings, budget)
            .await
    }

    fn execute_statement<'b>(
        &'b self,
        stmt: &'b Statement,
        bindings: &'b RowBindings,
        budget: &'b mut ExecutionBudget,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Flow>> + Send + 'b>> {
        Box::pin(async move {
            budget.check()?;

            match stmt {
                Statement::Dml { sql } => {
                    self.execute_dml(sql, bindings).await?;
                    Ok(Flow::Continue)
                }
                Statement::If {
                    condition,
                    then_block,
                    elsif_branches,
                    else_block,
                } => {
                    let cond_sql = bindings.substitute(&condition.sql);
                    if self.evaluate_condition(&cond_sql).await? {
                        return self
                            .execute_statements_flow(then_block, bindings, budget)
                            .await;
                    }
                    for branch in elsif_branches {
                        let branch_cond = bindings.substitute(&branch.condition.sql);
                        if self.evaluate_condition(&branch_cond).await? {
                            return self
                                .execute_statements_flow(&branch.body, bindings, budget)
                                .await;
                        }
                    }
                    if let Some(else_stmts) = else_block {
                        return self
                            .execute_statements_flow(else_stmts, bindings, budget)
                            .await;
                    }
                    Ok(Flow::Continue)
                }
                Statement::While { condition, body } => {
                    loop {
                        budget.consume_iteration()?;
                        let cond_sql = bindings.substitute(&condition.sql);
                        if !self.evaluate_condition(&cond_sql).await? {
                            break;
                        }
                        match self.execute_statements_flow(body, bindings, budget).await? {
                            Flow::Break => break,
                            Flow::Continue | Flow::LoopContinue => {}
                        }
                    }
                    Ok(Flow::Continue)
                }
                Statement::Loop { body } => {
                    loop {
                        budget.consume_iteration()?;
                        match self.execute_statements_flow(body, bindings, budget).await? {
                            Flow::Break => break,
                            Flow::Continue | Flow::LoopContinue => {}
                        }
                    }
                    Ok(Flow::Continue)
                }
                Statement::For {
                    var,
                    start,
                    end,
                    reverse,
                    body,
                } => {
                    // Evaluate start/end as SQL expressions.
                    let start_sql = bindings.substitute(&start.sql);
                    let end_sql = bindings.substitute(&end.sql);
                    let start_val = self.evaluate_int(&start_sql).await?;
                    let end_val = self.evaluate_int(&end_sql).await?;

                    let range: Box<dyn Iterator<Item = i64> + Send> = if *reverse {
                        Box::new((end_val..=start_val).rev())
                    } else {
                        Box::new(start_val..=end_val)
                    };

                    for val in range {
                        budget.consume_iteration()?;
                        // Create a modified bindings with the loop variable.
                        let loop_bindings = bindings.with_variable(var, &val.to_string());
                        match self
                            .execute_statements_flow(body, &loop_bindings, budget)
                            .await?
                        {
                            Flow::Break => break,
                            Flow::Continue | Flow::LoopContinue => {}
                        }
                    }
                    Ok(Flow::Continue)
                }
                Statement::Break => Ok(Flow::Break),
                Statement::Continue => Ok(Flow::LoopContinue),
                Statement::Raise {
                    level: RaiseLevel::Exception,
                    message,
                } => {
                    let msg = bindings.substitute(&message.sql);
                    let clean_msg = msg.trim().trim_matches('\'').to_string();
                    Err(crate::Error::BadRequest {
                        detail: format!("raised exception: {clean_msg}"),
                    })
                }
                Statement::Raise { .. } => Ok(Flow::Continue),
                Statement::Declare { .. } | Statement::Assign { .. } => Ok(Flow::Continue),
                Statement::Return { .. } | Statement::ReturnQuery { .. } => Ok(Flow::Continue),
                Statement::Commit | Statement::Rollback => Err(crate::Error::BadRequest {
                    detail: "COMMIT/ROLLBACK not yet supported in procedure bodies".into(),
                }),
            }
        })
    }

    async fn execute_statements(
        &self,
        stmts: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        for stmt in stmts {
            self.execute_statement(stmt, bindings, budget).await?;
        }
        Ok(())
    }

    /// Execute statements, propagating Break/Continue flow signals.
    async fn execute_statements_flow(
        &self,
        stmts: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        for stmt in stmts {
            let flow = self.execute_statement(stmt, bindings, budget).await?;
            match flow {
                Flow::Continue => {}
                Flow::Break | Flow::LoopContinue => return Ok(flow),
            }
        }
        Ok(Flow::Continue)
    }

    /// Execute a DML statement, substituting bindings into SQL.
    async fn execute_dml(&self, sql: &str, bindings: &RowBindings) -> crate::Result<()> {
        let bound_sql = bindings.substitute(sql);

        let ctx = crate::control::planner::context::QueryContext::with_catalog(
            std::sync::Arc::clone(&self.state.credentials),
            self.tenant_id.as_u32(),
        );
        let tasks = ctx.plan_sql(&bound_sql, self.tenant_id).await?;

        for task in tasks {
            crate::control::server::dispatch_utils::wal_append_if_write(
                &self.state.wal,
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            )?;

            crate::control::server::dispatch_utils::dispatch_to_data_plane(
                self.state,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                0,
            )
            .await?;
        }

        Ok(())
    }

    /// Evaluate a SQL boolean condition.
    async fn evaluate_condition(&self, condition_sql: &str) -> crate::Result<bool> {
        if let Some(result) = crate::control::trigger::try_eval_simple_condition(condition_sql) {
            return Ok(result);
        }

        let batches = self.eval_sql_expr(condition_sql, "condition").await?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                let col = batch.column(0);
                if let Some(bool_arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                {
                    return Ok(bool_arr.value(0));
                }
                if let Some(int_arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                {
                    return Ok(int_arr.value(0) != 0);
                }
            }
        }

        Ok(false)
    }

    /// Evaluate a SQL expression as an integer.
    async fn evaluate_int(&self, sql: &str) -> crate::Result<i64> {
        if let Ok(v) = sql.trim().parse::<i64>() {
            return Ok(v);
        }

        let batches = self.eval_sql_expr(sql, "integer").await?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                let col = batch.column(0);
                if let Some(arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    return Ok(arr.value(0));
                }
                if let Some(arr) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                {
                    return Ok(arr.value(0) as i64);
                }
            }
        }

        Err(crate::Error::PlanError {
            detail: format!("could not evaluate '{sql}' as integer"),
        })
    }

    /// Execute a SQL expression via DataFusion and return the result batches.
    ///
    /// Wraps the expression in `SELECT (<expr>) as __result` and collects.
    async fn eval_sql_expr(
        &self,
        expr: &str,
        context: &str,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let ctx = crate::control::planner::context::QueryContext::with_catalog(
            std::sync::Arc::clone(&self.state.credentials),
            self.tenant_id.as_u32(),
        );
        let select_sql = format!("SELECT ({expr}) as __result");
        let df = ctx
            .session()
            .sql(&select_sql)
            .await
            .map_err(|e| crate::Error::PlanError {
                detail: format!("{context} eval: {e}"),
            })?;
        df.collect().await.map_err(|e| crate::Error::PlanError {
            detail: format!("{context} eval collect: {e}"),
        })
    }

    pub fn cascade_depth(&self) -> u32 {
        self.cascade_depth
    }
}

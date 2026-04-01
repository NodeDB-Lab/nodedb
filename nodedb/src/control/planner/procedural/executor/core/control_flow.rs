//! Control flow execution: IF/ELSIF/ELSE, WHILE, LOOP, FOR.

use super::Flow;
use super::StatementExecutor;
use crate::control::planner::procedural::ast::*;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::eval;
use crate::control::planner::procedural::executor::fuel::ExecutionBudget;

impl<'a> StatementExecutor<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_if(
        &self,
        condition: &SqlExpr,
        then_block: &[Statement],
        elsif_branches: &[ElsIfBranch],
        else_block: &Option<Vec<Statement>>,
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        let cond_sql = bindings.substitute(&condition.sql);
        if eval::evaluate_condition(self.state, self.tenant_id, &cond_sql).await? {
            return self
                .execute_statements_flow(then_block, bindings, budget)
                .await;
        }
        for branch in elsif_branches {
            let branch_cond = bindings.substitute(&branch.condition.sql);
            if eval::evaluate_condition(self.state, self.tenant_id, &branch_cond).await? {
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

    pub(super) async fn execute_while(
        &self,
        condition: &SqlExpr,
        body: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        loop {
            budget.consume_iteration()?;
            let cond_sql = bindings.substitute(&condition.sql);
            if !eval::evaluate_condition(self.state, self.tenant_id, &cond_sql).await? {
                break;
            }
            match self.execute_statements_flow(body, bindings, budget).await? {
                Flow::Break => break,
                Flow::Continue | Flow::LoopContinue => {}
            }
        }
        Ok(Flow::Continue)
    }

    pub(super) async fn execute_loop(
        &self,
        body: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        loop {
            budget.consume_iteration()?;
            match self.execute_statements_flow(body, bindings, budget).await? {
                Flow::Break => break,
                Flow::Continue | Flow::LoopContinue => {}
            }
        }
        Ok(Flow::Continue)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_for(
        &self,
        var: &str,
        start: &SqlExpr,
        end: &SqlExpr,
        reverse: bool,
        body: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        let start_sql = bindings.substitute(&start.sql);
        let end_sql = bindings.substitute(&end.sql);
        let start_val = eval::evaluate_int(self.state, self.tenant_id, &start_sql).await?;
        let end_val = eval::evaluate_int(self.state, self.tenant_id, &end_sql).await?;

        let range: Box<dyn Iterator<Item = i64> + Send> = if reverse {
            Box::new((end_val..=start_val).rev())
        } else {
            Box::new(start_val..=end_val)
        };

        for val in range {
            budget.consume_iteration()?;
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
}

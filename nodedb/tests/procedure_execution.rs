//! Integration tests for stored procedure execution: transaction control,
//! SAVEPOINT, OUT parameters, exception handling, routability, parser changes.

use nodedb::control::planner::procedural::ast::*;
use nodedb::control::planner::procedural::executor::exception::exception_matches;
use nodedb::control::planner::procedural::executor::transaction::ProcedureTransactionCtx;
use nodedb::control::planner::procedural::parse_block;
use nodedb::control::security::catalog::procedure_types::*;

// ---------------------------------------------------------------------------
// Parser: SAVEPOINT / ROLLBACK TO / RELEASE SAVEPOINT
// ---------------------------------------------------------------------------

#[test]
fn parse_savepoint() {
    let block = parse_block("BEGIN SAVEPOINT sp1; RETURN 1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::Savepoint { name } if name == "sp1"));
}

#[test]
fn parse_rollback_to_savepoint() {
    let block = parse_block("BEGIN ROLLBACK TO sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::RollbackTo { name } if name == "sp1"));
}

#[test]
fn parse_rollback_to_savepoint_keyword() {
    let block = parse_block("BEGIN ROLLBACK TO SAVEPOINT sp2; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::RollbackTo { name } if name == "sp2"));
}

#[test]
fn parse_release_savepoint() {
    let block = parse_block("BEGIN RELEASE SAVEPOINT sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::ReleaseSavepoint { name } if name == "sp1"));
}

#[test]
fn parse_release_without_savepoint_keyword() {
    let block = parse_block("BEGIN RELEASE sp1; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::ReleaseSavepoint { name } if name == "sp1"));
}

#[test]
fn parse_commit_rollback_still_work() {
    let block = parse_block("BEGIN COMMIT; ROLLBACK; END").unwrap();
    assert!(matches!(&block.statements[0], Statement::Commit));
    assert!(matches!(&block.statements[1], Statement::Rollback));
}

// ---------------------------------------------------------------------------
// Parser: EXCEPTION handlers
// ---------------------------------------------------------------------------

#[test]
fn parse_exception_others() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN OTHERS THEN \
             RETURN 0; \
         END",
    )
    .unwrap();
    assert_eq!(block.statements.len(), 1); // INSERT only
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Others
    );
    assert!(!block.exception_handlers[0].body.is_empty());
}

#[test]
fn parse_exception_sqlstate() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN SQLSTATE '23505' THEN \
             RETURN -1; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::SqlState("23505".into())
    );
}

#[test]
fn parse_exception_named() {
    let block = parse_block(
        "BEGIN \
           DELETE FROM t WHERE id = 1; \
         EXCEPTION \
           WHEN UNIQUE_VIOLATION THEN \
             RETURN -1; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Named("UNIQUE_VIOLATION".into())
    );
}

#[test]
fn parse_multiple_exception_handlers() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO t (id) VALUES (1); \
         EXCEPTION \
           WHEN UNIQUE_VIOLATION THEN \
             RETURN -1; \
           WHEN OTHERS THEN \
             RETURN -2; \
         END",
    )
    .unwrap();
    assert_eq!(block.exception_handlers.len(), 2);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Named("UNIQUE_VIOLATION".into())
    );
    assert_eq!(
        block.exception_handlers[1].condition,
        ExceptionCondition::Others
    );
}

#[test]
fn parse_no_exception_block() {
    let block = parse_block("BEGIN RETURN 42; END").unwrap();
    assert!(block.exception_handlers.is_empty());
}

// ---------------------------------------------------------------------------
// Transaction context: buffer + savepoint + rollback
// ---------------------------------------------------------------------------

fn dummy_task(id: &str) -> nodedb::control::planner::physical::PhysicalTask {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    nodedb::control::planner::physical::PhysicalTask {
        tenant_id: nodedb::types::TenantId::new(1),
        vshard_id: nodedb::types::VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "test".into(),
            document_id: id.into(),
            value: vec![],
        }),
        post_dedup: false,
    }
}

#[test]
fn tx_ctx_buffer_and_commit() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_task(dummy_task("a"));
    ctx.buffer_task(dummy_task("b"));
    ctx.buffer_task(dummy_task("c"));
    let tasks = ctx.take_buffered_tasks();
    assert_eq!(tasks.len(), 3);
    assert!(ctx.take_buffered_tasks().is_empty()); // Empty after take
}

#[test]
fn tx_ctx_rollback_discards_all() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_task(dummy_task("a"));
    ctx.buffer_task(dummy_task("b"));
    ctx.rollback();
    assert!(ctx.take_buffered_tasks().is_empty());
}

#[test]
fn tx_ctx_savepoint_rollback_to() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_task(dummy_task("a"));
    ctx.savepoint("sp1");
    ctx.buffer_task(dummy_task("b"));
    ctx.buffer_task(dummy_task("c"));
    ctx.rollback_to("sp1").unwrap();

    let tasks = ctx.take_buffered_tasks();
    assert_eq!(tasks.len(), 1); // Only "a"
}

#[test]
fn tx_ctx_nested_savepoints() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_task(dummy_task("a"));
    ctx.savepoint("sp1");
    ctx.buffer_task(dummy_task("b"));
    ctx.savepoint("sp2");
    ctx.buffer_task(dummy_task("c"));
    ctx.buffer_task(dummy_task("d"));

    ctx.rollback_to("sp2").unwrap();
    // a + b remain (sp2 was after b)
    assert_eq!(ctx.take_buffered_tasks().len(), 2);
}

#[test]
fn tx_ctx_release_savepoint() {
    let mut ctx = ProcedureTransactionCtx::new();
    ctx.buffer_task(dummy_task("a"));
    ctx.savepoint("sp1");
    ctx.buffer_task(dummy_task("b"));
    ctx.release_savepoint("sp1").unwrap();

    // Released savepoint can't be rolled back to.
    assert!(ctx.rollback_to("sp1").is_err());
    // But data is still there.
    assert_eq!(ctx.take_buffered_tasks().len(), 2);
}

#[test]
fn tx_ctx_rollback_to_nonexistent() {
    let mut ctx = ProcedureTransactionCtx::new();
    assert!(ctx.rollback_to("nope").is_err());
}

#[test]
fn tx_ctx_release_nonexistent() {
    let mut ctx = ProcedureTransactionCtx::new();
    assert!(ctx.release_savepoint("nope").is_err());
}

// ---------------------------------------------------------------------------
// Exception matching
// ---------------------------------------------------------------------------

#[test]
fn exception_others_matches_all() {
    assert!(exception_matches(
        &ExceptionCondition::Others,
        "any error at all"
    ));
    assert!(exception_matches(&ExceptionCondition::Others, ""));
}

#[test]
fn exception_sqlstate_match() {
    assert!(exception_matches(
        &ExceptionCondition::SqlState("23505".into()),
        "duplicate key violates unique constraint (23505)"
    ));
}

#[test]
fn exception_sqlstate_no_match() {
    assert!(!exception_matches(
        &ExceptionCondition::SqlState("23505".into()),
        "foreign key violation"
    ));
}

#[test]
fn exception_named_unique_violation() {
    let cond = ExceptionCondition::Named("UNIQUE_VIOLATION".into());
    assert!(exception_matches(&cond, "duplicate key error"));
    assert!(exception_matches(&cond, "UNIQUE constraint failed"));
    assert!(!exception_matches(&cond, "timeout exceeded"));
}

#[test]
fn exception_named_no_data_found() {
    let cond = ExceptionCondition::Named("NO_DATA_FOUND".into());
    assert!(exception_matches(&cond, "collection not found"));
    assert!(!exception_matches(&cond, "success"));
}

// ---------------------------------------------------------------------------
// Routability classification
// ---------------------------------------------------------------------------

#[test]
fn routability_default_is_multi_collection() {
    assert_eq!(
        ProcedureRoutability::default(),
        ProcedureRoutability::MultiCollection
    );
}

#[test]
fn routability_single_collection() {
    let r = ProcedureRoutability::SingleCollection("orders".into());
    assert_eq!(r, ProcedureRoutability::SingleCollection("orders".into()));
}

// ---------------------------------------------------------------------------
// AST: new statement variants
// ---------------------------------------------------------------------------

#[test]
fn ast_savepoint_variant() {
    let stmt = Statement::Savepoint { name: "sp1".into() };
    assert!(matches!(stmt, Statement::Savepoint { name } if name == "sp1"));
}

#[test]
fn ast_rollback_to_variant() {
    let stmt = Statement::RollbackTo { name: "sp1".into() };
    assert!(matches!(stmt, Statement::RollbackTo { name } if name == "sp1"));
}

#[test]
fn ast_release_savepoint_variant() {
    let stmt = Statement::ReleaseSavepoint { name: "sp1".into() };
    assert!(matches!(stmt, Statement::ReleaseSavepoint { name } if name == "sp1"));
}

// ---------------------------------------------------------------------------
// AST: exception handler on ProceduralBlock
// ---------------------------------------------------------------------------

#[test]
fn procedural_block_with_exception_handlers() {
    let block = ProceduralBlock {
        statements: vec![Statement::Return {
            expr: SqlExpr::new("42"),
        }],
        exception_handlers: vec![ExceptionHandler {
            condition: ExceptionCondition::Others,
            body: vec![Statement::Return {
                expr: SqlExpr::new("0"),
            }],
        }],
    };
    assert_eq!(block.exception_handlers.len(), 1);
    assert_eq!(
        block.exception_handlers[0].condition,
        ExceptionCondition::Others
    );
}

#[test]
fn procedural_block_empty_handlers() {
    let block = ProceduralBlock {
        statements: vec![],
        exception_handlers: vec![],
    };
    assert!(block.exception_handlers.is_empty());
}

// ---------------------------------------------------------------------------
// StoredProcedure: routability field
// ---------------------------------------------------------------------------

#[test]
fn stored_procedure_has_routability() {
    let proc = StoredProcedure {
        tenant_id: 1,
        name: "test".into(),
        parameters: vec![],
        body_sql: "BEGIN RETURN; END".into(),
        max_iterations: 1_000_000,
        timeout_secs: 60,
        routability: ProcedureRoutability::SingleCollection("orders".into()),
        owner: "admin".into(),
        created_at: 0,
    };
    assert_eq!(
        proc.routability,
        ProcedureRoutability::SingleCollection("orders".into())
    );
}

// ---------------------------------------------------------------------------
// Fuel metering
// ---------------------------------------------------------------------------

#[test]
fn execution_budget_exhaustion() {
    use nodedb::control::planner::procedural::executor::fuel::ExecutionBudget;

    let mut budget = ExecutionBudget::new(3, 60);
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_ok());
    assert!(budget.consume_iteration().is_err()); // Exhausted
}

#[test]
fn execution_budget_unlimited() {
    use nodedb::control::planner::procedural::executor::fuel::ExecutionBudget;

    let mut budget = ExecutionBudget::unlimited();
    for _ in 0..1000 {
        assert!(budget.consume_iteration().is_ok());
    }
}

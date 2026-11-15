// SPDX-License-Identifier: BUSL-1.1

//! A simple query of several statements runs as one implicit transaction, as in PostgreSQL.
//!
//! Rules:
//! - The implicit block opens at the first statement of a run that holds a statement that is
//!   not a read. A run ends at a transaction-control statement. So every statement of the run
//!   reads the block's one snapshot, and COMMIT checks its leading reads too.
//!   A read is a statement whose first word is `SELECT`, `SHOW`, `VALUES` or `TABLE`.
//!   A `WITH` or `EXPLAIN` statement is a read when no write verb sits outside its literals.
//!   A request of reads alone opens no block, so it never ends in a commit-time 40001.
//! - It opens only while the session is `Idle`. An explicit block buffers the statements itself.
//! - A `BEGIN` inside the implicit block turns it into the explicit block.
//! - A `COMMIT` or `ROLLBACK` ends it. The next run that holds a statement that is not a read
//!   opens a new one.
//! - An error rolls the implicit block back. An explicit block goes `Failed` and stays open.
//! - The end of the string commits the implicit block. A commit error answers as the last error.
//! - The implicit block adds no `BEGIN` or `COMMIT` tag.
//!
//! The block opens and ends through the BEGIN, COMMIT and ROLLBACK adapters.
//! So DDL buffers in the block, and a command that refuses a block answers 25001.

use nodedb_sql::parser::preprocess::lex::{first_sql_word, keyword_position_outside_literals};
use pgwire::api::portal::Format;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use super::core::NodeDbPgHandler;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::audit_context::AuditScope;
use crate::control::server::shared::session::{SessionId, TransactionState};
use crate::control::server::shared::txn_control::{
    TxnControl, TxnModes, classify as classify_txn_control,
};

/// First words of a statement that runs before the implicit block opens.
const READ_HEADS: &[&str] = &["SELECT", "SHOW", "VALUES", "TABLE"];

/// First words of a statement that is a read only when it holds no write verb.
const READ_WRAPPER_HEADS: &[&str] = &["WITH", "EXPLAIN"];

/// Verbs that make a `WITH` or `EXPLAIN` statement a write.
///
/// The check is word-based. An identifier spelled as a verb opens the block, which costs a commit
/// and never skips one.
const WRITE_VERBS: &[&str] = &["INSERT", "UPSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE"];

/// First words of a transaction-control statement.
///
/// `classify_txn_control` returns `None` for a spelling outside its grammar.
/// These statements never open the implicit block, whatever their spelling.
const TRANSACTION_CONTROL_HEADS: &[&str] = &[
    "BEGIN",
    "START",
    "COMMIT",
    "END",
    "ROLLBACK",
    "ABORT",
    "SAVEPOINT",
    "RELEASE",
];

impl NodeDbPgHandler {
    /// Run `statements` in order inside implicit transaction blocks, and stop at the first error.
    ///
    /// Returns the responses of the statements that ran, and the error that stopped the loop.
    /// The error of the implicit COMMIT at the end returns as the loop error.
    pub(super) async fn execute_statements_implicit(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        statements: Vec<String>,
        requested: &Format,
    ) -> (Vec<Response>, PgWireResult<()>) {
        let mut all = Vec::new();
        // `true` while this loop owns an open implicit block.
        let mut implicit_open = false;
        for (index, stmt) in statements.iter().enumerate() {
            // Each statement's DDL proposals carry that statement's own text.
            let _audit = AuditScope::statement(identity, stmt);
            if let Some(binding) = self.sessions.binding(session_id) {
                binding.set_statement_text(stmt);
            }
            let control = classify_txn_control(stmt.trim());
            if !implicit_open
                && run_opens_implicit_block(&statements[index..])
                && self.sessions.transaction_state(session_id) == TransactionState::Idle
            {
                // The BEGIN tag of the adapter is dropped: the implicit block answers no tag.
                if let Err(error) = self.handle_begin(session_id, &TxnModes::default()) {
                    return (all, Err(error));
                }
                implicit_open = true;
            }
            match self
                .execute_single_sql(identity, session_id, stmt, requested)
                .await
            {
                Ok(mut resp) => {
                    all.append(&mut resp);
                    implicit_open = implicit_open
                        && !matches!(control, Some(TxnControl::Begin(_)))
                        && self.sessions.transaction_state(session_id) != TransactionState::Idle;
                }
                Err(error) => {
                    if implicit_open {
                        self.rollback_implicit(identity, session_id).await;
                    }
                    return (all, Err(error));
                }
            }
        }
        if implicit_open {
            // The COMMIT tag of the adapter is dropped: the implicit block answers no tag.
            if let Err(error) = self.handle_commit(identity, session_id).await {
                return (all, Err(error));
            }
        }
        (all, Ok(()))
    }

    /// Roll back the implicit block after a statement error.
    ///
    /// The ROLLBACK adapter answers only `Ok`. An `Err` is logged, and the statement error answers.
    async fn rollback_implicit(&self, identity: &AuthenticatedIdentity, session_id: SessionId) {
        if let Err(error) = self.handle_rollback(identity, session_id).await {
            tracing::error!(%error, "implicit transaction rollback after a statement error");
        }
    }
}

/// Whether the run at the head of `rest` opens the implicit block when none is
/// open: a statement before the next transaction-control statement opens it.
fn run_opens_implicit_block(rest: &[String]) -> bool {
    rest.iter()
        .take_while(|statement| !is_transaction_control(statement))
        .any(|statement| opens_implicit_block(statement))
}

/// Whether the first word of `statement` is a transaction-control word.
fn is_transaction_control(statement: &str) -> bool {
    first_sql_word(statement)
        .is_some_and(|head| TRANSACTION_CONTROL_HEADS.contains(&head.to_ascii_uppercase().as_str()))
}

/// Whether `statement` opens the implicit block when none is open.
///
/// A read and a transaction-control statement do not open it.
fn opens_implicit_block(statement: &str) -> bool {
    let Some(head) = first_sql_word(statement) else {
        return false;
    };
    let head = head.to_ascii_uppercase();
    let head = head.as_str();
    if READ_HEADS.contains(&head) || TRANSACTION_CONTROL_HEADS.contains(&head) {
        return false;
    }
    if READ_WRAPPER_HEADS.contains(&head) {
        return holds_write_verb(statement);
    }
    true
}

/// Whether a write verb sits outside the literals of `statement`.
fn holds_write_verb(statement: &str) -> bool {
    WRITE_VERBS
        .iter()
        .any(|verb| keyword_position_outside_literals(statement, verb).is_some())
}

#[cfg(test)]
mod tests {
    use super::{opens_implicit_block, run_opens_implicit_block};

    /// The run of `statements` at its head opens the block.
    fn run_opens(statements: &[&str]) -> bool {
        let owned: Vec<String> = statements.iter().map(|s| (*s).to_owned()).collect();
        run_opens_implicit_block(&owned)
    }

    #[test]
    fn a_leading_read_opens_the_block_of_its_run() {
        assert!(run_opens(&["SELECT 1", "UPDATE t SET v = 1"]));
        assert!(run_opens(&[
            "SELECT 1",
            "SHOW x",
            "INSERT INTO t (id) VALUES ('a')"
        ]));
        assert!(!run_opens(&["SELECT 1", "SELECT 2"]));
    }

    /// A transaction-control statement ends the run, so a write after it
    /// opens no block for the reads before it.
    #[test]
    fn a_run_ends_at_transaction_control() {
        assert!(!run_opens(&["SELECT 1", "BEGIN", "UPDATE t SET v = 1"]));
        assert!(!run_opens(&["SELECT 1", "COMMIT", "UPDATE t SET v = 1"]));
        assert!(!run_opens(&["COMMIT", "UPDATE t SET v = 1"]));
    }

    #[test]
    fn a_write_or_ddl_opens_the_block() {
        for sql in [
            "INSERT INTO t (id) VALUES ('a')",
            "upsert into t (id) values ('a')",
            "UPDATE t SET v = 1",
            "DELETE FROM t",
            "CREATE COLLECTION t (id TEXT PRIMARY KEY)",
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
            "SET x = 1",
        ] {
            assert!(opens_implicit_block(sql), "{sql} must open the block");
        }
    }

    #[test]
    fn a_read_does_not_open_the_block() {
        for sql in [
            "SELECT 1",
            "select(1)",
            "  -- c\n SELECT 2",
            "SHOW client_encoding",
            "VALUES (1), (2)",
            "TABLE t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "with x as (select 'insert into t' AS s) select s from x",
            "EXPLAIN SELECT * FROM t",
        ] {
            assert!(!opens_implicit_block(sql), "{sql} must not open the block");
        }
    }

    /// A `WITH` or `EXPLAIN` statement that holds a write verb opens the block.
    #[test]
    fn a_wrapped_write_opens_the_block() {
        for sql in [
            "WITH x AS (DELETE FROM t RETURNING id) SELECT * FROM x",
            "with x as (select 1) update t set v = 1",
            "EXPLAIN ANALYZE INSERT INTO t (id) VALUES ('a')",
            "WITH x AS (SELECT 1) MERGE INTO t USING x ON true WHEN MATCHED THEN DELETE",
        ] {
            assert!(opens_implicit_block(sql), "{sql} must open the block");
        }
    }

    #[test]
    fn transaction_control_does_not_open_the_block() {
        for sql in [
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "COMMIT AND CHAIN",
            "END",
            "ROLLBACK",
            "ABORT",
            "SAVEPOINT s",
            "RELEASE SAVEPOINT s",
        ] {
            assert!(!opens_implicit_block(sql), "{sql} must not open the block");
        }
    }

    #[test]
    fn an_empty_statement_does_not_open_the_block() {
        assert!(!opens_implicit_block("  -- only a comment"));
    }
}

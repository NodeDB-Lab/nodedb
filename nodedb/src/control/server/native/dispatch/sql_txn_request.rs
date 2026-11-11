// SPDX-License-Identifier: BUSL-1.1

//! Transaction control inside one native request of several statements.
//!
//! The native protocol holds a session, so it follows PostgreSQL rules for a
//! string such as `BEGIN; A; COMMIT; B`. Statements run in order:
//!
//! | Statement | Action |
//! |---|---|
//! | Statements outside a block | Collect into one implicit transaction. |
//! | `BEGIN` | Open the block. Collected statements join it. |
//! | Statement inside a block | Stage in the block. |
//! | `COMMIT` | Commit the block, or the implicit transaction. |
//! | `ROLLBACK` | Roll back the block, or drop the implicit transaction. |
//! | `SAVEPOINT`, `RELEASE`, `ROLLBACK TO` | Run as the lone statement runs. |
//!
//! The first error stops the request and answers that error. An error inside
//! an explicit block marks the block failed. The block stays open until the
//! client ends it.
//!
//! The answer folds the answers of every statement that is not transaction
//! control into one frame, as `A; B` answers: rows in statement order, one
//! summed tag. A request of transaction control alone answers its last
//! statement.

use nodedb_types::protocol::{NativeResponse, ResponseStatus};

use crate::control::server::response_shape::types::DmlOutcome;
use crate::control::server::response_shape::types::dml_outcome::MIXED_VERB;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::server::shared::txn_control::{TxnControl, classify};

use super::DispatchCtx;
use super::sql::{dispatch_statement, handle_txn_control};
use super::transaction::{handle_begin, handle_commit, handle_rollback};

/// The statements of `sql` when it holds two or more and at least one is
/// transaction control. `None` sends the text down the plain path.
pub(super) fn txn_control_statements(sql: &str) -> Option<Vec<String>> {
    let statements = split_sql_statements(sql);
    if statements.len() < 2 {
        return None;
    }
    statements
        .iter()
        .any(|statement| classify(statement).is_some())
        .then_some(statements)
}

/// Run `statements` in order on the session of `ctx` under PostgreSQL rules.
pub(super) async fn run_txn_control_request(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    statements: &[String],
) -> NativeResponse {
    let mut implicit: Vec<&str> = Vec::new();
    let mut fold = NativeFold::default();
    let mut last = NativeResponse::ok(seq);
    for statement in statements {
        let answer = match classify(statement) {
            Some(control) => run_control(ctx, seq, control, &mut implicit, &mut fold).await,
            None if in_block(ctx) => {
                let answer = run_alone(ctx, seq, statement).await;
                fold.add(&answer);
                answer
            }
            None => {
                implicit.push(statement.as_str());
                continue;
            }
        };
        if is_error(&answer) {
            return answer;
        }
        last = answer;
    }
    if !implicit.is_empty() {
        let answer = run_implicit(ctx, seq, &implicit).await;
        if is_error(&answer) {
            return answer;
        }
        fold.add(&answer);
    }
    fold.finish(last)
}

/// Run one transaction-control statement. `implicit` holds the statements
/// collected outside a block since the last control statement. Each
/// statement that runs folds its answer into `fold`.
async fn run_control(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    control: TxnControl,
    implicit: &mut Vec<&str>,
    fold: &mut NativeFold,
) -> NativeResponse {
    match control {
        TxnControl::Begin(modes) => {
            let begun = handle_begin(ctx, seq, &modes);
            if is_error(&begun) {
                return begun;
            }
            // PostgreSQL turns the implicit transaction into the block. The
            // collected statements stage in it, ahead of every later one.
            for statement in implicit.drain(..) {
                let answer = run_alone(ctx, seq, statement).await;
                if is_error(&answer) {
                    return answer;
                }
                fold.add(&answer);
            }
            begun
        }
        TxnControl::Commit if !implicit.is_empty() => {
            let answer = run_implicit(ctx, seq, implicit.as_slice()).await;
            implicit.clear();
            if is_error(&answer) {
                return answer;
            }
            fold.add(&answer);
            NativeResponse::status_row(seq, "COMMIT")
        }
        TxnControl::Commit => handle_commit(ctx, seq).await,
        TxnControl::Rollback if !implicit.is_empty() => {
            // The implicit transaction never ran, so nothing applies.
            implicit.clear();
            NativeResponse::status_row(seq, "ROLLBACK")
        }
        TxnControl::Rollback => handle_rollback(ctx, seq).await,
        savepoint => {
            if !implicit.is_empty() {
                let answer = run_implicit(ctx, seq, implicit.as_slice()).await;
                implicit.clear();
                if is_error(&answer) {
                    return answer;
                }
                fold.add(&answer);
            }
            handle_txn_control(ctx, seq, savepoint).await
        }
    }
}

/// Run the statements collected outside a block as one request: one implicit
/// transaction that applies all of them or none.
async fn run_implicit(ctx: &DispatchCtx<'_>, seq: u64, implicit: &[&str]) -> NativeResponse {
    let text = implicit.join("; ");
    dispatch_statement(ctx, seq, &text, false)
        .await
        .into_response()
}

/// Run one statement as a request of one statement. Inside a block it
/// stages. An error inside a block marks the block failed.
async fn run_alone(ctx: &DispatchCtx<'_>, seq: u64, statement: &str) -> NativeResponse {
    dispatch_statement(ctx, seq, statement, false)
        .await
        .into_response()
}

/// Whether the session holds a block, open or failed.
fn in_block(ctx: &DispatchCtx<'_>) -> bool {
    ctx.sessions.transaction_state(ctx.peer_addr) != TransactionState::Idle
}

fn is_error(answer: &NativeResponse) -> bool {
    answer.status == ResponseStatus::Error
}

/// The answers of a request's statements, folded into one frame.
///
/// Rules, as a request of several statements answers on the one-set path:
/// - Rows follow statement order. The columns are those of the first answer
///   that holds a row.
/// - The same verb sums its count. Different verbs answer `MIXED` with the
///   sum of the counts that carry one.
/// - A status row of a command that carries no count adds no row and no tag.
#[derive(Default)]
struct NativeFold {
    answered: bool,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<nodedb_types::value::Value>>,
    warnings: Vec<String>,
    command: Option<String>,
    affected: Option<u64>,
    watermark_lsn: u64,
}

impl NativeFold {
    /// Fold one statement's answer.
    fn add(&mut self, answer: &NativeResponse) {
        self.answered = true;
        self.warnings.extend(answer.warnings.iter().cloned());
        self.watermark_lsn = self.watermark_lsn.max(answer.watermark_lsn);
        if is_status_row(answer) {
            return;
        }
        if let Some(rows) = answer.rows.as_ref().filter(|rows| !rows.is_empty()) {
            if self.columns.is_none() {
                self.columns = answer.columns.clone();
            }
            self.rows.extend(rows.iter().cloned());
        }
        let Some(verb) = answer.command.as_deref() else {
            return;
        };
        self.command = match self.command.take() {
            None => Some(verb.to_owned()),
            Some(first) if first == verb => Some(first),
            Some(_) => Some(MIXED_VERB.to_owned()),
        };
        if DmlOutcome::verb_carries_count(verb)
            && let Some(count) = answer.rows_affected
        {
            self.affected = Some(self.affected.unwrap_or(0).saturating_add(count));
        }
    }

    /// The folded frame. `last` answers a request where no statement outside
    /// transaction control ran.
    fn finish(self, last: NativeResponse) -> NativeResponse {
        if !self.answered {
            return last;
        }
        let mut r = NativeResponse::ok(last.seq);
        if !self.rows.is_empty() {
            r.columns = self.columns;
            r.rows = Some(self.rows);
        }
        r.rows_affected = self.affected;
        r.command = self.command;
        r.watermark_lsn = self.watermark_lsn;
        r.warnings = self.warnings;
        r
    }
}

/// Whether `answer` is the status row of a command that carries no count.
fn is_status_row(answer: &NativeResponse) -> bool {
    answer.command.is_none()
        && answer
            .columns
            .as_deref()
            .is_some_and(|columns| columns == ["status"])
}

#[cfg(test)]
mod tests {
    use nodedb_types::protocol::NativeResponse;
    use nodedb_types::value::Value;

    use super::{NativeFold, txn_control_statements};

    #[test]
    fn a_request_of_one_statement_takes_the_plain_path() {
        assert_eq!(txn_control_statements("BEGIN"), None);
        assert_eq!(txn_control_statements("BEGIN;"), None);
        assert_eq!(
            txn_control_statements("INSERT INTO t (id) VALUES ('a')"),
            None
        );
    }

    #[test]
    fn a_request_with_no_transaction_control_takes_the_plain_path() {
        assert_eq!(txn_control_statements("SELECT 1; SELECT 2"), None);
        assert_eq!(txn_control_statements("SET x = 1; SELECT 2"), None);
    }

    #[test]
    fn a_request_with_transaction_control_splits_into_its_statements() {
        assert_eq!(
            txn_control_statements("BEGIN; SELECT 1; COMMIT"),
            Some(vec!["BEGIN".into(), "SELECT 1".into(), "COMMIT".into()])
        );
        assert_eq!(
            txn_control_statements("SELECT 1; COMMIT; SELECT 2"),
            Some(vec!["SELECT 1".into(), "COMMIT".into(), "SELECT 2".into()])
        );
        assert_eq!(
            txn_control_statements("SAVEPOINT s; SELECT 1"),
            Some(vec!["SAVEPOINT s".into(), "SELECT 1".into()])
        );
    }

    fn write(verb: &str, affected: u64) -> NativeResponse {
        let mut r = NativeResponse::ok(7);
        r.command = Some(verb.to_owned());
        r.rows_affected = Some(affected);
        r
    }

    fn read(value: &str) -> NativeResponse {
        let mut r = NativeResponse::ok(7);
        r.columns = Some(vec!["id".into()]);
        r.rows = Some(vec![vec![Value::String(value.into())]]);
        r
    }

    #[test]
    fn two_writes_of_one_verb_sum_their_count() {
        let mut fold = NativeFold::default();
        fold.add(&write("INSERT", 1));
        fold.add(&write("INSERT", 2));
        let r = fold.finish(NativeResponse::status_row(7, "COMMIT"));
        assert_eq!(r.command.as_deref(), Some("INSERT"));
        assert_eq!(r.rows_affected, Some(3));
        assert!(r.rows.is_none());
    }

    #[test]
    fn two_verbs_answer_mixed_with_the_sum() {
        let mut fold = NativeFold::default();
        fold.add(&write("UPDATE", 1));
        fold.add(&write("DELETE", 1));
        let r = fold.finish(NativeResponse::status_row(7, "COMMIT"));
        assert_eq!(r.command.as_deref(), Some("MIXED"));
        assert_eq!(r.rows_affected, Some(2));
    }

    #[test]
    fn rows_follow_statement_order_beside_the_tag() {
        let mut fold = NativeFold::default();
        fold.add(&write("INSERT", 1));
        fold.add(&read("a"));
        fold.add(&read("b"));
        let r = fold.finish(NativeResponse::status_row(7, "COMMIT"));
        assert_eq!(r.columns, Some(vec!["id".to_owned()]));
        assert_eq!(
            r.rows,
            Some(vec![
                vec![Value::String("a".into())],
                vec![Value::String("b".into())],
            ])
        );
        assert_eq!(r.command.as_deref(), Some("INSERT"));
        assert_eq!(r.rows_affected, Some(1));
    }

    #[test]
    fn a_status_row_adds_no_row_and_no_tag() {
        let mut fold = NativeFold::default();
        fold.add(&NativeResponse::status_row(7, "CREATE COLLECTION"));
        fold.add(&write("INSERT", 1));
        let r = fold.finish(NativeResponse::status_row(7, "COMMIT"));
        assert!(r.rows.is_none());
        assert_eq!(r.command.as_deref(), Some("INSERT"));
        assert_eq!(r.rows_affected, Some(1));
    }

    #[test]
    fn transaction_control_alone_answers_its_last_statement() {
        let r = NativeFold::default().finish(NativeResponse::status_row(7, "COMMIT"));
        assert_eq!(
            r.rows,
            Some(vec![vec![Value::String("COMMIT".into())]]),
            "the COMMIT status row"
        );
    }
}

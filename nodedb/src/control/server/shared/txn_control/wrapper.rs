// SPDX-License-Identifier: BUSL-1.1

//! The transaction-control shape of one request of several statements.
//!
//! HTTP, NDJSON and WebSocket RPC hold no session across requests. One
//! request already runs as one atomic transaction. So a `BEGIN` that opens
//! the request and a `COMMIT` or `ROLLBACK` that ends it add nothing. Every
//! other placement of transaction control needs a second transaction in the
//! same request, or a session. This module reads the shape only. The caller
//! owns the policy and its switch.
//!
//! Keyword detection is [`classify`]. A spelling outside its grammar, such as
//! `COMMIT AND CHAIN`, reads as no transaction control here. `SET` and
//! `RESET` read as no transaction control too. The request planner's solo
//! check refuses both kinds with SQLSTATE 0A000.

use super::classify::{TxnControl, classify};
use super::modes::TxnModes;

/// The transaction-control shape of a request's statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnWrapper {
    /// No statement is transaction control.
    None,
    /// The first statement is `BEGIN` or `START TRANSACTION` with no mode.
    /// The last is `COMMIT` or `END` (`commit` is true) or `ROLLBACK` or
    /// `ABORT` (`commit` is false). No other statement is transaction
    /// control.
    Wrapped { commit: bool },
    /// Transaction control in any other place: a `COMMIT` between two
    /// statements, a `BEGIN` with no end, a `BEGIN` with a mode, a
    /// `SAVEPOINT`, a nested `BEGIN`, or a request of one such statement.
    Unsupported,
}

/// Read the transaction-control shape of `statements`, the request text split
/// at top-level semicolons.
pub fn classify_txn_wrapper(statements: &[String]) -> TxnWrapper {
    let controls: Vec<Option<TxnControl>> = statements
        .iter()
        .map(|statement| classify(statement))
        .collect();
    if controls.iter().all(Option::is_none) {
        return TxnWrapper::None;
    }
    let [first, middle @ .., last] = controls.as_slice() else {
        return TxnWrapper::Unsupported;
    };
    let opens = matches!(first, Some(TxnControl::Begin(modes)) if *modes == TxnModes::default());
    let commit = match last {
        Some(TxnControl::Commit) => true,
        Some(TxnControl::Rollback) => false,
        _ => return TxnWrapper::Unsupported,
    };
    if !opens || middle.iter().any(Option::is_some) {
        return TxnWrapper::Unsupported;
    }
    TxnWrapper::Wrapped { commit }
}

/// How many statements of `statements` sit before the inner statements: 1
/// when `BEGIN` and an end wrap them, else 0.
pub fn wrapper_offset(statements: &[String]) -> usize {
    match classify_txn_wrapper(statements) {
        TxnWrapper::Wrapped { .. } => 1,
        TxnWrapper::None | TxnWrapper::Unsupported => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(statements: &[&str]) -> TxnWrapper {
        let statements: Vec<String> = statements.iter().map(|s| (*s).to_owned()).collect();
        classify_txn_wrapper(&statements)
    }

    const A: &str = "INSERT INTO t (id) VALUES ('a')";
    const B: &str = "INSERT INTO t (id) VALUES ('b')";

    #[test]
    fn a_request_with_no_transaction_control_is_none() {
        assert_eq!(shape(&[]), TxnWrapper::None);
        assert_eq!(shape(&[A]), TxnWrapper::None);
        assert_eq!(shape(&[A, B]), TxnWrapper::None);
        assert_eq!(shape(&["", ";", A]), TxnWrapper::None);
    }

    /// The whole-request wrapper.
    #[test]
    fn begin_and_an_end_around_the_request_wrap_it() {
        assert_eq!(
            shape(&["BEGIN", A, B, "COMMIT"]),
            TxnWrapper::Wrapped { commit: true }
        );
        assert_eq!(
            shape(&["BEGIN", A, B, "ROLLBACK"]),
            TxnWrapper::Wrapped { commit: false }
        );
        assert_eq!(
            shape(&["BEGIN", "COMMIT"]),
            TxnWrapper::Wrapped { commit: true }
        );
    }

    #[test]
    fn every_spelling_of_begin_and_the_end_wraps() {
        for (open, end, commit) in [
            ("begin", "commit", true),
            ("START TRANSACTION", "END", true),
            ("start transaction", "end transaction", true),
            ("BEGIN WORK", "COMMIT WORK", true),
            ("BEGIN TRANSACTION", "COMMIT AND NO CHAIN", true),
            ("BEGIN", "ABORT", false),
            ("Begin", "rollback work", false),
            ("-- open\nBEGIN", "/* end */ COMMIT", true),
            ("BEGIN;", "COMMIT ;", true),
        ] {
            assert_eq!(
                shape(&[open, A, end]),
                TxnWrapper::Wrapped { commit },
                "{open} .. {end}"
            );
        }
    }

    /// A COMMIT between two statements.
    #[test]
    fn a_commit_in_the_middle_is_unsupported() {
        assert_eq!(shape(&[A, "COMMIT", B]), TxnWrapper::Unsupported);
        assert_eq!(
            shape(&["BEGIN", A, "COMMIT", B, "COMMIT"]),
            TxnWrapper::Unsupported
        );
        assert_eq!(shape(&[A, "COMMIT"]), TxnWrapper::Unsupported);
    }

    /// A BEGIN with no end, and a SAVEPOINT.
    #[test]
    fn a_begin_with_no_end_or_a_savepoint_is_unsupported() {
        assert_eq!(shape(&["BEGIN", A]), TxnWrapper::Unsupported);
        assert_eq!(shape(&["BEGIN", A, B]), TxnWrapper::Unsupported);
        assert_eq!(shape(&["SAVEPOINT s", A]), TxnWrapper::Unsupported);
        assert_eq!(
            shape(&["BEGIN", "SAVEPOINT s", A, "COMMIT"]),
            TxnWrapper::Unsupported
        );
        assert_eq!(
            shape(&["BEGIN", A, "RELEASE s", "COMMIT"]),
            TxnWrapper::Unsupported
        );
        assert_eq!(
            shape(&["BEGIN", A, "ROLLBACK TO s"]),
            TxnWrapper::Unsupported
        );
    }

    #[test]
    fn a_begin_with_a_mode_is_unsupported() {
        for open in [
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "BEGIN READ ONLY",
            "START TRANSACTION READ WRITE",
            "BEGIN NOT DEFERRABLE",
        ] {
            assert_eq!(
                shape(&[open, A, "COMMIT"]),
                TxnWrapper::Unsupported,
                "{open}"
            );
        }
    }

    #[test]
    fn a_nested_begin_is_unsupported() {
        assert_eq!(
            shape(&["BEGIN", "BEGIN", A, "COMMIT"]),
            TxnWrapper::Unsupported
        );
        assert_eq!(
            shape(&["BEGIN", A, "COMMIT", "COMMIT"]),
            TxnWrapper::Unsupported
        );
    }

    #[test]
    fn an_end_with_no_begin_or_a_reversed_wrapper_is_unsupported() {
        assert_eq!(shape(&[A, B, "COMMIT"]), TxnWrapper::Unsupported);
        assert_eq!(shape(&["COMMIT", A, "BEGIN"]), TxnWrapper::Unsupported);
        assert_eq!(shape(&["ROLLBACK", A]), TxnWrapper::Unsupported);
    }

    #[test]
    fn the_offset_skips_only_a_wrapping_begin() {
        let owned =
            |list: &[&str]| -> Vec<String> { list.iter().map(|s| (*s).to_owned()).collect() };
        assert_eq!(wrapper_offset(&owned(&["BEGIN", A, "COMMIT"])), 1);
        assert_eq!(wrapper_offset(&owned(&["BEGIN", A, B])), 0);
        assert_eq!(wrapper_offset(&owned(&[A, B])), 0);
    }

    /// The caller skips a request of one statement. The detector reads it
    /// as unsupported.
    #[test]
    fn one_transaction_control_statement_alone_is_unsupported() {
        assert_eq!(shape(&["BEGIN"]), TxnWrapper::Unsupported);
        assert_eq!(shape(&["COMMIT"]), TxnWrapper::Unsupported);
    }

    /// `SET` and `RESET` are no transaction control here. The
    /// planner's solo check refuses them.
    #[test]
    fn a_session_command_is_not_read_here() {
        assert_eq!(shape(&["SET x = 1", A]), TxnWrapper::None);
        assert_eq!(shape(&[A, "RESET x"]), TxnWrapper::None);
        assert_eq!(
            shape(&["BEGIN", "SET x = 1", "COMMIT"]),
            TxnWrapper::Wrapped { commit: true }
        );
    }

    /// A spelling outside the classifier grammar reads as no transaction
    /// control. The planner's solo check refuses it by its first word.
    #[test]
    fn a_spelling_outside_the_grammar_is_not_read_here() {
        assert_eq!(shape(&[A, "COMMIT AND CHAIN"]), TxnWrapper::None);
        assert_eq!(
            shape(&["BEGIN", A, "COMMIT AND CHAIN"]),
            TxnWrapper::Unsupported
        );
    }
}

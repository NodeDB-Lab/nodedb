// SPDX-License-Identifier: BUSL-1.1

//! The one classifier for transaction-control statements on pgwire and the
//! native protocol.
//!
//! The pgwire Parse gate, DSL passthrough check, Execute gate and
//! simple-query execution arms call [`classify`]. The native SQL dispatch
//! calls it too. Every path agrees on which statements end or change a
//! transaction block. Keywords match in
//! any letter case. `WORK` and `TRANSACTION` are optional noise words, and
//! `SAVEPOINT` is optional after `RELEASE` and `ROLLBACK TO`.

use super::modes::{TxnModes, parse_modes};
use super::tokens::{Token, tokenize};

/// A transaction-control statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnControl {
    /// `BEGIN [WORK|TRANSACTION] [modes]` or `START TRANSACTION [modes]`.
    Begin(TxnModes),
    /// `COMMIT` or `END`, with optional `WORK|TRANSACTION` and `AND NO CHAIN`.
    Commit,
    /// `ROLLBACK` or `ABORT`, with optional `WORK|TRANSACTION` and `AND NO CHAIN`.
    Rollback,
    /// `SAVEPOINT name`.
    Savepoint(String),
    /// `RELEASE [SAVEPOINT] name`.
    Release(String),
    /// `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] name`.
    RollbackTo(String),
}

/// Classify `sql` as transaction control, or return `None` for any other
/// statement. A spelling outside the grammar, such as `COMMIT AND CHAIN` or
/// `BEGIN` with an unknown mode, returns `None`.
pub fn classify(sql: &str) -> Option<TxnControl> {
    let tokens = tokenize(sql)?;
    let (head, rest) = tokens.split_first()?;
    let Token::Word { upper, .. } = head else {
        return None;
    };
    match upper.as_str() {
        "BEGIN" => parse_modes(skip_noise_word(rest)).map(TxnControl::Begin),
        "START" => {
            let (first, modes) = rest.split_first()?;
            if !first.is_keyword("TRANSACTION") {
                return None;
            }
            parse_modes(modes).map(TxnControl::Begin)
        }
        "COMMIT" | "END" => no_chain_tail(skip_noise_word(rest)).then_some(TxnControl::Commit),
        "ABORT" => no_chain_tail(skip_noise_word(rest)).then_some(TxnControl::Rollback),
        "ROLLBACK" => {
            let rest = skip_noise_word(rest);
            if let Some((first, target)) = rest.split_first()
                && first.is_keyword("TO")
            {
                return savepoint_name(target, true).map(TxnControl::RollbackTo);
            }
            no_chain_tail(rest).then_some(TxnControl::Rollback)
        }
        "SAVEPOINT" => savepoint_name(rest, false).map(TxnControl::Savepoint),
        "RELEASE" => savepoint_name(rest, true).map(TxnControl::Release),
        _ => None,
    }
}

/// Drop one leading `WORK` or `TRANSACTION`.
fn skip_noise_word(tokens: &[Token]) -> &[Token] {
    match tokens.split_first() {
        Some((first, rest)) if first.is_keyword("WORK") || first.is_keyword("TRANSACTION") => rest,
        _ => tokens,
    }
}

/// Return true if `tokens` is empty or exactly `AND NO CHAIN`.
fn no_chain_tail(tokens: &[Token]) -> bool {
    match tokens {
        [] => true,
        [and, no, chain] => {
            and.is_keyword("AND") && no.is_keyword("NO") && chain.is_keyword("CHAIN")
        }
        _ => false,
    }
}

/// The savepoint name `tokens` holds: one identifier, after an optional
/// `SAVEPOINT` keyword when `keyword_optional` is set. An unquoted name is
/// folded to lower case, as PostgreSQL folds identifiers.
fn savepoint_name(tokens: &[Token], keyword_optional: bool) -> Option<String> {
    let tokens = match tokens {
        [first, rest @ ..]
            if keyword_optional && first.is_keyword("SAVEPOINT") && !rest.is_empty() =>
        {
            rest
        }
        _ => tokens,
    };
    match tokens {
        [name] => name.identifier().map(str::to_owned),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::modes::{AccessMode, IsolationLevel};
    use super::*;

    fn begin() -> Option<TxnControl> {
        Some(TxnControl::Begin(TxnModes::default()))
    }

    #[test]
    fn begin_spellings() {
        for sql in [
            "BEGIN",
            "begin;",
            "Begin Work",
            "BEGIN TRANSACTION",
            "START TRANSACTION",
            "start transaction ;",
        ] {
            assert_eq!(classify(sql), begin(), "{sql}");
        }
    }

    #[test]
    fn begin_carries_modes() {
        let Some(TxnControl::Begin(modes)) =
            classify("START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY")
        else {
            panic!("START TRANSACTION with modes must classify as Begin");
        };
        assert_eq!(modes.isolation, Some(IsolationLevel::Serializable));
        assert_eq!(modes.access, Some(AccessMode::ReadOnly));
        let Some(TxnControl::Begin(modes)) = classify("begin work read write") else {
            panic!("BEGIN WORK with modes must classify as Begin");
        };
        assert_eq!(modes.access, Some(AccessMode::ReadWrite));
    }

    #[test]
    fn commit_spellings() {
        for sql in [
            "COMMIT",
            "commit work",
            "COMMIT TRANSACTION;",
            "END",
            "end work",
            "END TRANSACTION",
            "COMMIT AND NO CHAIN",
        ] {
            assert_eq!(classify(sql), Some(TxnControl::Commit), "{sql}");
        }
    }

    #[test]
    fn rollback_spellings() {
        for sql in [
            "ROLLBACK",
            "rollback work",
            "ROLLBACK TRANSACTION",
            "ABORT",
            "abort work",
            "ABORT TRANSACTION;",
            "ROLLBACK AND NO CHAIN",
            "ROLLBACK -- end the block\n",
        ] {
            assert_eq!(classify(sql), Some(TxnControl::Rollback), "{sql}");
        }
    }

    #[test]
    fn savepoint_spellings() {
        assert_eq!(
            classify("SAVEPOINT s1"),
            Some(TxnControl::Savepoint("s1".to_owned()))
        );
        assert_eq!(
            classify("savepoint S1;"),
            Some(TxnControl::Savepoint("s1".to_owned()))
        );
        assert_eq!(
            classify(r#"SAVEPOINT "Mixed""#),
            Some(TxnControl::Savepoint("Mixed".to_owned()))
        );
        for sql in ["RELEASE SAVEPOINT s1", "release s1", "RELEASE S1;"] {
            assert_eq!(
                classify(sql),
                Some(TxnControl::Release("s1".to_owned())),
                "{sql}"
            );
        }
        for sql in [
            "ROLLBACK TO SAVEPOINT s1",
            "rollback to s1",
            "ROLLBACK WORK TO SAVEPOINT s1",
            "ROLLBACK TRANSACTION TO s1;",
        ] {
            assert_eq!(
                classify(sql),
                Some(TxnControl::RollbackTo("s1".to_owned())),
                "{sql}"
            );
        }
        // `savepoint` is a legal savepoint name.
        assert_eq!(
            classify("RELEASE savepoint"),
            Some(TxnControl::Release("savepoint".to_owned()))
        );
        assert_eq!(
            classify("ROLLBACK TO SAVEPOINT"),
            Some(TxnControl::RollbackTo("savepoint".to_owned()))
        );
    }

    /// Both protocols accept every isolation level on `START TRANSACTION`
    /// and refuse `DEFERRABLE` with one message.
    #[test]
    fn start_transaction_refusal_message() {
        let Some(TxnControl::Begin(modes)) =
            classify("start transaction isolation level repeatable read;")
        else {
            panic!("START TRANSACTION with an isolation level must classify as Begin");
        };
        assert_eq!(modes.refusal("BEGIN"), None);
        let Some(TxnControl::Begin(deferrable)) = classify("start transaction deferrable") else {
            panic!("START TRANSACTION DEFERRABLE must classify as Begin");
        };
        assert_eq!(
            deferrable.refusal("BEGIN").as_deref(),
            Some("BEGIN DEFERRABLE is not supported")
        );
    }

    #[test]
    fn other_statements_are_not_transaction_control() {
        for sql in [
            "",
            ";",
            "SELECT 1",
            "BEGINNING",
            "BEGIN FOO",
            "START",
            "START TRANSACTION ISOLATION LEVEL",
            "COMMIT AND CHAIN",
            "COMMIT PREPARED 'x'",
            "COMMIT OFFSET PARTITION 1 AT 1:1:1 ON s CONSUMER GROUP g",
            "COMMIT OFFSETS ON s CONSUMER GROUP g",
            "ABORT TO s1",
            "ROLLBACK TO",
            "SAVEPOINT",
            "SAVEPOINT a b",
            "RELEASE",
            "BEGIN; SELECT 1",
            "COMMIT WORK WORK",
        ] {
            assert_eq!(classify(sql), None, "{sql}");
        }
    }
}

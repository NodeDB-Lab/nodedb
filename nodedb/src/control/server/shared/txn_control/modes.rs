// SPDX-License-Identifier: BUSL-1.1

//! Transaction modes on `BEGIN` and `START TRANSACTION`, and the policy that
//! accepts or refuses them.
//!
//! NodeDB runs every transaction as a serializable snapshot transaction.
//! `SERIALIZABLE`, `REPEATABLE READ`, `READ COMMITTED` and `READ UNCOMMITTED`
//! are all accepted and all run serializable, because PostgreSQL permits a
//! stronger level than the one requested. `DEFERRABLE` is refused with
//! SQLSTATE 0A000.

use super::tokens::Token;

/// A PostgreSQL isolation level named in a transaction mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    /// The level as SQL spells it.
    pub fn as_sql(self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// A transaction access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

impl AccessMode {
    /// The session parameter value `SET TRANSACTION` stores for this mode.
    pub fn parameter_value(self) -> &'static str {
        match self {
            AccessMode::ReadOnly => "read_only",
            AccessMode::ReadWrite => "read_write",
        }
    }
}

/// The modes a `BEGIN` or `START TRANSACTION` statement names. A mode the
/// statement omits is `None`. A later mode of the same kind replaces an
/// earlier one, as in PostgreSQL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxnModes {
    pub isolation: Option<IsolationLevel>,
    pub access: Option<AccessMode>,
    pub deferrable: Option<bool>,
}

impl TxnModes {
    /// The reason NodeDB refuses these modes, or `None` when it runs them.
    /// Every isolation level runs serializable. `command` names the
    /// statement in the message.
    pub fn refusal(&self, command: &str) -> Option<String> {
        if self.deferrable == Some(true) {
            return Some(format!("{command} DEFERRABLE is not supported"));
        }
        None
    }
}

/// Parse the transaction-mode list that follows `BEGIN [WORK|TRANSACTION]` or
/// `START TRANSACTION`. Modes are separated by commas or whitespace. Returns
/// `None` for any token outside the mode grammar.
pub(super) fn parse_modes(tokens: &[Token]) -> Option<TxnModes> {
    let mut modes = TxnModes::default();
    let mut rest = tokens;
    let mut expect_mode = true;
    while let Some((first, tail)) = rest.split_first() {
        if *first == Token::Comma {
            if expect_mode {
                return None;
            }
            expect_mode = true;
            rest = tail;
            continue;
        }
        rest = parse_one_mode(rest, &mut modes)?;
        expect_mode = false;
    }
    // A trailing comma leaves the list open.
    if expect_mode && !tokens.is_empty() {
        return None;
    }
    Some(modes)
}

/// Parse one mode at the head of `tokens` into `modes` and return the tokens
/// after it.
fn parse_one_mode<'a>(tokens: &'a [Token], modes: &mut TxnModes) -> Option<&'a [Token]> {
    let keyword = |i: usize, word: &str| tokens.get(i).is_some_and(|t| t.is_keyword(word));
    if keyword(0, "ISOLATION") && keyword(1, "LEVEL") {
        let (level, used) = if keyword(2, "SERIALIZABLE") {
            (IsolationLevel::Serializable, 3)
        } else if keyword(2, "REPEATABLE") && keyword(3, "READ") {
            (IsolationLevel::RepeatableRead, 4)
        } else if keyword(2, "READ") && keyword(3, "COMMITTED") {
            (IsolationLevel::ReadCommitted, 4)
        } else if keyword(2, "READ") && keyword(3, "UNCOMMITTED") {
            (IsolationLevel::ReadUncommitted, 4)
        } else {
            return None;
        };
        modes.isolation = Some(level);
        return tokens.get(used..);
    }
    if keyword(0, "READ") && keyword(1, "ONLY") {
        modes.access = Some(AccessMode::ReadOnly);
        return tokens.get(2..);
    }
    if keyword(0, "READ") && keyword(1, "WRITE") {
        modes.access = Some(AccessMode::ReadWrite);
        return tokens.get(2..);
    }
    if keyword(0, "DEFERRABLE") {
        modes.deferrable = Some(true);
        return tokens.get(1..);
    }
    if keyword(0, "NOT") && keyword(1, "DEFERRABLE") {
        modes.deferrable = Some(false);
        return tokens.get(2..);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::tokens::tokenize;
    use super::*;

    fn modes(sql: &str) -> Option<TxnModes> {
        parse_modes(&tokenize(sql).expect("tokenizes"))
    }

    #[test]
    fn parses_every_mode_with_and_without_commas() {
        let parsed = modes("isolation level repeatable read, read only not deferrable")
            .expect("valid modes");
        assert_eq!(parsed.isolation, Some(IsolationLevel::RepeatableRead));
        assert_eq!(parsed.access, Some(AccessMode::ReadOnly));
        assert_eq!(parsed.deferrable, Some(false));
        assert_eq!(modes(""), Some(TxnModes::default()));
    }

    #[test]
    fn refuses_malformed_mode_lists() {
        assert_eq!(modes("ISOLATION LEVEL"), None);
        assert_eq!(modes("READ ONLY,"), None);
        assert_eq!(modes(", READ ONLY"), None);
        assert_eq!(modes("READ ONLY,, READ WRITE"), None);
        assert_eq!(modes("ISOLATION LEVEL SNAPSHOT"), None);
    }

    #[test]
    fn every_isolation_level_is_accepted() {
        for level in [
            "SERIALIZABLE",
            "REPEATABLE READ",
            "READ COMMITTED",
            "READ UNCOMMITTED",
        ] {
            let accepted = modes(&format!("ISOLATION LEVEL {level}, READ WRITE")).expect("valid");
            assert_eq!(accepted.refusal("BEGIN"), None, "{level}");
        }
    }

    #[test]
    fn deferrable_is_refused() {
        let deferrable = modes("DEFERRABLE").expect("valid");
        assert_eq!(
            deferrable.refusal("BEGIN").as_deref(),
            Some("BEGIN DEFERRABLE is not supported")
        );
        let not_deferrable = modes("NOT DEFERRABLE").expect("valid");
        assert_eq!(not_deferrable.refusal("BEGIN"), None);
    }
}

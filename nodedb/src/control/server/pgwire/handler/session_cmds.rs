// SPDX-License-Identifier: BUSL-1.1

//! Session parameter commands: SET, SHOW, SHOW ALL, EXPLAIN.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;

use super::super::types::sqlstate_error;
use super::core::NodeDbPgHandler;

/// Outcome of classifying a `SET TRANSACTION` / `SET SESSION CHARACTERISTICS` command.
enum TransactionCmd {
    /// `READ ONLY` — store access mode, return SET.
    SetReadOnly,
    /// `READ WRITE` — store access mode, return SET.
    SetReadWrite,
    /// `ISOLATION LEVEL SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED`.
    /// Accepted, and the transaction runs serializable.
    AcceptIsolation,
    /// An unknown isolation level or option — reject with SQLSTATE 0A000.
    RejectIsolation(String),
}

/// Classify a `SET TRANSACTION` or `SET SESSION CHARACTERISTICS` SQL statement.
///
/// `upper` must be `sql.to_uppercase()`. `sql` is the original, used for error messages.
fn classify_transaction_cmd(upper: &str, sql: &str) -> TransactionCmd {
    // Isolation-level branch: check before READ ONLY/READ WRITE so that a statement
    // like "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED" does not accidentally
    // match the READ-only access-mode branch.
    if upper.contains("ISOLATION LEVEL") {
        let named = [
            "SERIALIZABLE",
            "REPEATABLE READ",
            "READ COMMITTED",
            "READ UNCOMMITTED",
        ]
        .iter()
        .any(|level| upper.contains(level));
        if named {
            return TransactionCmd::AcceptIsolation;
        }
        return TransactionCmd::RejectIsolation(format!(
            "unsupported SET TRANSACTION option: {}",
            sql.split_whitespace().skip(2).collect::<Vec<_>>().join(" ")
        ));
    }

    // Access-mode branch.
    if upper.contains("READ ONLY") {
        return TransactionCmd::SetReadOnly;
    }
    if upper.contains("READ WRITE") {
        return TransactionCmd::SetReadWrite;
    }

    // Unknown option.
    TransactionCmd::RejectIsolation(format!(
        "unsupported SET TRANSACTION option: {}",
        sql.split_whitespace().skip(2).collect::<Vec<_>>().join(" ")
    ))
}

impl NodeDbPgHandler {
    /// Handle SET commands: parse, validate, store in session.
    pub(super) fn handle_set(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use crate::control::server::shared::session::parse_set_command;
        use pgwire::api::results::Tag;

        // Handle SET TRANSACTION ... and SET SESSION CHARACTERISTICS AS TRANSACTION ...
        let upper = sql.to_uppercase();
        if upper.starts_with("SET TRANSACTION") || upper.starts_with("SET SESSION CHARACTERISTICS")
        {
            match classify_transaction_cmd(&upper, sql) {
                TransactionCmd::SetReadOnly => {
                    self.sessions.set_parameter(
                        session_id,
                        "transaction_access_mode".into(),
                        "read_only".into(),
                    );
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::SetReadWrite => {
                    self.sessions.set_parameter(
                        session_id,
                        "transaction_access_mode".into(),
                        "read_write".into(),
                    );
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::AcceptIsolation => {
                    return Ok(vec![Response::Execution(Tag::new("SET"))]);
                }
                TransactionCmd::RejectIsolation(message) => {
                    return Err(sqlstate_error(
                        nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                        &message,
                    ));
                }
            }
        }

        // `SET ROLE <name>` and `SET SESSION AUTHORIZATION '<name>'` use
        // PostgreSQL's space-not-equals syntax, so `parse_set_command` (which
        // splits on `=` / `TO`) returns `None` for them. Catch the keywords
        // before falling through to that parser — both must reject explicitly
        // rather than land on the silent success path (the root cause behind
        // SET TENANT looking like a no-op).
        if upper.starts_with("SET ROLE ") || upper == "SET ROLE" {
            return Err(sqlstate_error(
                nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "SET ROLE is not supported: a session's role set is identity-bound \
                 at CREATE USER time. Use GRANT/REVOKE ROLE TO <user> to change \
                 a user's roles, or reconnect with a different user.",
            ));
        }
        if upper.starts_with("SET SESSION AUTHORIZATION") {
            return Err(sqlstate_error(
                nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "SET SESSION AUTHORIZATION is not supported: identity is bound at \
                 connection time. Reconnect as the target user.",
            ));
        }

        let (key, value) = match parse_set_command(sql) {
            Some(kv) => kv,
            None => {
                // Statements that look like `SET <something>` but don't match
                // any of the recognized shapes (k=v, k TO v, TRANSACTION,
                // ROLE, SESSION AUTHORIZATION) must NOT silently succeed.
                // Silent success on unparsed SET is exactly the bug class
                // that allowed `SET TENANT = 'x'` to look like a no-op.
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("syntax error in SET command: {sql}"),
                ))));
            }
        };

        // Identity / security context keys are dispatched before the
        // generic store-in-session path. Storing them in the parameter bag
        // without an enforcement contract is the silent-no-op class — every
        // such key must either be honored end-to-end or rejected explicitly.
        match key.as_str() {
            "tenant" => {
                return self.handle_set_tenant_name_or_id(identity, session_id, &value);
            }
            "nodedb.tenant_id" => {
                return self.handle_set_tenant_by_id(identity, session_id, &value);
            }
            "role" => {
                return Err(sqlstate_error(
                    nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                    "SET ROLE is not supported: a session's role set is identity-bound \
                     at CREATE USER time. Use GRANT/REVOKE ROLE TO <user> to change \
                     a user's roles, or reconnect with a different user.",
                ));
            }
            "session_authorization" => {
                return Err(sqlstate_error(
                    nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                    "SET SESSION AUTHORIZATION is not supported: identity is bound at \
                     connection time. Reconnect as the target user.",
                ));
            }
            _ => {}
        }

        // Eager validation for `nodedb.auth_session`: drive the resolve path
        // now so rate-limit / audit / fingerprint checks fire on each SET
        // rather than being deferred to the next query. A probing client
        // that hammers SET LOCAL with bogus handles and never runs a query
        // must still be throttled and observed.
        if key == "nodedb.auth_session" {
            use crate::control::security::session_handle::{ClientFingerprint, ResolveOutcome};
            let peer_addr = match session_id {
                SessionId::Connection(connection_id) => self
                    .sessions
                    .connection_metadata(connection_id)
                    .map(|metadata| metadata.peer_addr),
                SessionId::LegacySocket(peer_addr) => Some(peer_addr),
            }
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
                    "connection metadata is missing".to_owned(),
                )))
            })?;
            let caller_fp = ClientFingerprint::from_peer(identity.tenant_id, &peer_addr);
            let conn_key = match session_id {
                SessionId::Connection(connection_id) => connection_id.to_string(),
                SessionId::LegacySocket(peer_addr) => peer_addr.to_string(),
            };
            match self
                .state
                .session_handles
                .resolve(&value, &conn_key, &caller_fp)
            {
                ResolveOutcome::RateLimited => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "53300".to_owned(),
                        "session handle resolve rate limit exceeded on this \
                         connection — closing"
                            .to_owned(),
                    ))));
                }
                ResolveOutcome::Resolved(_) | ResolveOutcome::Miss => {
                    // Store the raw value either way — Miss might be a
                    // handle that was valid previously and expired; the
                    // next query's resolve will fall back to base identity.
                }
            }
        }

        // Any key that reaches this point must be a known runtime parameter
        // carrying a value its own grammar accepts. The check is the shared
        // one every protocol applies, so a name pgwire refuses is refused on
        // the native protocol too — silent storage on either is the class of
        // bug that allowed `SET TENANT` to look successful while routing
        // nothing.
        if let Err(error) =
            crate::control::server::shared::session::validate_set_parameter(&key, &value)
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                error.sqlstate().to_owned(),
                error.to_string(),
            ))));
        }

        self.sessions.set_parameter(session_id, key, value);
        Ok(vec![Response::Execution(Tag::new("SET"))])
    }
}

#[cfg(test)]
mod tests {
    use super::{TransactionCmd, classify_transaction_cmd};

    /// tenant_id values above u32::MAX must parse without error via u64.
    #[test]
    fn tenant_id_above_u32_max_parses_as_u64() {
        let big = "4294967296"; // u32::MAX + 1
        assert!(big.parse::<u64>().is_ok(), "should parse as u64");
        assert!(big.parse::<u32>().is_err(), "should NOT parse as u32");
    }

    fn run(sql: &str) -> TransactionCmd {
        let upper = sql.to_uppercase();
        classify_transaction_cmd(&upper, sql)
    }

    fn is_accept(cmd: TransactionCmd) -> bool {
        matches!(
            cmd,
            TransactionCmd::SetReadOnly
                | TransactionCmd::SetReadWrite
                | TransactionCmd::AcceptIsolation
        )
    }

    fn rejection_code(cmd: TransactionCmd) -> Option<String> {
        match cmd {
            TransactionCmd::RejectIsolation(msg) => Some(msg),
            _ => None,
        }
    }

    #[test]
    fn set_transaction_read_only() {
        assert!(is_accept(run("SET TRANSACTION READ ONLY")));
        assert!(matches!(
            run("SET TRANSACTION READ ONLY"),
            TransactionCmd::SetReadOnly
        ));
    }

    #[test]
    fn set_transaction_read_write() {
        assert!(matches!(
            run("SET TRANSACTION READ WRITE"),
            TransactionCmd::SetReadWrite
        ));
    }

    #[test]
    fn set_transaction_accepts_every_isolation_level() {
        for level in [
            "SERIALIZABLE",
            "REPEATABLE READ",
            "READ COMMITTED",
            "READ UNCOMMITTED",
        ] {
            assert!(
                matches!(
                    run(&format!("SET TRANSACTION ISOLATION LEVEL {level}")),
                    TransactionCmd::AcceptIsolation
                ),
                "{level}"
            );
        }
    }

    #[test]
    fn set_session_characteristics_serializable() {
        let sql = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        assert!(matches!(run(sql), TransactionCmd::AcceptIsolation));
    }

    #[test]
    fn set_transaction_unknown_isolation_level_is_rejected() {
        let msg = rejection_code(run("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"))
            .expect("expected rejection");
        assert!(msg.contains("unsupported"), "{msg}");
    }

    #[test]
    fn set_transaction_unknown_option() {
        let msg = rejection_code(run("SET TRANSACTION DEFERRABLE"))
            .expect("expected rejection for unknown option");
        assert!(
            msg.contains("unsupported"),
            "message should say unsupported: {msg}"
        );
    }
}

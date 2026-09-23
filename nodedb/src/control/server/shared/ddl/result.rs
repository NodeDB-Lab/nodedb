// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch result types.
//!
//! These carry no pgwire wire types, so every server entrypoint (native,
//! http, pgwire, RESP) can encode from them without depending on the pgwire
//! `Response` representation.

use nodedb_types::error::{ErrorCode, ErrorDetails, sqlstate};

use crate::control::server::response_shape::types::ShapedRows;

/// Protocol-neutral result of a DDL dispatch, encoded per-entrypoint.
#[derive(Debug, Clone)]
pub enum DdlResult {
    /// A command tag (e.g. "CREATE TABLE"), optional affected-row count.
    Status {
        command: String,
        rows_affected: Option<u64>,
    },
    /// A row-returning result (SHOW / EXPLAIN / introspection).
    Rows(ShapedRows),
    /// An empty query.
    Empty,
}

/// Protocol-neutral DDL error: ANSI SQLSTATE + numeric [`ErrorCode`] +
/// message (every entrypoint encodes from this).
///
/// `code` is the classification a client actually programs against
/// (`is_not_found()`, `is_retriable()`, …); `sqlstate` stays for
/// PostgreSQL-wire compatibility. The two are independent because a
/// SQLSTATE alone does not determine the code — several SQLSTATEs
/// (`0A000`, `55006`, `57014`, `XX000`, `02000`) are shared by more than one
/// `ErrorCode` meaning. Construct through [`DdlError::new`] for a SQLSTATE
/// with one unambiguous meaning; the ambiguous ones can only be built
/// through their dedicated constructor below, because their SQLSTATE
/// constant has type [`sqlstate::AmbiguousSqlstate`], not `&str`, so
/// `DdlError::new` (which takes `&str`) rejects them at compile time.
#[derive(Debug, Clone)]
pub struct DdlError {
    pub sqlstate: String,
    pub code: ErrorCode,
    pub message: String,
    /// The structured details of a typed verdict: the collection, gate, or
    /// document it names. `None` for an error built from a SQLSTATE alone.
    pub details: Option<Box<ErrorDetails>>,
}

impl DdlError {
    /// Build a `DdlError` from a SQLSTATE with one unambiguous `ErrorCode`
    /// meaning, deriving `code` from [`code_for_sqlstate`]. This is the
    /// common-path constructor nearly every DDL error site uses.
    pub fn new(sqlstate: impl Into<String>, message: impl Into<String>) -> Self {
        let sqlstate = sqlstate.into();
        let code = code_for_sqlstate(&sqlstate);
        DdlError {
            sqlstate,
            code,
            message: message.into(),
            details: None,
        }
    }

    /// Build a `DdlError` from a classified public error: its code and its
    /// details travel with the SQLSTATE and message the SQL surfaces render.
    pub fn from_public(
        sqlstate: impl Into<String>,
        message: impl Into<String>,
        public: &nodedb_types::NodeDbError,
    ) -> Self {
        DdlError {
            sqlstate: sqlstate.into(),
            code: public.code(),
            message: message.into(),
            details: Some(Box::new(public.details().clone())),
        }
    }

    /// Build a `DdlError` with an explicit code, bypassing derivation.
    /// Used by the named constructors below for ambiguous SQLSTATEs.
    fn with_code(sqlstate: &'static str, code: ErrorCode, message: impl Into<String>) -> Self {
        DdlError {
            sqlstate: sqlstate.to_string(),
            code,
            message: message.into(),
            details: None,
        }
    }

    /// `DROP DATABASE` targeted the built-in `default` database.
    pub fn cannot_drop_default_database(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::CANNOT_DROP_DEFAULT_DATABASE.0,
            ErrorCode::CANNOT_DROP_DEFAULT_DATABASE,
            message,
        )
    }

    /// `CLONE DATABASE` targeted a mirror database.
    pub fn cannot_clone_mirror(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::CANNOT_CLONE_MIRROR.0,
            ErrorCode::CANNOT_CLONE_MIRROR,
            message,
        )
    }

    /// `DROP DATABASE` refused because clones depend on the source.
    pub fn clone_dependency(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::CLONE_DEPENDENCY.0,
            ErrorCode::CLONE_DEPENDENCY,
            message,
        )
    }

    /// A write targeted a `Shadowed`/`Materializing` clone collection whose
    /// engine has no copy-on-write support.
    pub fn clone_write_requires_materialize(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
            ErrorCode::CLONE_WRITE_REQUIRES_MATERIALIZE,
            message,
        )
    }

    /// `MOVE TENANT` drain phase timed out.
    pub fn move_tenant_drain_timeout(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::MOVE_TENANT_DRAIN_TIMEOUT.0,
            ErrorCode::MOVE_TENANT_DRAIN_TIMEOUT,
            message,
        )
    }

    /// `MOVE TENANT` snapshot phase failed; source left unchanged.
    pub fn move_tenant_snapshot_failed(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::MOVE_TENANT_SNAPSHOT_FAILED.0,
            ErrorCode::MOVE_TENANT_SNAPSHOT_FAILED,
            message,
        )
    }

    /// `MOVE TENANT` cutover phase failed; source still holds the data.
    pub fn move_tenant_cutover_failed(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::MOVE_TENANT_CUTOVER_FAILED.0,
            ErrorCode::MOVE_TENANT_CUTOVER_FAILED,
            message,
        )
    }

    /// `MOVE TENANT` is a no-op: the tenant is already at the target.
    pub fn move_tenant_already_at_target(message: impl Into<String>) -> Self {
        Self::with_code(
            sqlstate::MOVE_TENANT_ALREADY_AT_TARGET.0,
            ErrorCode::MOVE_TENANT_ALREADY_AT_TARGET,
            message,
        )
    }
}

/// The `ErrorCode` a SQLSTATE classifies to when it has exactly one
/// classification. The single source of truth [`DdlError::new`] and every
/// `ddl_err`/`err`-style local helper derive from — do not scatter a second
/// copy of this table.
///
/// SQLSTATEs whose `ErrorCode` depends on which call site emitted them
/// (`0A000`, `55006`, `57014`, `XX000`, `02000`) are deliberately absent:
/// their named constants have type [`sqlstate::AmbiguousSqlstate`], which
/// cannot reach this function (it takes `&str`), so a caller that needs one
/// of those meanings is forced to the matching `DdlError::<name>`
/// constructor instead of silently landing on this table's default for the
/// bare string.
pub fn code_for_sqlstate(sqlstate_str: &str) -> ErrorCode {
    match sqlstate_str {
        sqlstate::UNDEFINED_TABLE => ErrorCode::COLLECTION_NOT_FOUND,
        sqlstate::INVALID_CATALOG_NAME => ErrorCode::DATABASE_NOT_FOUND,
        sqlstate::INSUFFICIENT_PRIVILEGE => ErrorCode::AUTHORIZATION_DENIED,
        sqlstate::UNDEFINED_FUNCTION => ErrorCode::UNDEFINED_FUNCTION,
        sqlstate::UNDEFINED_COLUMN => ErrorCode::UNDEFINED_COLUMN,
        sqlstate::AMBIGUOUS_COLUMN => ErrorCode::AMBIGUOUS_COLUMN,
        // A malformed request and a plan that cannot be built both render as
        // `42601`; both are non-retriable client errors, so one code covers
        // both without losing anything a client acts on.
        sqlstate::SYNTAX_ERROR => ErrorCode::BAD_REQUEST,
        sqlstate::SERIALIZATION_FAILURE => ErrorCode::WRITE_CONFLICT,
        sqlstate::TOO_MANY_CONNECTIONS => ErrorCode::RATE_EXCEEDED,
        sqlstate::INTERNAL_ERROR => ErrorCode::INTERNAL,
        // `0A000` here means the default, unambiguous "feature not
        // supported" case — the ambiguous named meanings sharing this
        // string (`CANNOT_DROP_DEFAULT_DATABASE`, `CANNOT_CLONE_MIRROR`)
        // cannot reach this function; see the doc comment above.
        sqlstate::FEATURE_NOT_SUPPORTED => ErrorCode::SQL_NOT_ENABLED,
        "42704" => ErrorCode::UNDEFINED_OBJECT,
        "42710" | "42P07" | "42723" => ErrorCode::ALREADY_EXISTS,
        // Invalid/incompatible object definition or a caller reaching a
        // dependent object still in use — all client-actionable, non-retriable.
        "42P17" | "42809" | "42P16" | "2BP01" => ErrorCode::BAD_REQUEST,
        // A declared literal the column type cannot represent.
        sqlstate::DATATYPE_MISMATCH => ErrorCode::BAD_REQUEST,
        // Default "object not in prerequisite state" meaning of `55006`;
        // `CLONE_DEPENDENCY` and `CLONE_WRITE_REQUIRES_MATERIALIZE` are
        // ambiguous-typed and cannot reach this function.
        "55000" | "55006" => ErrorCode::OBJECT_NOT_READY,
        // Default "no data found" meaning of `02000`;
        // `MOVE_TENANT_ALREADY_AT_TARGET` is ambiguous-typed.
        "02000" => ErrorCode::NOT_FOUND,
        sqlstate::CONFIGURATION_LIMIT_EXCEEDED => ErrorCode::QUOTA_OVERCOMMIT,
        sqlstate::IO_ERROR => ErrorCode::STORAGE,
        sqlstate::CONNECTION_FAILURE => ErrorCode::DISPATCH,
        "58000" => ErrorCode::INTERNAL,
        // `22023`/`22P02`/`22007`/`22003`/`42602`/`42000`/`23505` are all
        // client-supplied-value or client-syntax problems; none of them
        // carries a distinct retry/classification contract, so one code
        // covers the group.
        "22023" | "22P02" | "22007" | "22003" | "42602" | "42000" | "23505" => {
            ErrorCode::BAD_REQUEST
        }
        _ => ErrorCode::INTERNAL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unambiguous_sqlstates_derive_their_code() {
        assert_eq!(
            code_for_sqlstate(sqlstate::UNDEFINED_TABLE),
            ErrorCode::COLLECTION_NOT_FOUND
        );
        assert_eq!(
            code_for_sqlstate(sqlstate::FEATURE_NOT_SUPPORTED),
            ErrorCode::SQL_NOT_ENABLED
        );
        assert_eq!(code_for_sqlstate("42704"), ErrorCode::UNDEFINED_OBJECT);
        assert_eq!(code_for_sqlstate("42710"), ErrorCode::ALREADY_EXISTS);
        assert_eq!(code_for_sqlstate("55006"), ErrorCode::OBJECT_NOT_READY);
        assert_eq!(code_for_sqlstate("02000"), ErrorCode::NOT_FOUND);
    }

    #[test]
    fn unknown_sqlstate_falls_back_to_internal() {
        assert_eq!(code_for_sqlstate("99999"), ErrorCode::INTERNAL);
    }

    #[test]
    fn ddl_error_new_carries_the_derived_code() {
        let e = DdlError::new(sqlstate::UNDEFINED_TABLE, "collection 'x' not found");
        assert_eq!(e.code, ErrorCode::COLLECTION_NOT_FOUND);
        assert_eq!(e.sqlstate, "42P01");
    }

    /// The ambiguous constructors carry a code the bare-string derivation
    /// could never produce, since `0A000` alone also means
    /// `SQL_NOT_ENABLED`.
    #[test]
    fn ambiguous_constructors_carry_their_explicit_code() {
        let e = DdlError::cannot_drop_default_database("cannot drop 'default'");
        assert_eq!(e.sqlstate, "0A000");
        assert_eq!(e.code, ErrorCode::CANNOT_DROP_DEFAULT_DATABASE);
        assert_ne!(e.code, code_for_sqlstate(&e.sqlstate));

        let e = DdlError::cannot_clone_mirror("cannot clone a mirror");
        assert_eq!(e.sqlstate, "0A000");
        assert_eq!(e.code, ErrorCode::CANNOT_CLONE_MIRROR);

        let e = DdlError::move_tenant_snapshot_failed("snapshot failed");
        assert_eq!(e.sqlstate, "XX000");
        assert_eq!(e.code, ErrorCode::MOVE_TENANT_SNAPSHOT_FAILED);
        assert_ne!(e.code, code_for_sqlstate(&e.sqlstate));
    }
}

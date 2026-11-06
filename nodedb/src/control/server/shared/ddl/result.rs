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
    /// The typed error that caused this one, such as the Data-Plane refusal
    /// behind a MOVE TENANT phase failure. `None` when there is none.
    pub cause: Option<Box<nodedb_types::NodeDbError>>,
}

/// A crate error keeps the SQLSTATE and class every protocol reports for it.
impl From<crate::Error> for DdlError {
    fn from(error: crate::Error) -> Self {
        Self::from_error(&error)
    }
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
            cause: None,
        }
    }

    /// Build a `DdlError` for an internal fault whose source carries no
    /// SQLSTATE class: a codec, a storage engine outside `crate::Error`, a
    /// broken invariant, a system clock. SQLSTATE `XX000`.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(sqlstate::INTERNAL_ERROR, message)
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
            cause: None,
        }
    }

    /// Build a `DdlError` from an internal error: the SQLSTATE and message
    /// the pgwire table gives it, with its classified code and details.
    pub fn from_error(error: &crate::Error) -> Self {
        let (_, sqlstate, message) =
            crate::control::server::pgwire::types::error_to_sqlstate(error);
        let public = crate::error_classify::classify(error);
        Self::from_public(sqlstate, message, &public)
    }

    /// Build a `DdlError` from a parser refusal. The SQLSTATE is the one the
    /// planner path gives the same `SqlError`. The message is the parser's
    /// own text.
    pub fn from_sql_error(error: nodedb_sql::SqlError, tenant_id: crate::types::TenantId) -> Self {
        let message = error.to_string();
        let (_, sqlstate, _) = crate::control::server::pgwire::types::error_to_sqlstate(
            &crate::control::planner::plan_error_map::map_plan_error(error, tenant_id),
        );
        Self::new(sqlstate, message)
    }

    /// Build a `DdlError` from an internal error, with `context` before its
    /// message. The SQLSTATE, code and details stay the error's own, so a
    /// typed Data-Plane refusal keeps its class under the prefix.
    pub fn from_error_in_context(context: &str, error: &crate::Error) -> Self {
        let (_, sqlstate, message) =
            crate::control::server::pgwire::types::error_to_sqlstate(error);
        let public = crate::error_classify::classify(error);
        Self::from_public(sqlstate, format!("{context}: {message}"), &public)
    }

    /// Put `context` before this error's message. The SQLSTATE, code and
    /// details stay this error's own.
    pub fn in_context(mut self, context: &str) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }

    /// Carry `error`'s cause, when it has one, as this error's cause. The
    /// phase code stays this error's own, and the cause keeps its own class.
    pub fn with_cause_of(mut self, error: &nodedb_types::NodeDbError) -> Self {
        self.cause = error.cause().map(|cause| Box::new(cause.clone()));
        self
    }

    /// Build a `DdlError` with an explicit code, bypassing derivation.
    /// Used by the named constructors below for ambiguous SQLSTATEs.
    fn with_code(sqlstate: &'static str, code: ErrorCode, message: impl Into<String>) -> Self {
        DdlError {
            sqlstate: sqlstate.to_string(),
            code,
            message: message.into(),
            details: None,
            cause: None,
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

/// The `ErrorCode` a SQLSTATE classifies to. The single source of truth
/// [`DdlError::new`] and every `ddl_err`/`err`-style local helper derive
/// from — do not scatter a second copy of this table.
///
/// Each code a SQLSTATE maps to renders that SQLSTATE's class through the
/// numeric-code table, so a DDL error keeps its class after a node hop.
///
/// SQLSTATEs that more than one code shares (`0A000`, `55006`, `28000`,
/// `XX000`, `02000`) map to their default meaning here. The special
/// meanings have named constants of type [`sqlstate::AmbiguousSqlstate`],
/// which cannot reach this function (it takes `&str`), so a caller that
/// needs one of them is forced to the matching `DdlError::<name>`
/// constructor, or to a typed error that carries its code.
///
/// `57014` has no default meaning: it is sent for a deadline and for a
/// cancellation that is not one. It maps to `INTERNAL`, never to the
/// retriable deadline code.
pub fn code_for_sqlstate(sqlstate_str: &str) -> ErrorCode {
    match sqlstate_str {
        sqlstate::MIRROR_SYNC_TIMEOUT => ErrorCode::MIRROR_SYNC_TIMEOUT,
        sqlstate::UNDEFINED_TABLE => ErrorCode::COLLECTION_NOT_FOUND,
        sqlstate::INVALID_CATALOG_NAME => ErrorCode::DATABASE_NOT_FOUND,
        sqlstate::INSUFFICIENT_PRIVILEGE => ErrorCode::AUTHORIZATION_DENIED,
        // Default credential-failure meaning of `28000`, and `28P01`: one
        // non-retriable code, so a wrong password and an unknown user read
        // the same. `AUTH_TOKEN_EXPIRED` and `BACKUP_KEY_MISMATCH` are
        // ambiguous-typed.
        sqlstate::INVALID_AUTHORIZATION | sqlstate::INVALID_PASSWORD => {
            ErrorCode::AUTHENTICATION_FAILED
        }
        sqlstate::UNDEFINED_FUNCTION => ErrorCode::UNDEFINED_FUNCTION,
        sqlstate::UNDEFINED_COLUMN => ErrorCode::UNDEFINED_COLUMN,
        sqlstate::AMBIGUOUS_COLUMN => ErrorCode::AMBIGUOUS_COLUMN,
        // A column read outside GROUP BY and outside every aggregate: a
        // non-retriable client error.
        sqlstate::GROUPING_ERROR => ErrorCode::BAD_REQUEST,
        sqlstate::DATA_EXCEPTION => ErrorCode::DATA_EXCEPTION,
        // A bad parameter value, text representation, datetime format,
        // datetime range or COPY file format is a data exception: class
        // `22`, the class the code renders back.
        "22023" | "22P02" | "22007" | "22008" | sqlstate::BAD_COPY_FILE_FORMAT => {
            ErrorCode::DATA_EXCEPTION
        }
        sqlstate::NUMERIC_VALUE_OUT_OF_RANGE => ErrorCode::OVERFLOW,
        sqlstate::DIVISION_BY_ZERO => ErrorCode::DIVISION_BY_ZERO,
        sqlstate::INVALID_LIMIT_VALUE => ErrorCode::INVALID_LIMIT_VALUE,
        sqlstate::PROGRAM_LIMIT_EXCEEDED => ErrorCode::PROGRAM_LIMIT_EXCEEDED,
        sqlstate::CLONE_DEPTH_EXCEEDED => ErrorCode::CLONE_DEPTH_EXCEEDED,
        // Every integrity-constraint SQLSTATE is a constraint violation:
        // class `23`, the class the code renders back.
        sqlstate::INTEGRITY_CONSTRAINT_VIOLATION
        | sqlstate::NOT_NULL_VIOLATION
        | sqlstate::FOREIGN_KEY_VIOLATION
        | sqlstate::UNIQUE_VIOLATION
        | sqlstate::CHECK_VIOLATION => ErrorCode::CONSTRAINT_VIOLATION,
        sqlstate::APPEND_ONLY_VIOLATION => ErrorCode::APPEND_ONLY_VIOLATION,
        sqlstate::BALANCE_VIOLATION => ErrorCode::BALANCE_VIOLATION,
        sqlstate::PERIOD_LOCKED => ErrorCode::PERIOD_LOCKED,
        sqlstate::STATE_TRANSITION_VIOLATION => ErrorCode::STATE_TRANSITION_VIOLATION,
        sqlstate::TRANSITION_CHECK_VIOLATION => ErrorCode::TRANSITION_CHECK_VIOLATION,
        sqlstate::RETENTION_VIOLATION => ErrorCode::RETENTION_VIOLATION,
        sqlstate::LEGAL_HOLD_ACTIVE => ErrorCode::LEGAL_HOLD_ACTIVE,
        sqlstate::TYPE_GUARD_VIOLATION => ErrorCode::TYPE_GUARD_VIOLATION,
        sqlstate::PERIOD_LOCK_MISCONFIGURED => ErrorCode::PERIOD_LOCK_MISCONFIGURED,
        sqlstate::CANNOT_COERCE => ErrorCode::TYPE_MISMATCH,
        // A malformed request and a plan that cannot be built both render as
        // `42601`; both are non-retriable client errors, so one code covers
        // both without losing anything a client acts on.
        sqlstate::SYNTAX_ERROR => ErrorCode::BAD_REQUEST,
        sqlstate::SERIALIZATION_FAILURE => ErrorCode::WRITE_CONFLICT,
        sqlstate::TRANSACTION_ROLLBACK => ErrorCode::TRANSACTION_ROLLBACK,
        sqlstate::ACTIVE_SQL_TRANSACTION => ErrorCode::ACTIVE_SQL_TRANSACTION,
        sqlstate::READ_ONLY_SQL_TRANSACTION => ErrorCode::MIRROR_READ_ONLY,
        sqlstate::DEPENDENT_OBJECTS_STILL_EXIST => ErrorCode::DEPENDENT_OBJECTS_EXIST,
        sqlstate::TOO_MANY_CONNECTIONS => ErrorCode::RATE_EXCEEDED,
        sqlstate::DISK_FULL => ErrorCode::STORAGE_QUOTA_EXCEEDED,
        sqlstate::OUT_OF_MEMORY => ErrorCode::MEMORY_EXHAUSTED,
        sqlstate::SERVER_OVERLOAD => ErrorCode::SERVER_OVERLOAD,
        sqlstate::DATABASE_DROPPED => ErrorCode::NOT_LEADER,
        sqlstate::LOCK_NOT_AVAILABLE => ErrorCode::NO_LEADER,
        sqlstate::MOVE_TENANT_PREFLIGHT_FAILED => ErrorCode::MOVE_TENANT_PREFLIGHT_FAILED,
        // A target the server cannot reach: retriable, class `08`.
        sqlstate::CONNECTION_FAILURE => ErrorCode::NODE_UNREACHABLE,
        sqlstate::PROTOCOL_VIOLATION => ErrorCode::HANDSHAKE_FAILED,
        sqlstate::SERVER_REJECTED_ESTABLISHMENT => ErrorCode::SHAPE_SUBSCRIPTION_FAILED,
        sqlstate::INTERNAL_ERROR => ErrorCode::INTERNAL,
        // `0A000` here means the default, unambiguous "feature not
        // supported" case — the ambiguous named meanings sharing this
        // string (`CANNOT_DROP_DEFAULT_DATABASE`, `CANNOT_CLONE_MIRROR`)
        // cannot reach this function; see the doc comment above.
        sqlstate::FEATURE_NOT_SUPPORTED => ErrorCode::SQL_NOT_ENABLED,
        sqlstate::UNDEFINED_OBJECT => ErrorCode::UNDEFINED_OBJECT,
        // A duplicate object of any kind, a database (`42P04`) included.
        sqlstate::DUPLICATE_OBJECT | "42P07" | "42723" | "42P04" => ErrorCode::ALREADY_EXISTS,
        // Invalid or incompatible object definition: client-actionable and
        // non-retriable.
        "42P17" | "42809" | "42P16" => ErrorCode::BAD_REQUEST,
        // A declared literal the column type cannot represent.
        sqlstate::DATATYPE_MISMATCH => ErrorCode::BAD_REQUEST,
        // Default "object not in prerequisite state" meaning of `55006`;
        // `CLONE_DEPENDENCY` and `CLONE_WRITE_REQUIRES_MATERIALIZE` are
        // ambiguous-typed and cannot reach this function.
        sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE | "55006" => ErrorCode::OBJECT_NOT_READY,
        // Default "no data found" meaning of `02000`;
        // `MOVE_TENANT_ALREADY_AT_TARGET` is ambiguous-typed.
        sqlstate::NO_DATA => ErrorCode::NOT_FOUND,
        sqlstate::CONFIGURATION_LIMIT_EXCEEDED => ErrorCode::QUOTA_OVERCOMMIT,
        sqlstate::IO_ERROR => ErrorCode::STORAGE,
        "58000" => ErrorCode::INTERNAL,
        // Client-syntax problems with no distinct retry or classification
        // contract: one code covers the group.
        "42602" | "42000" => ErrorCode::BAD_REQUEST,
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

    /// A statement-class SQLSTATE derives its own code, never `INTERNAL`.
    #[test]
    fn statement_class_sqlstates_derive_their_code() {
        assert_eq!(
            code_for_sqlstate(sqlstate::DIVISION_BY_ZERO),
            ErrorCode::DIVISION_BY_ZERO
        );
        assert_eq!(
            code_for_sqlstate(sqlstate::INVALID_LIMIT_VALUE),
            ErrorCode::INVALID_LIMIT_VALUE
        );
        assert_eq!(
            code_for_sqlstate(sqlstate::PROGRAM_LIMIT_EXCEEDED),
            ErrorCode::PROGRAM_LIMIT_EXCEEDED
        );
    }

    /// Transaction-state and dependency SQLSTATEs derive a code whose numeric
    /// rendering keeps their class.
    #[test]
    fn transaction_and_dependency_sqlstates_derive_their_code() {
        assert_eq!(
            code_for_sqlstate(sqlstate::TRANSACTION_ROLLBACK),
            ErrorCode::TRANSACTION_ROLLBACK
        );
        assert_eq!(
            code_for_sqlstate(sqlstate::ACTIVE_SQL_TRANSACTION),
            ErrorCode::ACTIVE_SQL_TRANSACTION
        );
        assert_eq!(
            code_for_sqlstate(sqlstate::DEPENDENT_OBJECTS_STILL_EXIST),
            ErrorCode::DEPENDENT_OBJECTS_EXIST
        );
    }

    /// A typed error behind a context prefix keeps its own SQLSTATE and
    /// code: a quorum loss during a catalog propose stays retryable.
    #[test]
    fn an_error_in_context_keeps_its_class() {
        let quorum = crate::Error::GroupQuorumUnavailable {
            group_id: 0,
            voters: vec![1, 2, 3],
            unreachable: vec![2, 3],
        };
        let e = DdlError::from_error_in_context("metadata propose", &quorum);
        assert_eq!(e.sqlstate, sqlstate::LOCK_NOT_AVAILABLE);
        assert_eq!(e.code, ErrorCode::NO_LEADER);
        assert!(e.message.starts_with("metadata propose: "), "{}", e.message);

        let denied = crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(1),
            resource: "orders".into(),
        };
        let e = DdlError::from_error_in_context("catalog write", &denied);
        assert_eq!(e.sqlstate, sqlstate::INSUFFICIENT_PRIVILEGE);
        assert_eq!(e.code, ErrorCode::AUTHORIZATION_DENIED);
    }

    /// A typed error with no context keeps its own SQLSTATE and code.
    #[test]
    fn a_typed_error_keeps_its_class() {
        let missing = crate::Error::CollectionNotFound {
            tenant_id: crate::types::TenantId::new(1),
            collection: "orders".into(),
        };
        let e = DdlError::from_error(&missing);
        assert_eq!(e.sqlstate, sqlstate::UNDEFINED_TABLE);
        assert_eq!(e.code, ErrorCode::COLLECTION_NOT_FOUND);

        let deadline = crate::Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(1),
        };
        let e = DdlError::from_error(&deadline);
        assert_eq!(e.sqlstate, sqlstate::QUERY_CANCELED.0);
        assert_eq!(e.code, ErrorCode::DEADLINE_EXCEEDED);
    }

    /// A Data-Plane verdict read off an error response keeps its class: a
    /// duplicate key during an index backfill is `23505`.
    #[test]
    fn a_data_plane_verdict_in_context_keeps_its_class() {
        let duplicate =
            crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key 'a'".into(),
            });
        let e = DdlError::from_error_in_context("index backfill", &duplicate);
        assert_eq!(e.sqlstate, sqlstate::UNIQUE_VIOLATION);
        assert_eq!(e.code, ErrorCode::CONSTRAINT_VIOLATION);
    }

    /// A peer's typed refusal keeps its class after it crosses the cluster
    /// wire back into a `crate::Error`.
    #[test]
    fn a_peer_refusal_keeps_its_class() {
        let wire = nodedb_cluster::rpc_codec::TypedClusterError::RejectedConstraint {
            collection: "users".into(),
            constraint: "unique".into(),
            detail: "duplicate key 'a'".into(),
        };
        let e =
            DdlError::from_error_in_context("peer backfill on node 2", &crate::Error::from(wire));
        assert_eq!(e.sqlstate, sqlstate::UNIQUE_VIOLATION);
        assert_eq!(e.code, ErrorCode::CONSTRAINT_VIOLATION);
    }

    /// A fault with no typed class is `XX000` through the one helper.
    #[test]
    fn an_untyped_fault_is_internal() {
        let e = DdlError::internal("system clock error");
        assert_eq!(e.sqlstate, sqlstate::INTERNAL_ERROR);
        assert_eq!(e.code, ErrorCode::INTERNAL);
    }

    /// A context prefix on a built `DdlError` keeps its SQLSTATE and code.
    #[test]
    fn in_context_keeps_the_class() {
        let e = DdlError::new("42710", "synonym group 'g' already exists")
            .in_context("clone: copying synonym group 'g'");
        assert_eq!(e.sqlstate, "42710");
        assert_eq!(e.code, ErrorCode::ALREADY_EXISTS);
        assert_eq!(
            e.message,
            "clone: copying synonym group 'g': synonym group 'g' already exists"
        );
    }

    /// A role still in use keeps `2BP01` and the dependent-objects code.
    #[test]
    fn a_role_in_use_is_a_dependent_objects_error() {
        let in_use = crate::Error::RoleInUse {
            role: "analyst".into(),
            dependents: crate::control::security::role_assignment::RoleDependents::Users(vec![
                "bob".into(),
            ]),
        };
        let e = DdlError::from_error(&in_use);
        assert_eq!(e.sqlstate, sqlstate::DEPENDENT_OBJECTS_STILL_EXIST);
        assert_eq!(e.code, ErrorCode::DEPENDENT_OBJECTS_EXIST);
    }

    #[test]
    fn a_duplicate_database_is_already_exists() {
        assert_eq!(code_for_sqlstate("42P04"), ErrorCode::ALREADY_EXISTS);
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
    /// can never produce, since `0A000` alone also means
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

// SPDX-License-Identifier: Apache-2.0

//! Standard PostgreSQL SQLSTATE code constants.
//!
//! A single source of truth for every five-character SQLSTATE string used
//! across the codebase. Grouping follows the PostgreSQL documentation
//! appendix (Class 00–XX).  All constants are `&'static str` so they compose
//! directly with `pgwire::error::ErrorInfo` without any allocation.
//!
//! Add new codes here when a new error path needs one; never inline a literal
//! elsewhere — a typo in a SQLSTATE string is undetectable at compile time.

/// A SQLSTATE string shared by more than one [`crate::error::ErrorCode`]
/// meaning at the `DdlError` construction sites that actually use it.
///
/// Deliberately not `&str`: passing one to `DdlError::new`, which derives
/// the code from the SQLSTATE string, does not compile. Construct through
/// the paired `DdlError::<name>` constructor instead, which supplies the
/// code explicitly rather than guessing it from the wire string.
#[derive(Debug, Clone, Copy)]
pub struct AmbiguousSqlstate(pub &'static str);

// ── Class 00 — Successful Completion ────────────────────────────────────────

/// `00000` — `successful_completion`
pub const SUCCESS: &str = "00000";

// ── Class 01 — Warning ───────────────────────────────────────────────────────

/// `01000` — `warning` (generic warning)
pub const WARNING: &str = "01000";

// ── Class 02 — No Data ───────────────────────────────────────────────────────

/// `02000` — `no_data` (document / row not found)
pub const NO_DATA: &str = "02000";

// ── Class 0A — Feature Not Supported ────────────────────────────────────────

/// `0A000` — `feature_not_supported`
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// `0A000` — Cannot drop the built-in `default` database, which is immutable.
/// Aliased to `feature_not_supported` per the PostgreSQL convention for
/// unsupported DDL operations on reserved objects. Ambiguous with
/// `FEATURE_NOT_SUPPORTED` and `CANNOT_CLONE_MIRROR` — see
/// [`AmbiguousSqlstate`].
pub const CANNOT_DROP_DEFAULT_DATABASE: AmbiguousSqlstate = AmbiguousSqlstate("0A000");

// ── Class 22 — Data Exception ────────────────────────────────────────────────

/// `22003` — `numeric_value_out_of_range`
pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";

/// `22012` — `division_by_zero` (`/` or `%` with a zero divisor —
/// raised at runtime instead of evaluating to `NULL`)
pub const DIVISION_BY_ZERO: &str = "22012";

/// `2201W` — `invalid_limit_value` (a LIMIT/OFFSET/FETCH bound that is
/// negative, fractional, non-numeric, or wider than `usize`)
pub const INVALID_LIMIT_VALUE: &str = "2201W";

/// `22023` — `invalid_parameter_value` (a `SET` value the parameter's own
/// grammar refuses, e.g. `SET statement_timeout = 'later'`)
pub const INVALID_PARAMETER_VALUE: &str = "22023";

// ── Class 23 — Integrity Constraint Violation ────────────────────────────────

/// `23000` — `integrity_constraint_violation` (generic)
pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";

/// `23502` — `not_null_violation`
pub const NOT_NULL_VIOLATION: &str = "23502";

/// `23503` — `foreign_key_violation` (dangling-edge rejection)
pub const FOREIGN_KEY_VIOLATION: &str = "23503";

/// `23505` — `unique_violation`
pub const UNIQUE_VIOLATION: &str = "23505";

/// `23514` — `check_violation`
pub const CHECK_VIOLATION: &str = "23514";

/// `23601` — NodeDB extension: append-only write rejected.
pub const APPEND_ONLY_VIOLATION: &str = "23601";

/// `23602` — NodeDB extension: balance constraint violated.
pub const BALANCE_VIOLATION: &str = "23602";

/// `23603` — NodeDB extension: period lock; writes rejected.
pub const PERIOD_LOCKED: &str = "23603";

/// `23604` — NodeDB extension: state-transition constraint violated.
pub const STATE_TRANSITION_VIOLATION: &str = "23604";

/// `23605` — NodeDB extension: transition-check constraint violated.
pub const TRANSITION_CHECK_VIOLATION: &str = "23605";

/// `23606` — NodeDB extension: retention policy blocks deletion.
pub const RETENTION_VIOLATION: &str = "23606";

/// `23607` — NodeDB extension: legal hold blocks deletion.
pub const LEGAL_HOLD_ACTIVE: &str = "23607";

/// `23608` — NodeDB extension: type-guard constraint violated.
pub const TYPE_GUARD_VIOLATION: &str = "23608";

/// `23609` — NodeDB extension: period-lock reference row has no configured
/// status column; a misconfigured column name, not a locked period.
pub const PERIOD_LOCK_MISCONFIGURED: &str = "23609";

// ── Class 28 — Invalid Authorization Specification ───────────────────────────

/// `28000` — `invalid_authorization_specification` (no valid credentials)
pub const INVALID_AUTHORIZATION: &str = "28000";

// ── Class 3D — Invalid Catalog Name ──────────────────────────────────────────

/// `3D000` — `invalid_catalog_name` (the selected database does not exist)
pub const INVALID_CATALOG_NAME: &str = "3D000";

// ── Class 40 — Transaction Rollback ──────────────────────────────────────────

/// `40000` — `transaction_rollback` (rolled back for a reason other than a
/// serialization conflict; class 40 tells the client to retry)
pub const TRANSACTION_ROLLBACK: &str = "40000";

/// `40001` — `serialization_failure` (write conflict; client should retry)
pub const SERIALIZATION_FAILURE: &str = "40001";

// ── Class 42 — Syntax Error or Access Rule Violation ─────────────────────────

/// `42501` — `insufficient_privilege`
pub const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// `42601` — `syntax_error`
pub const SYNTAX_ERROR: &str = "42601";

/// `42704` — `undefined_object` (a runtime parameter the server does not
/// carry, e.g. `SET nonsense = 1` or `SHOW nonsense`)
pub const UNDEFINED_OBJECT: &str = "42704";

/// `42703` — `undefined_column` (a column reference that resolves against no
/// relation in scope)
pub const UNDEFINED_COLUMN: &str = "42703";

/// `42702` — `ambiguous_column`: a bare column name that resolves against
/// more than one relation in scope.
pub const AMBIGUOUS_COLUMN: &str = "42702";

/// `42846` — `cannot_coerce`
pub const CANNOT_COERCE: &str = "42846";

/// `42P01` — `undefined_table` (collection not found)
pub const UNDEFINED_TABLE: &str = "42P01";

/// `42883` — `undefined_function` (no scalar/aggregate/window function
/// registered under the called name)
pub const UNDEFINED_FUNCTION: &str = "42883";

// ── Class 53 — Insufficient Resources ────────────────────────────────────────

/// `53200` — `out_of_memory`
pub const OUT_OF_MEMORY: &str = "53200";

/// `53300` — `too_many_connections` (closest match for rate-limit denial)
pub const TOO_MANY_CONNECTIONS: &str = "53300";

/// `53400` — `configuration_limit_exceeded` (quota exceeded)
pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";

// ── Class 54 — Program Limit Exceeded ────────────────────────────────────────

/// `54000` — `program_limit_exceeded` (generic over-cap)
pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";

/// `54001` — `statement_too_complex` (fan-out / rate limit exceeded)
pub const STATEMENT_TOO_COMPLEX: &str = "54001";

// ── Class 55 — Object Not In Prerequisite State ──────────────────────────────

/// `55000` — `object_not_in_prerequisite_state` (the object exists but the
/// step this statement needs has not run, e.g. `currval` before `nextval`)
pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";

/// `55P03` — `lock_not_available` (no cluster leader)
pub const LOCK_NOT_AVAILABLE: &str = "55P03";

// ── Class 57 — Operator Intervention ─────────────────────────────────────────

/// `57014` — `query_canceled` (deadline exceeded)
pub const QUERY_CANCELED: &str = "57014";

/// `57P03` — `cannot_connect_now` (collection is draining)
pub const CANNOT_CONNECT_NOW: &str = "57P03";

/// `57P04` — `database_dropped` (not-leader redirect; client should retry elsewhere)
pub const DATABASE_DROPPED: &str = "57P04";

// ── Quota-specific aliases (Class 53 / 57) ───────────────────────────────────
//
// PostgreSQL class 53 "Insufficient Resources" is the closest match for quota
// exhaustion conditions.  Class 57P03 "cannot_connect_now" covers transient
// overload situations where the server is running but cannot accept the request.

/// `53400` — `configuration_limit_exceeded`: sum of tenant/database quotas would
/// exceed the configured global or parent ceiling (`QUOTA_OVERCOMMIT`).
/// Alias for [`CONFIGURATION_LIMIT_EXCEEDED`].
pub const QUOTA_OVERCOMMIT: &str = "53400";

/// `53400` — `configuration_limit_exceeded`: tenant or database has exhausted its
/// configured resource budget (`TENANT_QUOTA_EXCEEDED`, `DATABASE_QUOTA_EXCEEDED`).
/// Class 53 is preferred over 54 because the limit is a runtime configuration
/// setting, not a hard-coded program limit.
pub const QUOTA_EXCEEDED: &str = "53400";

/// `57P03` — `cannot_connect_now`: server is under global resource pressure and
/// cannot accept new requests (`SERVER_OVERLOAD`). Using `57P03` rather than
/// `53300` (too_many_connections) because the condition is transient and the
/// server may accept requests again shortly — clients should retry after backoff.
pub const SERVER_OVERLOAD: &str = "57P03";

// ── Clone DDL (Class 54 / 55 / 0A) ──────────────────────────────────────────

/// `54011` — NodeDB extension: clone chain depth exceeds `MAX_CLONE_DEPTH`.
///
/// Uses Class 54 "Program Limit Exceeded" because the limit is a hard-coded
/// structural cap (depth 8), not a runtime quota setting.
pub const CLONE_DEPTH_EXCEEDED: &str = "54011";

/// `0A000` — NodeDB extension: a mirror database cannot be cloned.
///
/// Aliased to `feature_not_supported` — cloning a mirror creates ambiguous
/// lineage; the operator must promote the mirror to a writable database
/// first. Ambiguous with `FEATURE_NOT_SUPPORTED` and
/// `CANNOT_DROP_DEFAULT_DATABASE` — see [`AmbiguousSqlstate`].
pub const CANNOT_CLONE_MIRROR: AmbiguousSqlstate = AmbiguousSqlstate("0A000");

/// `55006` — NodeDB extension: source database has active clone dependents.
///
/// Uses Class 55 "Object Not In Prerequisite State" because the source is in
/// the correct state for normal use but cannot be dropped until dependents
/// are resolved. Ambiguous with the generic "object busy" use of `55006` —
/// see [`AmbiguousSqlstate`].
pub const CLONE_DEPENDENCY: AmbiguousSqlstate = AmbiguousSqlstate("55006");

/// `22023` — NodeDB extension: `AS OF` timestamp predates the clone's
/// creation point; the database did not exist at that time.
///
/// Uses Class 22 "Data Exception" / `22023` (invalid parameter value) because
/// the user-supplied timestamp is valid in general but out of range for this
/// specific clone.
pub const CLONE_PREDATES_QUERY_TIME: &str = "22023";

/// `55P03` — NodeDB extension: this node cannot satisfy the requested
/// consistency level, because it is not the Raft leader for the data and
/// cannot prove that it is.
///
/// Uses Class 55 "Object Not In Prerequisite State": the target is valid but
/// is not in the state (Raft leader) the requested consistency needs. Raised
/// for a mirror database, which should be redirected to the source cluster,
/// and for a linearizable read whose leadership no quorum confirms. Retriable
/// in both cases.
pub const STALE_READ_NOT_LEADER: &str = "55P03";

/// `55006` — NodeDB extension: a write targeted a `Shadowed`/`Materializing`
/// clone collection whose engine has no copy-on-write support.
///
/// Uses Class 55 "Object Not In Prerequisite State", same as
/// [`CLONE_DEPENDENCY`] — the collection is valid but not yet in the state
/// (materialized) the write needs. Ambiguous — see [`AmbiguousSqlstate`].
pub const CLONE_WRITE_REQUIRES_MATERIALIZE: AmbiguousSqlstate = AmbiguousSqlstate("55006");

// ── Backup / Restore (Class 22 / 28) ────────────────────────────────────────

/// `22023` — NodeDB extension: RESTORE's target tenant does not match the
/// tenant the envelope belongs to. Uses Class 22 "Data Exception" because the
/// envelope is well-formed, just not for the requested tenant.
pub const BACKUP_TENANT_MISMATCH: &str = "22023";

/// `28000` — NodeDB extension: the backup envelope did not decrypt under this
/// server's configured `backup_encryption` key. Aliased to
/// `invalid_authorization_specification`, the same class used for any other
/// presented-key mismatch.
pub const BACKUP_KEY_MISMATCH: &str = "28000";

// ── Move Tenant DDL (Class 55 / 57) ─────────────────────────────────────────

/// `57014` — `query_canceled`: drain phase timed out; client should re-try after
/// ensuring the tenant has no active connections on the source database.
/// Ambiguous with `QUERY_CANCELED` (deadline exceeded) — see
/// [`AmbiguousSqlstate`].
pub const MOVE_TENANT_DRAIN_TIMEOUT: AmbiguousSqlstate = AmbiguousSqlstate("57014");

/// `55P02` — `lock_not_available`: pre-flight check found schema incompatibility
/// between the source and target databases; no state was mutated.
pub const MOVE_TENANT_PREFLIGHT_FAILED: &str = "55P02";

/// `XX000` — internal error during snapshot phase; source left unchanged.
/// Ambiguous with the generic internal-error use of `XX000` — see
/// [`AmbiguousSqlstate`].
pub const MOVE_TENANT_SNAPSHOT_FAILED: AmbiguousSqlstate = AmbiguousSqlstate("XX000");

/// `XX000` — internal error during cutover phase; source still holds data.
/// Ambiguous with the generic internal-error use of `XX000` — see
/// [`AmbiguousSqlstate`].
pub const MOVE_TENANT_CUTOVER_FAILED: AmbiguousSqlstate = AmbiguousSqlstate("XX000");

/// `02000` — `no_data`: tenant is already present in the target database;
/// the `MOVE TENANT` is a no-op (idempotent retry of a completed move).
/// Ambiguous with the generic "no data found" use of `02000` — see
/// [`AmbiguousSqlstate`].
pub const MOVE_TENANT_ALREADY_AT_TARGET: AmbiguousSqlstate = AmbiguousSqlstate("02000");

// ── Class 08 — Connection Exception ──────────────────────────────────────────

/// `08006` — `connection_failure`: a Control-to-Data-Plane dispatch could not
/// reach the target core (bridge closed, core panic, timeout).
pub const CONNECTION_FAILURE: &str = "08006";

// ── Class 58 — System Error ──────────────────────────────────────────────────

/// `58030` — `io_error`: a WAL append or filesystem operation failed.
pub const IO_ERROR: &str = "58030";

// ── Class XX — Internal Error ────────────────────────────────────────────────

/// `XX000` — `internal_error`
pub const INTERNAL_ERROR: &str = "XX000";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_codes_are_five_chars() {
        let codes = [
            SUCCESS,
            WARNING,
            NO_DATA,
            FEATURE_NOT_SUPPORTED,
            NUMERIC_VALUE_OUT_OF_RANGE,
            DIVISION_BY_ZERO,
            INVALID_LIMIT_VALUE,
            INTEGRITY_CONSTRAINT_VIOLATION,
            NOT_NULL_VIOLATION,
            FOREIGN_KEY_VIOLATION,
            UNIQUE_VIOLATION,
            CHECK_VIOLATION,
            APPEND_ONLY_VIOLATION,
            BALANCE_VIOLATION,
            PERIOD_LOCKED,
            PERIOD_LOCK_MISCONFIGURED,
            STATE_TRANSITION_VIOLATION,
            TRANSITION_CHECK_VIOLATION,
            RETENTION_VIOLATION,
            LEGAL_HOLD_ACTIVE,
            TYPE_GUARD_VIOLATION,
            INVALID_AUTHORIZATION,
            SERIALIZATION_FAILURE,
            INSUFFICIENT_PRIVILEGE,
            SYNTAX_ERROR,
            UNDEFINED_COLUMN,
            AMBIGUOUS_COLUMN,
            UNDEFINED_OBJECT,
            CANNOT_COERCE,
            UNDEFINED_TABLE,
            UNDEFINED_FUNCTION,
            OUT_OF_MEMORY,
            TOO_MANY_CONNECTIONS,
            CONFIGURATION_LIMIT_EXCEEDED,
            PROGRAM_LIMIT_EXCEEDED,
            STATEMENT_TOO_COMPLEX,
            OBJECT_NOT_IN_PREREQUISITE_STATE,
            LOCK_NOT_AVAILABLE,
            QUERY_CANCELED,
            CANNOT_CONNECT_NOW,
            DATABASE_DROPPED,
            INTERNAL_ERROR,
            QUOTA_OVERCOMMIT,
            QUOTA_EXCEEDED,
            SERVER_OVERLOAD,
            CLONE_DEPTH_EXCEEDED,
            CLONE_PREDATES_QUERY_TIME,
            STALE_READ_NOT_LEADER,
            CONNECTION_FAILURE,
            IO_ERROR,
        ];
        for code in &codes {
            assert_eq!(
                code.len(),
                5,
                "SQLSTATE '{code}' must be exactly 5 characters"
            );
        }

        let ambiguous = [
            CANNOT_DROP_DEFAULT_DATABASE,
            CANNOT_CLONE_MIRROR,
            CLONE_DEPENDENCY,
            CLONE_WRITE_REQUIRES_MATERIALIZE,
            MOVE_TENANT_DRAIN_TIMEOUT,
            MOVE_TENANT_SNAPSHOT_FAILED,
            MOVE_TENANT_CUTOVER_FAILED,
            MOVE_TENANT_ALREADY_AT_TARGET,
        ];
        for code in &ambiguous {
            assert_eq!(
                code.0.len(),
                5,
                "SQLSTATE '{}' must be exactly 5 characters",
                code.0
            );
        }
    }

    #[test]
    fn spot_check_well_known_codes() {
        assert_eq!(UNIQUE_VIOLATION, "23505");
        assert_eq!(UNDEFINED_COLUMN, "42703");
        assert_eq!(AMBIGUOUS_COLUMN, "42702");
        assert_eq!(UNDEFINED_TABLE, "42P01");
        assert_eq!(INSUFFICIENT_PRIVILEGE, "42501");
        assert_eq!(QUERY_CANCELED, "57014");
        assert_eq!(INTERNAL_ERROR, "XX000");
        assert_eq!(FEATURE_NOT_SUPPORTED, "0A000");
    }

    #[test]
    fn ambiguous_sqlstates_alias_their_documented_string() {
        assert_eq!(CANNOT_DROP_DEFAULT_DATABASE.0, "0A000");
        assert_eq!(CANNOT_CLONE_MIRROR.0, "0A000");
        assert_eq!(CLONE_DEPENDENCY.0, "55006");
        assert_eq!(CLONE_WRITE_REQUIRES_MATERIALIZE.0, "55006");
        assert_eq!(MOVE_TENANT_DRAIN_TIMEOUT.0, "57014");
        assert_eq!(MOVE_TENANT_SNAPSHOT_FAILED.0, "XX000");
        assert_eq!(MOVE_TENANT_CUTOVER_FAILED.0, "XX000");
        assert_eq!(MOVE_TENANT_ALREADY_AT_TARGET.0, "02000");
    }
}

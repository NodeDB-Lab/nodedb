// SPDX-License-Identifier: BUSL-1.1

//! Audit event taxonomy + per-variant routing rules (auth-stream flag,
//! minimum level).

use super::level::AuditLevel;

/// Categories of audit events.
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum AuditEvent {
    /// Authentication succeeded.
    AuthSuccess = 0,
    /// Authentication failed.
    AuthFailure = 1,
    /// Authorization denied.
    AuthzDenied = 2,
    /// Privilege/role change.
    PrivilegeChange = 3,
    /// Tenant created.
    TenantCreated = 4,
    /// Tenant deleted.
    TenantDeleted = 5,
    /// Snapshot initiated.
    SnapshotBegin = 6,
    /// Snapshot completed.
    SnapshotEnd = 7,
    /// Snapshot restore initiated.
    RestoreBegin = 8,
    /// Snapshot restore completed.
    RestoreEnd = 9,
    /// TLS certificate rotated.
    CertRotation = 10,
    /// TLS certificate rotation failed.
    CertRotationFailed = 11,
    /// Encryption key rotated.
    KeyRotation = 12,
    /// Configuration change.
    ConfigChange = 13,
    /// Node joined cluster.
    NodeJoined = 14,
    /// Node left cluster.
    NodeLeft = 15,
    /// Admin action (catch-all for ops).
    AdminAction = 16,
    /// Session connected.
    SessionConnect = 17,
    /// Session disconnected.
    SessionDisconnect = 18,
    /// Query executed (full/forensic level only).
    QueryExec = 19,
    /// RLS denial (full level).
    RlsDenied = 20,
    /// Row-level change (forensic level only).
    RowChange = 21,
    /// DDL change committed to the metadata Raft group. Emitted on
    /// every replica from `MetadataCommitApplier` with full before /
    /// after descriptor versions + HLC + raw SQL text. (J.4)
    DdlChange = 22,
    /// Session handle resolve failed fingerprint check — caller's
    /// (tenant_id, ip) didn't match the fingerprint captured at
    /// `SessionHandleStore::create()`. Signals handle theft across
    /// origins even when the handle itself is otherwise valid.
    SessionHandleFingerprintMismatch = 23,
    /// Resolve-miss rate on a single connection crossed the configured
    /// threshold within the detection window. Signals enumeration
    /// attempts or misconfigured clients probing bogus handles.
    SessionHandleResolveMissSpike = 24,
    /// Emitted immediately before audit entries are deleted during a
    /// retention prune. `prev_hash` = hash of the last deleted entry,
    /// so the surviving chain head links into this checkpoint.
    ///
    /// Invariant: this event is emitted ONLY when entries are actually
    /// deleted; never on a no-op prune. The surviving chain verifies as:
    ///   verify(checkpoint) → valid; verify(first_surviving) → valid.
    AuditCheckpoint = 25,
    /// An open session was actively revoked (DROP USER, soft-delete,
    /// full role purge).  Emitted by the bus consumer **before** the
    /// connection is closed so the row is durable even if close fails.
    SessionRevoked = 26,
    /// The security audit bus consumer fell behind its broadcast channel
    /// and dropped events.  Lag on the audit bus is itself a security
    /// event: it means audit rows may be missing.
    AuditBusLagged = 27,
    /// A runtime permission check returned false — the authenticated user
    /// was denied access to a collection or cluster resource. Emitted at
    /// the `PermissionStore::check` decision point.
    PermissionDenied = 28,
    /// A Row-Level Security write policy rejected a document write.
    /// Emitted by `rls::eval::check_compiled_write` on denial.
    RlsRejected = 29,
    /// A user's failed-login counter reached the lockout threshold and
    /// the account was locked for `lockout_duration_secs`.
    LockoutTriggered = 30,
    /// A pre-authentication rate-limit bucket (per-IP or per-username)
    /// rejected a login attempt before SCRAM/Argon2 verification.
    LoginRateLimited = 31,
    /// A new database was created.
    DatabaseCreated = 32,
    /// A database was dropped.
    DatabaseDropped = 33,
    /// A database was renamed.
    DatabaseRenamed = 34,
    /// A database quota was changed.
    DatabaseQuotaChanged = 35,
    /// A database was cloned.
    DatabaseCloned = 36,
    /// A database mirror was created.
    DatabaseMirrored = 37,
    /// A mirror database was promoted to writable primary.
    DatabasePromoted = 38,
    /// A cloned database was materialized.
    DatabaseMaterialized = 39,
    /// A tenant was moved between databases.
    TenantMoved = 40,
    /// A database backup was initiated.
    DatabaseBackedUp = 41,
    /// A database was restored from backup.
    DatabaseRestored = 42,
    /// A DML operation (INSERT/UPDATE/DELETE) was audited for a database with
    /// AUDIT_DML mode enabled. Recorded at Forensic level only.
    DmlAudit = 43,
    /// The AUDIT_DML mode for a database was changed via
    /// `ALTER DATABASE SET AUDIT_DML`.
    DatabaseAuditDmlChanged = 44,
    /// The idle session timeout for a database was changed via
    /// `ALTER DATABASE SET IDLE_TIMEOUT`.
    DatabaseIdleTimeoutChanged = 45,
    /// An OIDC provider was created, altered, or dropped.
    OidcProviderChanged = 46,
    /// A statement's durable record of intent, written before its mutation commits.
    /// A record that fails refuses the action, so a row here does not prove the action ran.
    ActionIntent = 47,
}

impl AuditEvent {
    /// Return the stable `#[repr(u8)]` discriminant.
    ///
    /// Used in `hash_entry` to produce a canonical, stable byte for the
    /// event type — independent of `Debug` formatting changes.
    pub fn discriminant(&self) -> u8 {
        match self {
            Self::AuthSuccess => 0,
            Self::AuthFailure => 1,
            Self::AuthzDenied => 2,
            Self::PrivilegeChange => 3,
            Self::TenantCreated => 4,
            Self::TenantDeleted => 5,
            Self::SnapshotBegin => 6,
            Self::SnapshotEnd => 7,
            Self::RestoreBegin => 8,
            Self::RestoreEnd => 9,
            Self::CertRotation => 10,
            Self::CertRotationFailed => 11,
            Self::KeyRotation => 12,
            Self::ConfigChange => 13,
            Self::NodeJoined => 14,
            Self::NodeLeft => 15,
            Self::AdminAction => 16,
            Self::SessionConnect => 17,
            Self::SessionDisconnect => 18,
            Self::QueryExec => 19,
            Self::RlsDenied => 20,
            Self::RowChange => 21,
            Self::DdlChange => 22,
            Self::SessionHandleFingerprintMismatch => 23,
            Self::SessionHandleResolveMissSpike => 24,
            Self::AuditCheckpoint => 25,
            Self::SessionRevoked => 26,
            Self::AuditBusLagged => 27,
            Self::PermissionDenied => 28,
            Self::RlsRejected => 29,
            Self::LockoutTriggered => 30,
            Self::LoginRateLimited => 31,
            Self::DatabaseCreated => 32,
            Self::DatabaseDropped => 33,
            Self::DatabaseRenamed => 34,
            Self::DatabaseQuotaChanged => 35,
            Self::DatabaseCloned => 36,
            Self::DatabaseMirrored => 37,
            Self::DatabasePromoted => 38,
            Self::DatabaseMaterialized => 39,
            Self::TenantMoved => 40,
            Self::DatabaseBackedUp => 41,
            Self::DatabaseRestored => 42,
            Self::DmlAudit => 43,
            Self::DatabaseAuditDmlChanged => 44,
            Self::DatabaseIdleTimeoutChanged => 45,
            Self::OidcProviderChanged => 46,
            Self::ActionIntent => 47,
        }
    }

    /// Whether this event belongs to the auth event stream.
    pub fn is_auth_event(&self) -> bool {
        match self {
            Self::AuthSuccess
            | Self::AuthFailure
            | Self::AuthzDenied
            | Self::SessionConnect
            | Self::SessionDisconnect
            | Self::PermissionDenied
            | Self::RlsRejected
            | Self::LockoutTriggered
            | Self::LoginRateLimited => true,
            Self::PrivilegeChange
            | Self::TenantCreated
            | Self::TenantDeleted
            | Self::SnapshotBegin
            | Self::SnapshotEnd
            | Self::RestoreBegin
            | Self::RestoreEnd
            | Self::CertRotation
            | Self::CertRotationFailed
            | Self::KeyRotation
            | Self::ConfigChange
            | Self::NodeJoined
            | Self::NodeLeft
            | Self::AdminAction
            | Self::QueryExec
            | Self::RlsDenied
            | Self::RowChange
            | Self::DdlChange
            | Self::SessionHandleFingerprintMismatch
            | Self::SessionHandleResolveMissSpike
            | Self::AuditCheckpoint
            | Self::SessionRevoked
            | Self::AuditBusLagged
            | Self::DatabaseCreated
            | Self::DatabaseDropped
            | Self::DatabaseRenamed
            | Self::DatabaseQuotaChanged
            | Self::DatabaseCloned
            | Self::DatabaseMirrored
            | Self::DatabasePromoted
            | Self::DatabaseMaterialized
            | Self::TenantMoved
            | Self::DatabaseBackedUp
            | Self::DatabaseRestored
            | Self::DmlAudit
            | Self::DatabaseAuditDmlChanged
            | Self::DatabaseIdleTimeoutChanged
            | Self::OidcProviderChanged
            | Self::ActionIntent => false,
        }
    }

    /// Return a stable snake_case filter key for use in SQL `WHERE event_type = '...'`
    /// comparisons.  The returned strings are used by `SHOW AUDIT WHERE event_type`
    /// and by the `event_type` column in audit query results.
    pub fn snake_name(&self) -> &'static str {
        match self {
            Self::AuthSuccess => "auth_success",
            Self::AuthFailure => "auth_failure",
            Self::AuthzDenied => "authz_denied",
            Self::PrivilegeChange => "privilege_change",
            Self::TenantCreated => "tenant_created",
            Self::TenantDeleted => "tenant_deleted",
            Self::SnapshotBegin => "snapshot_begin",
            Self::SnapshotEnd => "snapshot_end",
            Self::RestoreBegin => "restore_begin",
            Self::RestoreEnd => "restore_end",
            Self::CertRotation => "cert_rotation",
            Self::CertRotationFailed => "cert_rotation_failed",
            Self::KeyRotation => "key_rotation",
            Self::ConfigChange => "config_change",
            Self::NodeJoined => "node_joined",
            Self::NodeLeft => "node_left",
            Self::AdminAction => "admin_action",
            Self::SessionConnect => "session_connect",
            Self::SessionDisconnect => "session_disconnect",
            Self::QueryExec => "query_exec",
            Self::RlsDenied => "rls_denied",
            Self::RowChange => "row_change",
            Self::DdlChange => "ddl_change",
            Self::SessionHandleFingerprintMismatch => "session_handle_fingerprint_mismatch",
            Self::SessionHandleResolveMissSpike => "session_handle_resolve_miss_spike",
            Self::AuditCheckpoint => "audit_checkpoint",
            Self::SessionRevoked => "session_revoked",
            Self::AuditBusLagged => "audit_bus_lagged",
            Self::PermissionDenied => "permission_denied",
            Self::RlsRejected => "rls_rejected",
            Self::LockoutTriggered => "lockout_triggered",
            Self::LoginRateLimited => "login_rate_limited",
            Self::DatabaseCreated => "database_created",
            Self::DatabaseDropped => "database_dropped",
            Self::DatabaseRenamed => "database_renamed",
            Self::DatabaseQuotaChanged => "database_quota_changed",
            Self::DatabaseCloned => "database_cloned",
            Self::DatabaseMirrored => "database_mirrored",
            Self::DatabasePromoted => "database_promoted",
            Self::DatabaseMaterialized => "database_materialized",
            Self::TenantMoved => "tenant_moved",
            Self::DatabaseBackedUp => "database_backed_up",
            Self::DatabaseRestored => "database_restored",
            Self::DmlAudit => "dml_audit",
            Self::DatabaseAuditDmlChanged => "database_audit_dml_changed",
            Self::DatabaseIdleTimeoutChanged => "database_idle_timeout_changed",
            Self::OidcProviderChanged => "oidc_provider_changed",
            Self::ActionIntent => "action_intent",
        }
    }

    /// Minimum audit level required to record this event.
    pub fn min_level(&self) -> AuditLevel {
        match self {
            Self::AuthSuccess | Self::AuthFailure | Self::AuthzDenied => AuditLevel::Minimal,
            Self::PrivilegeChange
            | Self::AdminAction
            | Self::ConfigChange
            | Self::SessionConnect
            | Self::SessionDisconnect
            | Self::TenantCreated
            | Self::TenantDeleted
            | Self::SnapshotBegin
            | Self::SnapshotEnd
            | Self::RestoreBegin
            | Self::RestoreEnd
            | Self::CertRotation
            | Self::CertRotationFailed
            | Self::KeyRotation
            | Self::NodeJoined
            | Self::NodeLeft => AuditLevel::Standard,
            Self::QueryExec | Self::RlsDenied => AuditLevel::Full,
            Self::RowChange => AuditLevel::Forensic,
            Self::DdlChange => AuditLevel::Standard,
            Self::SessionHandleFingerprintMismatch | Self::SessionHandleResolveMissSpike => {
                AuditLevel::Standard
            }
            Self::AuditCheckpoint => AuditLevel::Minimal,
            // Security-critical events: always recorded at Minimal level so
            // they are never filtered out even in the lowest-verbosity mode.
            Self::SessionRevoked => AuditLevel::Minimal,
            Self::AuditBusLagged => AuditLevel::Minimal,
            // Denial events are security-critical: always at Minimal.
            Self::PermissionDenied => AuditLevel::Minimal,
            Self::RlsRejected => AuditLevel::Minimal,
            Self::LockoutTriggered => AuditLevel::Minimal,
            Self::LoginRateLimited => AuditLevel::Minimal,
            Self::DatabaseCreated
            | Self::DatabaseDropped
            | Self::DatabaseRenamed
            | Self::DatabaseQuotaChanged
            | Self::DatabaseCloned
            | Self::DatabaseMirrored
            | Self::DatabasePromoted
            | Self::DatabaseMaterialized
            | Self::TenantMoved
            | Self::DatabaseBackedUp
            | Self::DatabaseRestored
            | Self::DatabaseAuditDmlChanged
            | Self::DatabaseIdleTimeoutChanged
            | Self::OidcProviderChanged
            | Self::ActionIntent => AuditLevel::Standard,
            Self::DmlAudit => AuditLevel::Forensic,
        }
    }
}

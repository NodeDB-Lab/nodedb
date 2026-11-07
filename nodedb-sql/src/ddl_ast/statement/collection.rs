// SPDX-License-Identifier: Apache-2.0

use nodedb_types::{AuditDmlMode, QuotaSpec};

/// Temporal anchor for a `CLONE DATABASE` statement.
///
/// Every `match` on this enum must be exhaustive — no `_ =>` arms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloneAsOf {
    /// Use the source database's current commit LSN at clone time.
    /// Corresponds to the bare `CLONE DATABASE … FROM …` form or the
    /// explicit `… AS OF SYSTEM TIME LATEST` form.
    Latest,
    /// Use the LSN corresponding to the given milliseconds-since-epoch
    /// timestamp, resolved from the WAL time anchors.
    ///
    /// Corresponds to `… AS OF SYSTEM TIME <ms>`.
    SystemTimeMs(i64),
}

/// Operations available on `ALTER DATABASE <name> <operation>`.
///
/// Every variant must be matched exhaustively — no `_ =>` arms anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterDatabaseOperation {
    /// `ALTER DATABASE <name> RENAME TO <new_name>`
    Rename { new_name: String },
    /// `ALTER DATABASE <name> SET QUOTA (max_memory_bytes = ..., ...)`
    ///
    /// All fields in the spec are optional; absent fields leave the existing
    /// quota value unchanged (merged at apply time with the stored record or
    /// `QuotaRecord::DEFAULT`).
    SetQuota(QuotaSpec),
    /// `ALTER DATABASE <name> SET DEFAULT` — marks this database as the
    /// per-user default for future sessions. Returns
    /// `FEATURE_NOT_YET_IMPLEMENTED` until the per-user default-database
    /// binding lands; the canonical path is
    /// `ALTER USER <name> SET DEFAULT DATABASE <db>`.
    SetDefault,
    /// `ALTER DATABASE <name> MATERIALIZE` — triggers background materialization
    /// of a cloned database. Returns `FEATURE_NOT_YET_IMPLEMENTED` until the
    /// clone/mirror subsystem lands.
    Materialize,
    /// `ALTER DATABASE <name> PROMOTE` — promotes a mirror to writable primary.
    Promote,
    /// `ALTER DATABASE <name> MIRROR RESUME` — a failed mirror's link retries from
    /// its replicated positions.
    MirrorResume,
    /// `ALTER DATABASE <name> MIRROR REBOOTSTRAP` — a failed mirror removes what
    /// it holds and takes a new capture of its source.
    MirrorRebootstrap,
    /// `ALTER DATABASE <name> SET AUDIT_DML = <mode>` — sets the DML audit level.
    SetAuditDml(AuditDmlMode),
    /// `ALTER DATABASE <name> SET IDLE_TIMEOUT = <secs>` — sets the idle session
    /// timeout in seconds for sessions in this database. `0` disables the per-database
    /// timeout (falls back to the global `idle_timeout_secs` setting).
    SetIdleTimeout(u64),
}

/// Operations available on `ALTER TENANT <name> IN DATABASE <db> <operation>`.
///
/// Every variant must be matched exhaustively — no `_ =>` arms anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTenantOperation {
    /// `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)`
    SetQuota(QuotaSpec),
}

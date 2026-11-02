// SPDX-License-Identifier: BUSL-1.1

//! Types stored in the `_system.databases` catalog table.

use nodedb_types::{AuditDmlMode, DatabaseId, MirrorOrigin};

/// Lifecycle status of a database.
///
/// Exhaustive matches are required — no `_ =>` arms anywhere.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum DatabaseStatus {
    /// Normal operating state.
    Active,
    /// Database has been dropped but is within the retention window.
    /// Collections remain accessible in read-only mode for un-drop.
    Deactivated,
    /// A `CLONE DATABASE` operation is pending materialization.
    /// Writes are rejected until materialization completes or the
    /// clone is promoted/abandoned.
    Cloning,
    /// A `MIRROR DATABASE` observer: read-only replica of a source.
    /// Promoted to `Active` via `ALTER DATABASE PROMOTE`.
    Mirroring,
}

/// Persisted descriptor for a single database row in `_system.databases`.
#[derive(
    Debug,
    Clone,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    serde::Serialize,
    serde::Deserialize,
)]
#[msgpack(map)]
pub struct DatabaseDescriptor {
    pub id: DatabaseId,
    /// Human-readable name. Mutable via `ALTER DATABASE RENAME`.
    /// The durable identity is always `id`, never `name`.
    pub name: String,
    pub status: DatabaseStatus,
    /// WAL LSN at which this database was created (or 0 for the
    /// bootstrapped `default` database).
    pub created_at_lsn: u64,
    /// Quota reference id — links to the quota hierarchy.
    /// `0` means "no explicit quota; inherits global default".
    pub quota_ref: u64,
    /// For cloned databases: the source `DatabaseId` this was
    /// forked from, and the LSN / system-time boundary.
    /// `None` for non-clones.
    pub parent_clone: Option<ParentCloneRef>,
    /// For mirror databases: the source cluster/database and replication state.
    /// `None` for non-mirrors and post-promotion mirrors where the lineage
    /// record was cleared. Post-promotion databases retain this field as a
    /// historical record of their origin; `status` will be `Promoted`.
    pub mirror_origin: Option<MirrorOrigin>,
    /// DML audit level for this database.
    /// Set via `ALTER DATABASE <name> SET AUDIT_DML = <mode>`.
    pub audit_dml: AuditDmlMode,
    /// Idle session timeout in seconds for sessions in this database.
    /// `0` means no per-database timeout (falls back to global `idle_timeout_secs`).
    /// Set via `ALTER DATABASE <name> SET IDLE_TIMEOUT = <secs>`.
    pub idle_session_timeout_secs: u64,
}

/// Reference to a clone's origin. Populated by the clone subsystem and
/// stored on the descriptor.
#[derive(
    Debug,
    Clone,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    serde::Serialize,
    serde::Deserialize,
)]
#[msgpack(map)]
pub struct ParentCloneRef {
    pub source_db_id: DatabaseId,
    /// WAL LSN at which the clone was taken (the `AS OF` point).
    pub as_of_lsn: u64,
    /// System-time milliseconds at the `AS OF` point.
    pub as_of_ms: u64,
    /// Surrogate high-water captured from the source's `SurrogateAssigner`
    /// at clone-create time.  Used by the lazy KV read path to filter out
    /// source rows whose binding was allocated AFTER the clone — those
    /// rows belong strictly to post-clone writes and must not leak through
    /// source delegation. `None` means no ceiling.
    pub kv_surrogate_ceiling: Option<u32>,
}

impl DatabaseDescriptor {
    /// Construct a minimal descriptor for the built-in `default` database.
    pub fn default_db() -> Self {
        Self {
            id: DatabaseId::DEFAULT,
            name: "default".to_string(),
            status: DatabaseStatus::Active,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: None,
            audit_dml: AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Type definitions for stored procedure catalog storage.

use nodedb_types::id::DatabaseId;

/// Parameter direction for stored procedures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ParamDirection {
    In = 0,
    Out = 1,
    InOut = 2,
}

impl ParamDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::In => "IN",
            Self::Out => "OUT",
            Self::InOut => "INOUT",
        }
    }
}

/// A stored procedure parameter.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ProcedureParam {
    pub name: String,
    pub data_type: String,
    pub direction: ParamDirection,
}

/// Routability classification for procedure DML targets.
///
/// Determined at CREATE PROCEDURE time by parsing the body for DML target
/// collections. Used by the Event Plane cron scheduler for per-collection
/// affinity routing of `CALL procedure(...)` in scheduled jobs.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum ProcedureRoutability {
    /// Procedure targets a single collection — can be routed to that
    /// collection's shard leader for locality.
    SingleCollection(String),
    /// Procedure targets multiple collections or has dynamic SQL —
    /// must execute on the coordinator (no affinity routing).
    #[default]
    MultiCollection,
}

/// Serializable stored procedure definition for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredProcedure {
    pub tenant_id: u64,
    /// Database namespace.
    pub database_id: DatabaseId,
    pub name: String,
    pub parameters: Vec<ProcedureParam>,
    /// Procedural SQL body (BEGIN ... END).
    pub body_sql: String,
    /// Maximum loop iterations.
    pub max_iterations: u64,
    /// Execution timeout in seconds.
    pub timeout_secs: u64,
    /// Routability classification: which collections the procedure targets.
    /// Used by cron scheduler for affinity routing.
    pub routability: ProcedureRoutability,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version, stamped by the metadata applier.
    /// See `StoredCollection::descriptor_version`.
    pub descriptor_version: u64,
    /// HLC stamped by the metadata applier at commit time.
    pub modification_hlc: nodedb_types::Hlc,
}

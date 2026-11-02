// SPDX-License-Identifier: BUSL-1.1

//! Type definitions for trigger catalog storage.

use nodedb_types::id::DatabaseId;

/// When the trigger fires relative to the DML operation.
///
/// A BEFORE or INSTEAD OF body runs in the Control Plane before the
/// triggering write stages. It joins the triggering statement's transaction:
/// the client's transaction block, or an implicit one around the statement.
/// The write and the body's writes commit together or not at all. A body
/// error rolls back the body's own writes and fails the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TriggerTiming {
    Before = 0,
    After = 1,
    InsteadOf = 2,
}

impl TriggerTiming {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Before => "BEFORE",
            Self::After => "AFTER",
            Self::InsteadOf => "INSTEAD OF",
        }
    }
}

/// Which DML event(s) the trigger responds to.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TriggerEvents {
    pub on_insert: bool,
    pub on_update: bool,
    pub on_delete: bool,
}

impl TriggerEvents {
    pub fn display(&self) -> String {
        let mut parts = Vec::new();
        if self.on_insert {
            parts.push("INSERT");
        }
        if self.on_update {
            parts.push("UPDATE");
        }
        if self.on_delete {
            parts.push("DELETE");
        }
        parts.join(" OR ")
    }
}

/// Execution mode for AFTER triggers.
///
/// Controls where and when the trigger body executes. A body's writes fire
/// no triggers:
/// - `Async` (default): Event Plane, after the triggering write commits. The
///   body commits as its own transaction.
/// - `Sync`: Control Plane write path, after the triggering write stages. The
///   body joins the triggering statement's transaction.
/// - `Deferred`: Event Plane, once the triggering transaction commits. The
///   body commits as its own transaction.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TriggerExecutionMode {
    /// Trigger fires asynchronously via Event Plane after commit.
    /// Default. Eventually consistent side effects. Zero write latency impact.
    #[default]
    Async = 0,
    /// Trigger fires synchronously in the Control Plane write path, after
    /// the triggering write stages. Adds trigger execution time to write
    /// latency. The body joins the triggering statement's transaction: a
    /// failed body fails the statement, and the statement's transaction
    /// commits neither the write nor the body's writes.
    /// Cross-shard SYNC triggers are rejected at CREATE TRIGGER time.
    Sync = 1,
    /// Trigger fires from the Event Plane once the triggering transaction
    /// commits, batched per COMMIT. The body commits as its own transaction.
    Deferred = 2,
}

impl TriggerExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Async => "ASYNC",
            Self::Sync => "SYNC",
            Self::Deferred => "DEFERRED",
        }
    }
}

/// Row-level or statement-level granularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TriggerGranularity {
    Row = 0,
    Statement = 1,
}

impl TriggerGranularity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Row => "FOR EACH ROW",
            Self::Statement => "FOR EACH STATEMENT",
        }
    }
}

/// Security execution mode for triggers and functions.
///
/// - `Invoker` (default): body executes with caller's credentials. Subqueries
///   and DML subject to caller's GRANT/DENY and RLS policies.
/// - `Definer`: body executes with the trigger/function owner's credentials.
///   Allows privileged operations (e.g., admin-owned trigger can update system tables).
///   Tenant boundary still enforced — DEFINER cannot cross tenant boundaries.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TriggerSecurity {
    /// Not selectable at `CREATE TRIGGER`: an asynchronously-fired body has no
    /// invoking session to adopt. Retained so existing catalog rows that stored
    /// discriminant 0 still decode; they execute as `Definer`, which is what
    /// the dispatcher has always done.
    Invoker = 0,
    #[default]
    Definer = 1,
}

impl TriggerSecurity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invoker => "INVOKER",
            Self::Definer => "DEFINER",
        }
    }
}

/// Batch execution mode for trigger bodies, determined at CREATE TRIGGER time.
///
/// Controls whether the trigger can process multiple rows as a batch (vectorized)
/// or must fall back to row-at-a-time execution.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TriggerBatchMode {
    /// Trigger body has a single uniform DML target — safe for batch execution.
    /// All rows can be collected into a RecordBatch and the trigger body's DML
    /// can be dispatched as a single bulk INSERT/UPDATE/DELETE.
    #[default]
    BatchSafe = 0,
    /// Trigger body has row-dependent control flow or multiple DML targets.
    /// Must execute row-at-a-time (the current behavior).
    RowAtATime = 1,
}

impl TriggerBatchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BatchSafe => "BATCH_SAFE",
            Self::RowAtATime => "ROW_AT_A_TIME",
        }
    }
}

/// Serializable trigger definition for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredTrigger {
    pub tenant_id: u64,
    /// Database namespace.
    pub database_id: DatabaseId,
    pub name: String,
    /// Collection this trigger is attached to.
    pub collection: String,
    pub timing: TriggerTiming,
    pub events: TriggerEvents,
    pub granularity: TriggerGranularity,
    /// Optional WHEN condition (SQL expression). Trigger body only fires
    /// if this predicate evaluates to true for the row.
    pub when_condition: Option<String>,
    /// Procedural SQL body (BEGIN ... END).
    pub body_sql: String,
    /// Firing priority. Lower numbers fire first.
    /// Tiebreaker: alphabetical by trigger name.
    pub priority: i32,
    /// Whether the trigger is currently enabled.
    pub enabled: bool,
    /// Execution mode: ASYNC (Event Plane), SYNC (write path), DEFERRED (COMMIT time).
    pub execution_mode: TriggerExecutionMode,
    /// Security mode: INVOKER (default) or DEFINER.
    pub security: TriggerSecurity,
    /// Batch execution mode: determined at CREATE time by analyzing the body.
    pub batch_mode: TriggerBatchMode,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version, stamped by the metadata applier.
    pub descriptor_version: u64,
    /// HLC stamped by the metadata applier at commit time.
    pub modification_hlc: nodedb_types::Hlc,
}

impl StoredTrigger {
    /// Sort key for deterministic execution order: (priority, name).
    pub fn sort_key(&self) -> (i32, &str) {
        (self.priority, &self.name)
    }
}

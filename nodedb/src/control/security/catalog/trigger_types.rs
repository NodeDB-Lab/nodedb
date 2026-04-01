//! Type definitions for trigger catalog storage.

/// When the trigger fires relative to the DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
/// Controls where and when the trigger body executes:
/// - `Async` (default): Event Plane, eventually consistent, zero write latency impact.
/// - `Sync`: Control Plane write path, same logical transaction, adds to write latency.
/// - `Deferred`: Data Plane at COMMIT time, same transaction, batched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TriggerExecutionMode {
    /// Trigger fires asynchronously via Event Plane after commit.
    /// Default. Eventually consistent side effects. Zero write latency impact.
    #[default]
    Async,
    /// Trigger fires synchronously in the Control Plane write path.
    /// ACID (same logical transaction). Adds trigger execution time to write latency.
    /// Cross-shard SYNC triggers are rejected at CREATE TRIGGER time.
    Sync,
    /// Trigger fires at COMMIT time in the Data Plane, batched.
    /// ACID (same transaction). Only adds latency at COMMIT, not per-statement.
    Deferred,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerGranularity {
    Row,
    Statement,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TriggerSecurity {
    #[default]
    Invoker,
    Definer,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TriggerBatchMode {
    /// Trigger body has a single uniform DML target — safe for batch execution.
    /// All rows can be collected into a RecordBatch and the trigger body's DML
    /// can be dispatched as a single bulk INSERT/UPDATE/DELETE.
    #[default]
    BatchSafe,
    /// Trigger body has row-dependent control flow or multiple DML targets.
    /// Must execute row-at-a-time (the current behavior).
    RowAtATime,
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredTrigger {
    pub tenant_id: u32,
    pub name: String,
    /// Collection this trigger is attached to.
    pub collection: String,
    pub timing: TriggerTiming,
    pub events: TriggerEvents,
    pub granularity: TriggerGranularity,
    /// Optional WHEN condition (SQL expression). Trigger body only fires
    /// if this predicate evaluates to true for the row.
    #[serde(default)]
    pub when_condition: Option<String>,
    /// Procedural SQL body (BEGIN ... END).
    pub body_sql: String,
    /// Firing priority. Lower numbers fire first.
    /// Tiebreaker: alphabetical by trigger name.
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// Whether the trigger is currently enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Execution mode: ASYNC (Event Plane), SYNC (write path), DEFERRED (COMMIT time).
    /// Backward-compatible: defaults to ASYNC for triggers created before this field existed.
    #[serde(default)]
    pub execution_mode: TriggerExecutionMode,
    /// Security mode: INVOKER (default) or DEFINER.
    #[serde(default)]
    pub security: TriggerSecurity,
    /// Batch execution mode: determined at CREATE time by analyzing the body.
    #[serde(default)]
    pub batch_mode: TriggerBatchMode,
    pub owner: String,
    pub created_at: u64,
}

fn default_priority() -> i32 {
    0
}

fn default_enabled() -> bool {
    true
}

impl StoredTrigger {
    /// Sort key for deterministic execution order: (priority, name).
    pub fn sort_key(&self) -> (i32, &str) {
        (self.priority, &self.name)
    }
}

//! Schedule definition types.

/// What to do when a scheduled execution was missed (server was down).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum MissedPolicy {
    /// Skip missed executions (default). Resume from next scheduled time.
    #[default]
    Skip = 0,
    /// Catch up: run once immediately for all missed executions.
    CatchUp = 1,
    /// Queue: run each missed execution in order.
    Queue = 2,
}

impl MissedPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "SKIP",
            Self::CatchUp => "CATCH_UP",
            Self::Queue => "QUEUE",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SKIP" => Some(Self::Skip),
            "CATCH_UP" | "CATCHUP" => Some(Self::CatchUp),
            "QUEUE" => Some(Self::Queue),
            _ => None,
        }
    }
}

/// Where the schedule runs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ScheduleScope {
    /// Runs on the shard leader for the target collection (or `_system` coordinator
    /// for cross-collection jobs). In single-node mode, always runs locally.
    #[default]
    Normal = 0,
    /// Runs on the creating node only. Never migrates, never syncs.
    Local = 1,
}

impl ScheduleScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Local => "LOCAL",
        }
    }
}

/// Persistent definition of a scheduled job. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ScheduleDef {
    /// Tenant that owns this schedule.
    pub tenant_id: u32,
    /// Schedule name (unique per tenant).
    pub name: String,
    /// Cron expression (5-field: minute hour day_of_month month day_of_week).
    pub cron_expr: String,
    /// SQL body to execute on each fire.
    pub body_sql: String,
    /// Execution scope.
    pub scope: ScheduleScope,
    /// What to do when executions are missed.
    pub missed_policy: MissedPolicy,
    /// Whether concurrent runs are allowed (default: true).
    pub allow_overlap: bool,
    /// Whether the schedule is currently enabled.
    pub enabled: bool,
    /// Target collection inferred from the SQL body (e.g., "orders" from
    /// `DELETE FROM orders ...`). Used for shard affinity in cluster mode.
    /// `None` for cross-collection or opaque jobs → runs on `_system` coordinator.
    #[msgpack(default)]
    pub target_collection: Option<String>,
    /// Owner (creator). Job runs with this user's privileges.
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// A completed job execution record.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct JobRun {
    /// Schedule name.
    pub schedule_name: String,
    /// Tenant ID.
    pub tenant_id: u32,
    /// When the job started (epoch millis).
    pub started_at: u64,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Whether the job succeeded.
    pub success: bool,
    /// Error message (if failed).
    pub error: Option<String>,
}

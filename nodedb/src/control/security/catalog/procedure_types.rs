//! Type definitions for stored procedure catalog storage.

/// Parameter direction for stored procedures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ParamDirection {
    In,
    Out,
    InOut,
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProcedureParam {
    pub name: String,
    pub data_type: String,
    #[serde(default = "default_direction")]
    pub direction: ParamDirection,
}

fn default_direction() -> ParamDirection {
    ParamDirection::In
}

/// Serializable stored procedure definition for redb storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredProcedure {
    pub tenant_id: u32,
    pub name: String,
    pub parameters: Vec<ProcedureParam>,
    /// Procedural SQL body (BEGIN ... END).
    pub body_sql: String,
    /// Maximum loop iterations (default 1_000_000).
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u64,
    /// Execution timeout in seconds (default 60).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    pub owner: String,
    pub created_at: u64,
}

/// Default max loop iterations — allows moderate data processing (1M rows).
/// Override per-procedure via `WITH (MAX_ITERATIONS = N)`.
fn default_max_iterations() -> u64 {
    1_000_000
}

/// Default execution timeout — prevents long-running procedures from
/// blocking the Tokio Control Plane. Override via `WITH (TIMEOUT = N)`.
fn default_timeout_secs() -> u64 {
    60
}

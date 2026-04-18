//! Streaming materialized view type definitions.

/// Persistent definition of a streaming materialized view.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StreamingMvDef {
    /// Tenant that owns this MV.
    pub tenant_id: u32,
    /// MV name (unique per tenant).
    pub name: String,
    /// Source change stream name.
    pub source_stream: String,
    /// GROUP BY column names (extracted from the query).
    pub group_by_columns: Vec<String>,
    /// Aggregate functions to compute. Each is (output_column, function, input_expression).
    pub aggregates: Vec<AggDef>,
    /// Optional WHERE filter expression (raw SQL fragment).
    pub filter_expr: Option<String>,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// A single aggregate function definition.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct AggDef {
    /// Output column name (e.g., "cnt", "total_revenue").
    pub output_name: String,
    /// Aggregate function: COUNT, SUM, MIN, MAX, AVG.
    pub function: AggFunction,
    /// Input expression (e.g., "doc_get(new_value, '$.total')").
    /// For COUNT(*), this is empty.
    pub input_expr: String,
}

/// Supported aggregate functions for streaming MVs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum AggFunction {
    Count = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
    Avg = 4,
}

impl AggFunction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "COUNT" => Some(Self::Count),
            "SUM" => Some(Self::Sum),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "AVG" => Some(Self::Avg),
            _ => None,
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Streaming materialized view type definitions.

/// Persistent definition of a streaming materialized view.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StreamingMvDef {
    /// Database that owns this MV.
    pub database_id: crate::types::DatabaseId,
    /// Tenant that owns this MV.
    pub tenant_id: u64,
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

impl AggDef {
    /// The stored column this aggregate reads from an event's `new_value`, or
    /// `None` when it reads no column value.
    ///
    /// `COUNT` counts events and never looks at a column, and an empty input
    /// expression (`COUNT(*)`) names none. Anything else is a field name,
    /// written plainly or wrapped in `doc_get(new_value, '$.field')`.
    ///
    /// The processor extracts the aggregate input through this, and the
    /// definition-time redaction refusal decides on it, so the column a rule is
    /// matched against is the same one the MV would actually persist.
    pub fn source_field(&self) -> Option<&str> {
        if self.function == AggFunction::Count {
            return None;
        }
        let field = if self.input_expr.contains("doc_get") {
            self.input_expr
                .split("'$.")
                .nth(1)
                .and_then(|rest| rest.split('\'').next())
                .unwrap_or(&self.input_expr)
        } else {
            self.input_expr.trim()
        };
        (!field.is_empty()).then_some(field)
    }
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

//! Alert rule type definitions.

use serde::{Deserialize, Serialize};

/// Complete definition of a threshold alert rule.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct AlertDef {
    /// Owning tenant.
    pub tenant_id: u32,
    /// Alert name (unique per tenant).
    pub name: String,
    /// Source collection (must be timeseries).
    pub collection: String,
    /// Pre-filter expression (e.g., `device_type = 'compressor'`).
    /// Applied as WHERE clause before aggregation.
    pub where_filter: Option<String>,
    /// Aggregate condition expression (e.g., `AVG(temperature) > 90.0`).
    /// The aggregate function + column + comparison operator + threshold.
    pub condition: AlertCondition,
    /// Columns to GROUP BY (alert fires per group, e.g., per device_id).
    pub group_by: Vec<String>,
    /// Aggregation window duration in milliseconds.
    pub window_ms: u64,
    /// Consecutive windows condition must be true to fire.
    pub fire_after: u32,
    /// Consecutive windows condition must be false to clear.
    pub recover_after: u32,
    /// Alert severity label.
    pub severity: String,
    /// Notification targets (all notified on fire/clear).
    pub notify_targets: Vec<NotifyTarget>,
    /// Whether this alert is actively evaluated.
    pub enabled: bool,
    /// Who created this alert.
    pub owner: String,
    /// Unix timestamp (seconds) when created.
    pub created_at: u64,
}

/// The aggregate condition: function(column) op threshold.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct AlertCondition {
    /// Aggregate function name (e.g., "avg", "max", "min", "sum", "count").
    pub agg_func: String,
    /// Column to aggregate (e.g., "temperature"). "*" for COUNT.
    pub column: String,
    /// Comparison operator.
    pub op: CompareOp,
    /// Threshold value.
    pub threshold: f64,
}

/// Comparison operators for alert conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum CompareOp {
    Gt = 0,
    Gte = 1,
    Lt = 2,
    Lte = 3,
    Eq = 4,
    Neq = 5,
}

impl CompareOp {
    /// Parse from SQL operator string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Gte),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Lte),
            "=" | "==" => Some(Self::Eq),
            "!=" | "<>" => Some(Self::Neq),
            _ => None,
        }
    }

    /// Evaluate the comparison.
    pub fn evaluate(&self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Gte => value >= threshold,
            Self::Lt => value < threshold,
            Self::Lte => value <= threshold,
            Self::Eq => (value - threshold).abs() < f64::EPSILON,
            Self::Neq => (value - threshold).abs() >= f64::EPSILON,
        }
    }

    /// SQL representation.
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Eq => "=",
            Self::Neq => "!=",
        }
    }
}

/// Where to send notifications on alert fire/clear.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum NotifyTarget {
    /// Publish alert event to a CDC topic.
    Topic { name: String },
    /// HTTP POST to external endpoint.
    Webhook { url: String },
    /// Insert alert event as a row into a table.
    InsertInto { table: String, columns: Vec<String> },
}

/// Per-group alert state (Active or Cleared).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertStatus {
    /// Condition not met (or not enough consecutive windows).
    #[default]
    Cleared,
    /// Condition met for >= fire_after consecutive windows.
    Active,
}

/// Notification event payload sent to targets on fire/clear.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub alert_name: String,
    pub group_key: String,
    pub severity: String,
    pub status: String,
    pub value: f64,
    pub threshold: f64,
    pub timestamp_ms: u64,
    pub collection: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_op_evaluate() {
        assert!(CompareOp::Gt.evaluate(91.0, 90.0));
        assert!(!CompareOp::Gt.evaluate(90.0, 90.0));
        assert!(CompareOp::Gte.evaluate(90.0, 90.0));
        assert!(CompareOp::Lt.evaluate(89.0, 90.0));
        assert!(CompareOp::Lte.evaluate(90.0, 90.0));
        assert!(CompareOp::Eq.evaluate(90.0, 90.0));
        assert!(CompareOp::Neq.evaluate(91.0, 90.0));
    }

    #[test]
    fn compare_op_parse() {
        assert_eq!(CompareOp::parse(">"), Some(CompareOp::Gt));
        assert_eq!(CompareOp::parse(">="), Some(CompareOp::Gte));
        assert_eq!(CompareOp::parse("<"), Some(CompareOp::Lt));
        assert_eq!(CompareOp::parse("<="), Some(CompareOp::Lte));
        assert_eq!(CompareOp::parse("="), Some(CompareOp::Eq));
        assert_eq!(CompareOp::parse("!="), Some(CompareOp::Neq));
        assert_eq!(CompareOp::parse("<>"), Some(CompareOp::Neq));
        assert_eq!(CompareOp::parse("LIKE"), None);
    }
}

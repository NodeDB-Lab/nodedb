//! Designated timestamp column auto-detection.
//!
//! Three-tier heuristic:
//! 1. Column type is DateTime/Timestamp → use it
//! 2. Column name matches common patterns → use it
//! 3. Column is i64 and values look epoch-like → weak signal, warn user
//!
//! Monotonicity is NOT required (O3 ingest is explicitly supported).

use super::columnar_memtable::ColumnType;

/// Result of timestamp column detection.
#[derive(Debug, PartialEq)]
pub enum TsDetection {
    /// Found by type (strongest signal).
    ByType {
        column_index: usize,
        column_name: String,
    },
    /// Found by name pattern (strong signal).
    ByName {
        column_index: usize,
        column_name: String,
    },
    /// Found by value heuristic (weak signal — warn user).
    ByValueHeuristic {
        column_index: usize,
        column_name: String,
    },
    /// No timestamp column detected.
    NotFound,
}

impl TsDetection {
    pub fn column_index(&self) -> Option<usize> {
        match self {
            Self::ByType { column_index, .. }
            | Self::ByName { column_index, .. }
            | Self::ByValueHeuristic { column_index, .. } => Some(*column_index),
            Self::NotFound => None,
        }
    }

    pub fn is_weak(&self) -> bool {
        matches!(self, Self::ByValueHeuristic { .. })
    }
}

/// Common timestamp column name patterns (case-insensitive).
const TS_NAME_PATTERNS: &[&str] = &[
    "ts",
    "timestamp",
    "time",
    "created_at",
    "event_time",
    "event_timestamp",
    "occurred_at",
    "recorded_at",
    "ingested_at",
    "date",
    "datetime",
];

/// Detect the designated timestamp column from a schema.
pub fn detect_timestamp(
    columns: &[(String, ColumnType)],
    sample_values: Option<&[i64]>,
) -> TsDetection {
    // Tier 1: Column type is Timestamp.
    for (i, (name, ty)) in columns.iter().enumerate() {
        if *ty == ColumnType::Timestamp {
            return TsDetection::ByType {
                column_index: i,
                column_name: name.clone(),
            };
        }
    }

    // Tier 2: Column name matches common patterns.
    for (i, (name, _ty)) in columns.iter().enumerate() {
        let lower = name.to_lowercase();
        if TS_NAME_PATTERNS.contains(&lower.as_str()) {
            return TsDetection::ByName {
                column_index: i,
                column_name: name.clone(),
            };
        }
    }

    // Tier 3: First i64 column with epoch-like values.
    if let Some(values) = sample_values {
        for (i, (name, ty)) in columns.iter().enumerate() {
            if *ty == ColumnType::Int64 && looks_epoch_like(values) {
                return TsDetection::ByValueHeuristic {
                    column_index: i,
                    column_name: name.clone(),
                };
            }
        }
    }

    TsDetection::NotFound
}

/// Check if a sample of i64 values look like epoch timestamps.
///
/// Heuristic: at least 80% of values fall within a reasonable epoch range
/// (2000-01-01 to 2100-01-01 in seconds, milliseconds, microseconds, or nanoseconds).
fn looks_epoch_like(values: &[i64]) -> bool {
    if values.is_empty() {
        return false;
    }

    // Epoch ranges for different resolutions.
    let ranges: &[(i64, i64)] = &[
        (946_684_800, 4_102_444_800),                         // seconds
        (946_684_800_000, 4_102_444_800_000),                 // milliseconds
        (946_684_800_000_000, 4_102_444_800_000_000),         // microseconds
        (946_684_800_000_000_000, 4_102_444_800_000_000_000), // nanoseconds
    ];

    let threshold = (values.len() * 4) / 5; // 80%
    for &(min, max) in ranges {
        let matches = values.iter().filter(|&&v| v >= min && v <= max).count();
        if matches >= threshold {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_by_type() {
        let cols = vec![
            ("id".into(), ColumnType::Int64),
            ("ts".into(), ColumnType::Timestamp),
            ("value".into(), ColumnType::Float64),
        ];
        let result = detect_timestamp(&cols, None);
        assert_eq!(
            result,
            TsDetection::ByType {
                column_index: 1,
                column_name: "ts".into(),
            }
        );
    }

    #[test]
    fn detect_by_name() {
        let cols = vec![
            ("id".into(), ColumnType::Int64),
            ("created_at".into(), ColumnType::Int64),
            ("value".into(), ColumnType::Float64),
        ];
        let result = detect_timestamp(&cols, None);
        assert_eq!(
            result,
            TsDetection::ByName {
                column_index: 1,
                column_name: "created_at".into(),
            }
        );
    }

    #[test]
    fn detect_by_name_case_insensitive() {
        // CamelCase "EventTimestamp" doesn't match exact lowercase name check.
        let cols_camel: Vec<(String, ColumnType)> = vec![
            ("EventTimestamp".into(), ColumnType::Int64),
            ("value".into(), ColumnType::Float64),
        ];
        assert!(!matches!(
            detect_timestamp(&cols_camel, None),
            TsDetection::ByName { .. }
        ));

        // Lowercase "event_timestamp" does match.
        let cols_lower: Vec<(String, ColumnType)> = vec![
            ("event_timestamp".into(), ColumnType::Int64),
            ("value".into(), ColumnType::Float64),
        ];
        assert!(matches!(
            detect_timestamp(&cols_lower, None),
            TsDetection::ByName { .. }
        ));
    }

    #[test]
    fn detect_by_value_heuristic_ms() {
        let cols = vec![
            ("t".into(), ColumnType::Int64),
            ("value".into(), ColumnType::Float64),
        ];
        // Epoch milliseconds around 2024.
        let values = vec![
            1_704_067_200_000,
            1_704_153_600_000,
            1_704_240_000_000,
            1_704_326_400_000,
            1_704_412_800_000,
        ];
        let result = detect_timestamp(&cols, Some(&values));
        assert!(matches!(result, TsDetection::ByValueHeuristic { .. }));
        assert!(result.is_weak());
    }

    #[test]
    fn detect_by_value_heuristic_seconds() {
        let cols = vec![
            ("epoch".into(), ColumnType::Int64),
            ("val".into(), ColumnType::Float64),
        ];
        let values = vec![1_704_067_200, 1_704_153_600, 1_704_240_000];
        let result = detect_timestamp(&cols, Some(&values));
        assert!(matches!(result, TsDetection::ByValueHeuristic { .. }));
    }

    #[test]
    fn no_detection() {
        let cols = vec![
            ("id".into(), ColumnType::Int64),
            ("value".into(), ColumnType::Float64),
        ];
        // Non-epoch values.
        let values = vec![1, 2, 3, 4, 5];
        let result = detect_timestamp(&cols, Some(&values));
        assert_eq!(result, TsDetection::NotFound);
    }

    #[test]
    fn type_takes_priority_over_name() {
        let cols = vec![
            ("timestamp".into(), ColumnType::Int64), // name match
            ("t".into(), ColumnType::Timestamp),     // type match
        ];
        let result = detect_timestamp(&cols, None);
        // Type should win.
        assert_eq!(
            result,
            TsDetection::ByType {
                column_index: 1,
                column_name: "t".into(),
            }
        );
    }

    #[test]
    fn name_takes_priority_over_value() {
        let cols = vec![
            ("event_time".into(), ColumnType::Int64),
            ("other".into(), ColumnType::Int64),
        ];
        let values = vec![1_704_067_200_000]; // epoch-like
        let result = detect_timestamp(&cols, Some(&values));
        // Name should win.
        assert!(matches!(result, TsDetection::ByName { .. }));
    }
}

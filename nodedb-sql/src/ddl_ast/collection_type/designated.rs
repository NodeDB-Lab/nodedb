// SPDX-License-Identifier: Apache-2.0

//! Designated-column resolution for the columnar-family engines: the
//! timeseries time key and the spatial geometry column, plus the reserved
//! column names no declaration may claim.

use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use crate::error::SqlError;

/// Column names the columnar-family storage core stamps itself. A
/// declaration that claims one of them would be shadowed at write time, so
/// it is rejected at DDL instead.
const RESERVED_COLUMNS: &[&str] = &[TS_SYSTEM, TS_SERIES, TS_VALID_FROM, TS_VALID_UNTIL];

/// Reject any declared column whose name is reserved for engine use.
pub(crate) fn reject_reserved_columns(columns: &[(String, String)]) -> Result<(), SqlError> {
    for (name, _) in columns {
        if RESERVED_COLUMNS
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        {
            return Err(SqlError::Parse {
                detail: format!(
                    "column name '{name}' is reserved for engine-managed \
                     storage and cannot be declared"
                ),
            });
        }
    }
    Ok(())
}

/// Resolve the designated time-key column of a timeseries collection.
///
/// The resolved name is authoritative: it becomes the collection's storage
/// time column, so every value written to it and read back from it is the
/// user's own. Resolution is deliberately unambiguous:
///
/// - An explicit `TIME_KEY` modifier wins. Exactly one column may carry it.
/// - With no modifier, a single bare `TIMESTAMP` / `TIMESTAMPTZ` column is
///   accepted as the designation.
/// - Two or more timestamp columns and no modifier is ambiguous — which one
///   partitions the collection cannot be guessed, so it is an error.
pub(crate) fn resolve_time_key(columns: &[(String, String)]) -> Result<String, SqlError> {
    let marked: Vec<&String> = columns
        .iter()
        .filter(|(_, t)| t.to_uppercase().contains("TIME_KEY"))
        .map(|(n, _)| n)
        .collect();
    match marked.as_slice() {
        [single] => return Ok((*single).clone()),
        [] => {}
        _ => {
            let names: Vec<&str> = marked.iter().map(|n| n.as_str()).collect();
            return Err(SqlError::Parse {
                detail: format!(
                    "timeseries collections take exactly one TIME_KEY column; found {}",
                    names.join(", ")
                ),
            });
        }
    }

    let timestamps: Vec<&String> = columns
        .iter()
        .filter(|(_, t)| t.to_uppercase().starts_with("TIMESTAMP"))
        .map(|(n, _)| n)
        .collect();
    match timestamps.as_slice() {
        [single] => Ok((*single).clone()),
        [] => Err(SqlError::MissingField {
            field: "time_key".to_string(),
            context: "timeseries engine".to_string(),
        }),
        _ => {
            let names: Vec<&str> = timestamps.iter().map(|n| n.as_str()).collect();
            Err(SqlError::Parse {
                detail: format!(
                    "timeseries collection declares several timestamp columns ({}); \
                     mark the designated one with TIME_KEY",
                    names.join(", ")
                ),
            })
        }
    }
}

/// The time-key column [`resolve_time_key`] designates over planner columns.
///
/// Each column offers its declared type text. A column with no declared text
/// offers `TIMESTAMP` when its type is a timestamp, else nothing. `None` when
/// the rule designates no column, or the declarations are ambiguous.
pub(crate) fn designated_time_key(columns: &[crate::types::ColumnInfo]) -> Option<String> {
    use crate::types_expr::SqlDataType;
    let declared: Vec<(String, String)> = columns
        .iter()
        .map(|c| {
            let text = match (&c.raw_type, &c.data_type) {
                (Some(raw), _) => raw.clone(),
                (None, SqlDataType::Timestamp | SqlDataType::Timestamptz) => "TIMESTAMP".into(),
                (None, _) => String::new(),
            };
            (c.name.clone(), text)
        })
        .collect();
    resolve_time_key(&declared).ok()
}

/// Find the geometry column from the column list.
///
/// Matches columns whose type_str contains `SPATIAL_INDEX` modifier or whose
/// bare type is `GEOMETRY` or `GEOM`.
pub(crate) fn find_geom_col(columns: &[(String, String)]) -> Option<String> {
    columns
        .iter()
        .find(|(_, t)| {
            let u = t.to_uppercase();
            u.contains("SPATIAL_INDEX") || u == "GEOMETRY" || u == "GEOM"
        })
        .map(|(n, _)| n.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn explicit_marker_wins_over_an_earlier_bare_timestamp() {
        let c = cols(&[
            ("created", "TIMESTAMP"),
            ("event_at", "TIMESTAMP TIME_KEY"),
            ("v", "FLOAT"),
        ]);
        assert_eq!(resolve_time_key(&c).unwrap(), "event_at");
    }

    #[test]
    fn bare_timestamp_is_the_designation_when_unique() {
        let c = cols(&[("captured_at", "TIMESTAMP"), ("v", "FLOAT")]);
        assert_eq!(resolve_time_key(&c).unwrap(), "captured_at");
    }

    #[test]
    fn two_markers_are_rejected() {
        let c = cols(&[("a", "TIMESTAMP TIME_KEY"), ("b", "BIGINT TIME_KEY")]);
        let err = resolve_time_key(&c).unwrap_err();
        assert!(err.to_string().contains("exactly one TIME_KEY"), "{err}");
    }

    #[test]
    fn ambiguous_bare_timestamps_are_rejected() {
        let c = cols(&[("created", "TIMESTAMP"), ("updated", "TIMESTAMP")]);
        let err = resolve_time_key(&c).unwrap_err();
        assert!(err.to_string().contains("TIME_KEY"), "{err}");
    }

    #[test]
    fn a_non_timestamp_marker_still_designates() {
        // Epoch-millis time keys are declared BIGINT + TIME_KEY.
        let c = cols(&[("ts", "BIGINT TIME_KEY"), ("v", "FLOAT")]);
        assert_eq!(resolve_time_key(&c).unwrap(), "ts");
    }

    #[test]
    fn reserved_names_are_rejected_case_insensitively() {
        let err = reject_reserved_columns(&cols(&[("_TS_VALID_FROM", "BIGINT")])).unwrap_err();
        assert!(err.to_string().contains("reserved"), "{err}");
    }

    #[test]
    fn ordinary_names_pass_the_reserved_check() {
        assert!(reject_reserved_columns(&cols(&[("ts", "TIMESTAMP"), ("v", "FLOAT")])).is_ok());
    }
}

// SPDX-License-Identifier: Apache-2.0

//! Shared graph types used by both Origin and Lite CSR engines.

use serde::{Deserialize, Serialize};

/// Aggregate stats for a single graph collection (or the full edge store
/// when no collection is specified).
///
/// Returned by [`NodeDb::graph_stats`]. Wire-safe: serializes to/from
/// MessagePack so the value can cross the Data-Plane boundary unchanged.
///
/// The `node_count` field counts distinct node IDs observed as edge
/// endpoints (not necessarily all nodes ever inserted). For Origin, this
/// equals `distinct_node_count` from the persistent stats table; for Lite,
/// it is derived from the CRDT edge store at query time.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct GraphStats {
    /// Name of the collection (or `"__edges"` on Lite when no collection is
    /// supplied).
    pub collection: String,
    /// Distinct node IDs that appear as an edge source or destination.
    pub node_count: u64,
    /// Total number of edges in the collection.
    pub edge_count: u64,
    /// Number of distinct edge label strings.
    pub distinct_label_count: u64,
    /// Per-label edge counts, sorted ascending by label name.
    pub labels: Vec<(String, u64)>,
}

/// Edge traversal direction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum Direction {
    /// Outgoing edges only.
    Out,
    /// Incoming edges only.
    In,
    /// Both directions.
    Both,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
            Self::Both => "both",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A traversal direction string that names no [`Direction`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown direction '{input}': expected 'out', 'in' or 'both'")]
pub struct ParseDirectionError {
    pub input: String,
}

impl std::str::FromStr for Direction {
    type Err = ParseDirectionError;
    /// Case-insensitive. Accepts `out`/`outgoing`, `in`/`incoming` and
    /// `both`/`any`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "out" | "outgoing" => Ok(Self::Out),
            "in" | "incoming" => Ok(Self::In),
            "both" | "any" => Ok(Self::Both),
            _ => Err(ParseDirectionError {
                input: s.to_string(),
            }),
        }
    }
}

impl GraphStats {
    /// Column names of the wire row shape produced by
    /// `SHOW GRAPH STATS [<'collection'>]`. Single source of truth — the
    /// parser validates against this, and clients use it to build the
    /// `columns` slice in tests so the two never drift.
    pub const EXPECTED_COLUMNS: [&'static str; 5] = [
        "collection",
        "node_count",
        "edge_count",
        "distinct_label_count",
        "labels",
    ];

    pub fn zero(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            node_count: 0,
            edge_count: 0,
            distinct_label_count: 0,
            labels: Vec::new(),
        }
    }

    /// Parse the wire shape produced by `SHOW GRAPH STATS [<'collection'>]`
    /// into a vec of `GraphStats`, one entry per row. Used by both the
    /// native and the pgwire remote clients — keeping a single parser here
    /// is what makes the `wire_shape:` smoke-probe error messages a
    /// meaningful diagnostic rather than a coincidence between two copies.
    ///
    /// Expected columns: `(collection, node_count, edge_count,
    /// distinct_label_count, labels)`, where `labels` is a JSON array of
    /// `{"label": str, "count": u64}` objects.
    ///
    /// Both wire paths can deliver count cells as `Value::Integer`
    /// (extended-query / native typed) or `Value::String` (pgwire simple
    /// query text protocol); both shapes are accepted.
    ///
    /// Column-shape mismatches surface as errors (this is exactly the bug
    /// class the smoke probe is designed to trap, so no fallback); empty
    /// rows return an empty vec — callers decide how to interpret no rows.
    pub fn parse_show_stats_response(
        columns: &[String],
        rows: &[Vec<crate::value::Value>],
    ) -> crate::error::NodeDbResult<Vec<Self>> {
        use crate::error::NodeDbError;

        if columns.len() != Self::EXPECTED_COLUMNS.len()
            || columns
                .iter()
                .zip(Self::EXPECTED_COLUMNS.iter())
                .any(|(a, b)| a != b)
        {
            if !columns.is_empty() {
                return Err(NodeDbError::storage(format!(
                    "wire_shape: SHOW GRAPH STATS returned unexpected columns: {columns:?}"
                )));
            }
            // No columns and no rows: treat as empty result.
            return Ok(Vec::new());
        }

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(Self::parse_one_row(row)?);
        }
        Ok(out)
    }

    fn parse_one_row(row: &[crate::value::Value]) -> crate::error::NodeDbResult<Self> {
        use crate::error::NodeDbError;

        let coll_name = row
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                NodeDbError::storage("wire_shape: SHOW GRAPH STATS: missing collection cell")
            })?
            .to_string();
        let node_count = row.get(1).and_then(parse_u64_cell).ok_or_else(|| {
            NodeDbError::storage("wire_shape: SHOW GRAPH STATS: missing node_count")
        })?;
        let edge_count = row.get(2).and_then(parse_u64_cell).ok_or_else(|| {
            NodeDbError::storage("wire_shape: SHOW GRAPH STATS: missing edge_count")
        })?;
        let distinct_label_count = row.get(3).and_then(parse_u64_cell).ok_or_else(|| {
            NodeDbError::storage("wire_shape: SHOW GRAPH STATS: missing distinct_label_count")
        })?;
        let labels_json = row.get(4).and_then(|v| v.as_str()).unwrap_or("[]");
        let parsed: Vec<sonic_rs::Value> = sonic_rs::from_str(labels_json)
            .map_err(|e| NodeDbError::storage(format!("wire_shape: labels JSON parse: {e}")))?;
        let mut labels: Vec<(String, u64)> = Vec::with_capacity(parsed.len());
        for entry in &parsed {
            use sonic_rs::JsonValueTrait;
            let label = entry
                .get("label")
                .and_then(|v| v.as_str())
                .ok_or_else(|| NodeDbError::storage("wire_shape: labels entry missing 'label'"))?
                .to_string();
            let count = entry
                .get("count")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| NodeDbError::storage("wire_shape: labels entry missing 'count'"))?;
            labels.push((label, count));
        }
        Ok(Self {
            collection: coll_name,
            node_count,
            edge_count,
            distinct_label_count,
            labels,
        })
    }
}

/// Parse a count cell that may arrive typed (`Value::Integer`) or as
/// pgwire text (`Value::String`).
fn parse_u64_cell(v: &crate::value::Value) -> Option<u64> {
    match v {
        crate::value::Value::Integer(i) => Some(*i as u64),
        crate::value::Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_parses_case_insensitively_and_names_an_unknown_value() {
        assert_eq!("IN".parse::<Direction>(), Ok(Direction::In));
        assert_eq!("outgoing".parse::<Direction>(), Ok(Direction::Out));
        assert_eq!("Both".parse::<Direction>(), Ok(Direction::Both));
        let err = "sideways".parse::<Direction>().unwrap_err();
        assert_eq!(err.input, "sideways");
        assert!(err.to_string().contains("'sideways'"), "{err}");
    }

    #[test]
    fn direction_roundtrip() {
        for dir in [Direction::Out, Direction::In, Direction::Both] {
            let s = dir.as_str();
            let parsed: Direction = s.parse().unwrap();
            assert_eq!(dir, parsed);
        }
    }

    #[test]
    fn direction_display() {
        assert_eq!(Direction::Out.to_string(), "out");
    }

    #[test]
    fn graph_stats_zero() {
        let s = GraphStats::zero("my_coll");
        assert_eq!(s.collection, "my_coll");
        assert_eq!(s.node_count, 0);
        assert_eq!(s.edge_count, 0);
        assert_eq!(s.distinct_label_count, 0);
        assert!(s.labels.is_empty());
    }

    #[test]
    fn graph_stats_serde_round_trip() {
        let s = GraphStats {
            collection: "coll".into(),
            node_count: 10,
            edge_count: 5,
            distinct_label_count: 2,
            labels: vec![("KNOWS".into(), 3), ("OWNS".into(), 2)],
        };
        let json = sonic_rs::to_string(&s).unwrap();
        let back: GraphStats = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn parse_show_stats_multi_row() {
        use crate::value::Value;
        let columns: Vec<String> = GraphStats::EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let labels_json = r#"[{"label":"KNOWS","count":3},{"label":"OWNS","count":2}]"#;
        let rows = vec![
            vec![
                Value::String("social".into()),
                // pgwire simple-query text protocol arrives as strings;
                // the native protocol arrives as integers — both shapes are accepted.
                Value::String("10".into()),
                Value::Integer(5),
                Value::String("2".into()),
                Value::String(labels_json.into()),
            ],
            vec![
                Value::String("comms".into()),
                Value::Integer(3),
                Value::Integer(2),
                Value::Integer(1),
                Value::String(r#"[{"label":"CALLS","count":2}]"#.into()),
            ],
        ];
        let result = GraphStats::parse_show_stats_response(&columns, &rows).unwrap();
        assert_eq!(result.len(), 2);
        let social = &result[0];
        assert_eq!(social.collection, "social");
        assert_eq!(social.node_count, 10);
        assert_eq!(social.edge_count, 5);
        assert_eq!(social.distinct_label_count, 2);
        assert_eq!(social.labels, vec![("KNOWS".into(), 3), ("OWNS".into(), 2)]);
        let comms = &result[1];
        assert_eq!(comms.collection, "comms");
        assert_eq!(comms.edge_count, 2);
        assert_eq!(comms.labels, vec![("CALLS".into(), 2)]);
    }

    #[test]
    fn parse_show_stats_empty_rows_returns_empty_vec() {
        let columns: Vec<String> = GraphStats::EXPECTED_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let result = GraphStats::parse_show_stats_response(&columns, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_show_stats_wrong_columns_errors() {
        let columns = vec!["id".to_string(), "count".to_string()];
        let err = GraphStats::parse_show_stats_response(&columns, &[]).unwrap_err();
        assert!(
            err.to_string().contains("unexpected columns"),
            "error should mention unexpected columns: {err}"
        );
    }

    #[test]
    fn parse_show_stats_no_columns_no_rows_returns_empty_vec() {
        let result = GraphStats::parse_show_stats_response(&[], &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn graph_stats_msgpack_round_trip() {
        let s = GraphStats {
            collection: "coll".into(),
            node_count: 7,
            edge_count: 3,
            distinct_label_count: 1,
            labels: vec![("FOLLOWS".into(), 3)],
        };
        let bytes = zerompk::to_msgpack_vec(&s).unwrap();
        let back: GraphStats = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, s);
    }
}

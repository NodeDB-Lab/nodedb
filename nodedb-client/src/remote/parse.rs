// SPDX-License-Identifier: Apache-2.0

//! JSON response parsing for the remote client.
//!
//! The `SEARCH ... USING VECTOR` DSL returns its results as JSON in a single
//! text column. This helper decodes that into typed `SearchResult` values
//! for the trait surface. Graph traversal and path results decode through
//! the shared `crate::graph_dsl` decoders. Row-shaped responses (system
//! catalog tables) go through [`crate::row_decode`] instead so the remote
//! and trait-default decoders share one parser.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::result::SearchResult;

/// Parse a JSON string from the DSL's "result" column into `Vec<SearchResult>`.
pub(super) fn parse_vector_search_json(json_text: &str) -> NodeDbResult<Vec<SearchResult>> {
    let parsed: serde_json::Value = sonic_rs::from_str(json_text)
        .map_err(|e| NodeDbError::serialization("json", e.to_string()))?;

    let mut results = Vec::new();
    if let Some(arr) = parsed.as_array() {
        for item in arr {
            let id = item
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let distance = item.get("distance").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            results.push(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            });
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vector_search_json_works() {
        let json = r#"[{"id":"v1","distance":0.1},{"id":"v2","distance":0.5}]"#;
        let results = parse_vector_search_json(json).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1");
        assert!((results[0].distance - 0.1).abs() < 0.001);
        assert_eq!(results[1].id, "v2");
    }

    #[test]
    fn parse_empty_search_json() {
        let results = parse_vector_search_json("[]").unwrap();
        assert!(results.is_empty());
    }
}

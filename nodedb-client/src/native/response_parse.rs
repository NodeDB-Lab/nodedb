// SPDX-License-Identifier: Apache-2.0

//! Response parsing helpers for native protocol results.

use std::collections::HashMap;

use nodedb_types::error::NodeDbResult;
use nodedb_types::result::SearchResult;

/// Parse search results from a native response.
pub(crate) fn parse_search_results(
    resp: &nodedb_types::protocol::NativeResponse,
) -> NodeDbResult<Vec<SearchResult>> {
    let rows = match &resp.rows {
        Some(r) => r,
        None => return Ok(Vec::new()),
    };

    let mut results = Vec::new();
    for row in rows {
        if let Some(text) = row.first().and_then(|v| v.as_str()) {
            if let Ok(items) = sonic_rs::from_str::<Vec<serde_json::Value>>(text) {
                for item in items {
                    if let Some(sr) = parse_single_search_result(&item) {
                        results.push(sr);
                    }
                }
            } else if let Ok(item) = sonic_rs::from_str::<serde_json::Value>(text)
                && let Some(sr) = parse_single_search_result(&item)
            {
                results.push(sr);
            }
        }
    }
    Ok(results)
}

fn parse_single_search_result(v: &serde_json::Value) -> Option<SearchResult> {
    let id = v.get("id")?.as_str()?.to_string();
    let distance = v.get("distance")?.as_f64()? as f32;
    Some(SearchResult {
        id,
        node_id: None,
        distance,
        metadata: HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_search_result_from_json() {
        let v = serde_json::json!({"id": "vec-1", "distance": 0.123});
        let sr =
            parse_single_search_result(&v).expect("failed to parse search result from valid JSON");
        assert_eq!(sr.id, "vec-1");
        assert!((sr.distance - 0.123).abs() < 0.001);
    }
}

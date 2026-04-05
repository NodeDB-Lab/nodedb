//! Post-scan filter evaluation.
//!
//! `ScanFilter` represents a single filter predicate. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.
//!
//! Shared between Origin (Control Plane + Data Plane) and Lite.

pub mod aggregate;
pub mod like;
pub mod parse;

pub use aggregate::compute_aggregate;
pub use like::sql_like_match;
pub use parse::parse_simple_predicates;

use crate::json_ops::{coerced_eq, compare_json_optional as compare_json_values};

/// A single filter predicate for document scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null) and disjunctive groups via the `"or"` operator.
///
/// OR representation: `{"op": "or", "clauses": [[filter1, filter2], [filter3]]}`
/// means `(filter1 AND filter2) OR filter3`. Each clause is an AND-group;
/// the document matches if ANY clause group fully matches.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanFilter {
    #[serde(default)]
    pub field: String,
    pub op: String,
    #[serde(default)]
    pub value: serde_json::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        if self.op == "match_all" {
            return true;
        }

        if self.op == "exists" || self.op == "not_exists" {
            return true;
        }

        if self.op == "or" {
            return self
                .clauses
                .iter()
                .any(|clause| clause.iter().all(|f| f.matches(doc)));
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == "is_null",
        };

        match self.op.as_str() {
            "eq" => coerced_eq(field_val, &self.value),
            "ne" | "neq" => !coerced_eq(field_val, &self.value),
            "gt" => {
                compare_json_values(Some(field_val), Some(&self.value))
                    == std::cmp::Ordering::Greater
            }
            "gte" | "ge" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            "lt" => {
                compare_json_values(Some(field_val), Some(&self.value)) == std::cmp::Ordering::Less
            }
            "lte" | "le" => {
                let cmp = compare_json_values(Some(field_val), Some(&self.value));
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            "contains" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            "like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "not_like" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            "ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "not_ilike" => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            "in" => {
                if let Some(arr) = self.value.as_array() {
                    arr.iter().any(|v| field_val == v)
                } else {
                    false
                }
            }
            "not_in" => {
                if let Some(arr) = self.value.as_array() {
                    !arr.iter().any(|v| field_val == v)
                } else {
                    true
                }
            }
            "is_null" => field_val.is_null(),
            "is_not_null" => !field_val.is_null(),

            // ── Array operators ──
            // field is an array, value is a scalar: true if array contains the value.
            "array_contains" => {
                if let Some(arr) = field_val.as_array() {
                    arr.iter().any(|v| coerced_eq(v, &self.value))
                } else {
                    false
                }
            }
            // field is an array, value is an array: true if field contains ALL values.
            "array_contains_all" => {
                if let (Some(field_arr), Some(needle_arr)) =
                    (field_val.as_array(), self.value.as_array())
                {
                    needle_arr
                        .iter()
                        .all(|needle| field_arr.iter().any(|v| coerced_eq(v, needle)))
                } else {
                    false
                }
            }
            // field is an array, value is an array: true if any element is shared.
            "array_overlap" => {
                if let (Some(field_arr), Some(needle_arr)) =
                    (field_val.as_array(), self.value.as_array())
                {
                    needle_arr
                        .iter()
                        .any(|needle| field_arr.iter().any(|v| coerced_eq(v, needle)))
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filter_eq_coercion() {
        let doc = json!({"age": 25});
        let filter = ScanFilter {
            field: "age".into(),
            op: "eq".into(),
            value: json!("25"),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn filter_gt_coercion() {
        let doc = json!({"score": "90"});
        let filter = ScanFilter {
            field: "score".into(),
            op: "gt".into(),
            value: json!(80),
            clauses: vec![],
        };
        assert!(filter.matches(&doc));
    }

    #[test]
    fn like_basic() {
        assert!(sql_like_match("hello world", "%world", false));
        assert!(sql_like_match("hello world", "hello%", false));
        assert!(!sql_like_match("hello world", "xyz%", false));
    }

    #[test]
    fn ilike_case_insensitive() {
        assert!(sql_like_match("Hello", "hello", true));
        assert!(sql_like_match("WORLD", "%world%", true));
    }

    #[test]
    fn aggregate_count() {
        let docs = vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})];
        assert_eq!(compute_aggregate("count", "x", &docs), json!(3));
    }

    #[test]
    fn aggregate_sum() {
        let docs = vec![json!({"v": 10}), json!({"v": 20}), json!({"v": 30})];
        assert_eq!(compute_aggregate("sum", "v", &docs), json!(60.0));
    }

    #[test]
    fn aggregate_min_max() {
        let docs = vec![json!({"v": 5}), json!({"v": 1}), json!({"v": 9})];
        assert_eq!(compute_aggregate("min", "v", &docs), json!(1));
        assert_eq!(compute_aggregate("max", "v", &docs), json!(9));
    }
}

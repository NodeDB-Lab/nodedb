//! Post-scan filter evaluation.
//!
//! `ScanFilter` represents a single filter predicate. `compare_json_values`
//! provides total ordering for JSON values used in sort and range comparisons.
//!
//! Shared between Origin (Control Plane + Data Plane) and Lite.

pub mod like;
pub mod parse;

pub use like::sql_like_match;
pub use parse::parse_simple_predicates;

/// Filter operator enum for O(1) dispatch instead of string comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    Like,
    NotLike,
    Ilike,
    NotIlike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    ArrayContains,
    ArrayContainsAll,
    ArrayOverlap,
    #[default]
    MatchAll,
    Exists,
    NotExists,
    Or,
    /// Column-vs-column comparison: `field` op `value` where `value` is a
    /// `Value::String` containing the name of the other column. The comparison
    /// reads both fields from the same document row.
    GtColumn,
    GteColumn,
    LtColumn,
    LteColumn,
    EqColumn,
    NeColumn,
}

impl FilterOp {
    pub fn parse_op(s: &str) -> Self {
        match s {
            "eq" => Self::Eq,
            "ne" | "neq" => Self::Ne,
            "gt" => Self::Gt,
            "gte" | "ge" => Self::Gte,
            "lt" => Self::Lt,
            "lte" | "le" => Self::Lte,
            "contains" => Self::Contains,
            "like" => Self::Like,
            "not_like" => Self::NotLike,
            "ilike" => Self::Ilike,
            "not_ilike" => Self::NotIlike,
            "in" => Self::In,
            "not_in" => Self::NotIn,
            "is_null" => Self::IsNull,
            "is_not_null" => Self::IsNotNull,
            "array_contains" => Self::ArrayContains,
            "array_contains_all" => Self::ArrayContainsAll,
            "array_overlap" => Self::ArrayOverlap,
            "match_all" => Self::MatchAll,
            "exists" => Self::Exists,
            "not_exists" => Self::NotExists,
            "or" => Self::Or,
            "gt_col" => Self::GtColumn,
            "gte_col" => Self::GteColumn,
            "lt_col" => Self::LtColumn,
            "lte_col" => Self::LteColumn,
            "eq_col" => Self::EqColumn,
            "ne_col" => Self::NeColumn,
            _ => Self::MatchAll,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Contains => "contains",
            Self::Like => "like",
            Self::NotLike => "not_like",
            Self::Ilike => "ilike",
            Self::NotIlike => "not_ilike",
            Self::In => "in",
            Self::NotIn => "not_in",
            Self::IsNull => "is_null",
            Self::IsNotNull => "is_not_null",
            Self::ArrayContains => "array_contains",
            Self::ArrayContainsAll => "array_contains_all",
            Self::ArrayOverlap => "array_overlap",
            Self::MatchAll => "match_all",
            Self::Exists => "exists",
            Self::NotExists => "not_exists",
            Self::Or => "or",
            Self::GtColumn => "gt_col",
            Self::GteColumn => "gte_col",
            Self::LtColumn => "lt_col",
            Self::LteColumn => "lte_col",
            Self::EqColumn => "eq_col",
            Self::NeColumn => "ne_col",
        }
    }
}

impl From<&str> for FilterOp {
    fn from(s: &str) -> Self {
        Self::parse_op(s)
    }
}

impl From<String> for FilterOp {
    fn from(s: String) -> Self {
        Self::parse_op(&s)
    }
}

impl serde::Serialize for FilterOp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for FilterOp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(FilterOp::parse_op(&s))
    }
}

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
    pub op: FilterOp,
    #[serde(default)]
    pub value: nodedb_types::Value,
    /// Disjunctive clause groups for OR predicates.
    /// Each inner Vec is an AND-group. The document matches if ANY group matches.
    #[serde(default)]
    pub clauses: Vec<Vec<ScanFilter>>,
}

impl zerompk::ToMessagePack for ScanFilter {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(4)?;
        self.field.write(writer)?;
        writer.write_string(self.op.as_str())?;
        // Convert nodedb_types::Value → serde_json::Value for wire compat.
        let json_val: serde_json::Value = self.value.clone().into();
        nodedb_types::JsonValue(json_val).write(writer)?;
        self.clauses.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for ScanFilter {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(4)?;
        let field = String::read(reader)?;
        let op_str = String::read(reader)?;
        let jv = nodedb_types::JsonValue::read(reader)?;
        let clauses = Vec::<Vec<ScanFilter>>::read(reader)?;
        Ok(Self {
            field,
            op: FilterOp::parse_op(&op_str),
            // Convert serde_json::Value → nodedb_types::Value at wire boundary.
            value: nodedb_types::Value::from(jv.0),
            clauses,
        })
    }
}

impl ScanFilter {
    /// Evaluate this filter against a JSON document.
    ///
    /// Uses `FilterOp` enum for O(1) dispatch instead of string comparison.
    pub fn matches(&self, doc: &serde_json::Value) -> bool {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return true,
            FilterOp::Or => {
                return self
                    .clauses
                    .iter()
                    .any(|clause| clause.iter().all(|f| f.matches(doc)));
            }
            _ => {}
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == FilterOp::IsNull,
        };

        match self.op {
            FilterOp::Eq => self.value.eq_json(field_val),
            FilterOp::Ne => !self.value.eq_json(field_val),
            FilterOp::Gt => self.value.cmp_json(field_val) == std::cmp::Ordering::Less,
            FilterOp::Gte => {
                let cmp = self.value.cmp_json(field_val);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Lt => self.value.cmp_json(field_val) == std::cmp::Ordering::Greater,
            FilterOp::Lte => {
                let cmp = self.value.cmp_json(field_val);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Contains => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::NotLike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::Ilike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::NotIlike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::In => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    iter.any(|v| v.eq_json(field_val))
                } else {
                    false
                }
            }
            FilterOp::NotIn => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    !iter.any(|v| v.eq_json(field_val))
                } else {
                    true
                }
            }
            FilterOp::IsNull => field_val.is_null(),
            FilterOp::IsNotNull => !field_val.is_null(),
            FilterOp::ArrayContains => {
                if let Some(arr) = field_val.as_array() {
                    arr.iter().any(|v| self.value.eq_json(v))
                } else {
                    false
                }
            }
            FilterOp::ArrayContainsAll => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.all(|needle| field_arr.iter().any(|v| needle.eq_json(v)))
                } else {
                    false
                }
            }
            FilterOp::ArrayOverlap => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.any(|needle| field_arr.iter().any(|v| needle.eq_json(v)))
                } else {
                    false
                }
            }
            // Column-vs-column comparisons: self.value is String(other_col_name).
            FilterOp::GtColumn
            | FilterOp::GteColumn
            | FilterOp::LtColumn
            | FilterOp::LteColumn
            | FilterOp::EqColumn
            | FilterOp::NeColumn => {
                let other_col = match &self.value {
                    nodedb_types::Value::String(s) => s.as_str(),
                    _ => return false,
                };
                let other_val = match doc.get(other_col) {
                    Some(v) => v,
                    None => return false,
                };
                let left = nodedb_types::Value::from(field_val.clone());
                let right = nodedb_types::Value::from(other_val.clone());
                match self.op {
                    FilterOp::GtColumn => left.cmp_coerced(&right) == std::cmp::Ordering::Greater,
                    FilterOp::GteColumn => left.cmp_coerced(&right) != std::cmp::Ordering::Less,
                    FilterOp::LtColumn => left.cmp_coerced(&right) == std::cmp::Ordering::Less,
                    FilterOp::LteColumn => left.cmp_coerced(&right) != std::cmp::Ordering::Greater,
                    FilterOp::EqColumn => left.eq_coerced(&right),
                    FilterOp::NeColumn => !left.eq_coerced(&right),
                    _ => false,
                }
            }
            // MatchAll/Exists/NotExists/Or handled above.
            _ => false,
        }
    }

    /// Evaluate this filter against a `nodedb_types::Value` document.
    ///
    /// Same semantics as `matches()` but operates on the native Value type
    /// instead of serde_json::Value, avoiding lossy JSON roundtrips.
    pub fn matches_value(&self, doc: &nodedb_types::Value) -> bool {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return true,
            FilterOp::Or => {
                return self
                    .clauses
                    .iter()
                    .any(|clause| clause.iter().all(|f| f.matches_value(doc)));
            }
            _ => {}
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return self.op == FilterOp::IsNull,
        };

        match self.op {
            FilterOp::Eq => self.value.eq_coerced(field_val),
            FilterOp::Ne => !self.value.eq_coerced(field_val),
            FilterOp::Gt => self.value.cmp_coerced(field_val) == std::cmp::Ordering::Less,
            FilterOp::Gte => {
                let cmp = self.value.cmp_coerced(field_val);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Lt => self.value.cmp_coerced(field_val) == std::cmp::Ordering::Greater,
            FilterOp::Lte => {
                let cmp = self.value.cmp_coerced(field_val);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            FilterOp::Contains => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    s.contains(pattern)
                } else {
                    false
                }
            }
            FilterOp::Like => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::NotLike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, false)
                } else {
                    false
                }
            }
            FilterOp::Ilike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::NotIlike => {
                if let (Some(s), Some(pattern)) = (field_val.as_str(), self.value.as_str()) {
                    !like::sql_like_match(s, pattern, true)
                } else {
                    false
                }
            }
            FilterOp::In => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    iter.any(|v| v.eq_coerced(field_val))
                } else {
                    false
                }
            }
            FilterOp::NotIn => {
                if let Some(mut iter) = self.value.as_array_iter() {
                    !iter.any(|v| v.eq_coerced(field_val))
                } else {
                    true
                }
            }
            FilterOp::IsNull => field_val.is_null(),
            FilterOp::IsNotNull => !field_val.is_null(),
            FilterOp::ArrayContains => {
                if let Some(arr) = field_val.as_array() {
                    arr.iter().any(|v| self.value.eq_coerced(v))
                } else {
                    false
                }
            }
            FilterOp::ArrayContainsAll => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.all(|needle| field_arr.iter().any(|v| needle.eq_coerced(v)))
                } else {
                    false
                }
            }
            FilterOp::ArrayOverlap => {
                if let (Some(field_arr), Some(mut needles)) =
                    (field_val.as_array(), self.value.as_array_iter())
                {
                    needles.any(|needle| field_arr.iter().any(|v| needle.eq_coerced(v)))
                } else {
                    false
                }
            }
            FilterOp::GtColumn
            | FilterOp::GteColumn
            | FilterOp::LtColumn
            | FilterOp::LteColumn
            | FilterOp::EqColumn
            | FilterOp::NeColumn => {
                let other_col = match &self.value {
                    nodedb_types::Value::String(s) => s.as_str(),
                    _ => return false,
                };
                let other_val = match doc.get(other_col) {
                    Some(v) => v,
                    None => return false,
                };
                match self.op {
                    FilterOp::GtColumn => {
                        field_val.cmp_coerced(other_val) == std::cmp::Ordering::Greater
                    }
                    FilterOp::GteColumn => {
                        field_val.cmp_coerced(other_val) != std::cmp::Ordering::Less
                    }
                    FilterOp::LtColumn => {
                        field_val.cmp_coerced(other_val) == std::cmp::Ordering::Less
                    }
                    FilterOp::LteColumn => {
                        field_val.cmp_coerced(other_val) != std::cmp::Ordering::Greater
                    }
                    FilterOp::EqColumn => field_val.eq_coerced(other_val),
                    FilterOp::NeColumn => !field_val.eq_coerced(other_val),
                    _ => false,
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
            value: nodedb_types::Value::String("25".into()),
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
            value: nodedb_types::Value::Integer(80),
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
}

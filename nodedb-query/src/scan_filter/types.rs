//! `ScanFilter` record, its wire codec, and per-row evaluation against a
//! `nodedb_types::Value` document.

use crate::expr::SqlExpr;

use super::like;
use super::op::FilterOp;

/// A single filter predicate for document scan evaluation.
///
/// Supports simple comparison operators (eq, ne, gt, gte, lt, lte, contains,
/// is_null, is_not_null), disjunctive groups via the `"or"` operator, and
/// full SqlExpr predicates via `FilterOp::Expr` for anything the planner
/// cannot reduce to a simple `(field, op, value)` — scalar functions in
/// WHERE, non-literal IN lists, column arithmetic, `NOT(...)`, etc.
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
    /// Expression predicate payload. Only meaningful when `op == FilterOp::Expr`;
    /// must be `None` for every other operator.
    #[serde(default)]
    pub expr: Option<SqlExpr>,
}

impl zerompk::ToMessagePack for ScanFilter {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(5)?;
        self.field.write(writer)?;
        writer.write_string(self.op.as_str())?;
        // Convert nodedb_types::Value → serde_json::Value for wire compat.
        let json_val: serde_json::Value = self.value.clone().into();
        nodedb_types::JsonValue(json_val).write(writer)?;
        self.clauses.write(writer)?;
        self.expr.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for ScanFilter {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(5)?;
        let field = String::read(reader)?;
        let op_str = String::read(reader)?;
        let jv = nodedb_types::JsonValue::read(reader)?;
        let clauses = Vec::<Vec<ScanFilter>>::read(reader)?;
        let expr = Option::<SqlExpr>::read(reader)?;
        Ok(Self {
            field,
            op: FilterOp::parse_op(&op_str),
            // Convert serde_json::Value → nodedb_types::Value at wire boundary.
            value: nodedb_types::Value::from(jv.0),
            clauses,
            expr,
        })
    }
}

impl ScanFilter {
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
            FilterOp::Expr => {
                return match &self.expr {
                    Some(expr) => crate::value_ops::is_truthy(&expr.eval(doc)),
                    None => false,
                };
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

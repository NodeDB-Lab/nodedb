// SPDX-License-Identifier: Apache-2.0

//! `ScanFilter` record, its wire codec, and per-row evaluation against a
//! `nodedb_types::Value` document.

use crate::expr::{EvalError, SqlExpr};

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
///
/// Wire form (zerompk): `[field, op_name, value, clauses, expr]`. `op_name`
/// is the `FilterOp::as_str` tag and decodes through `FilterOp::parse`, so
/// an unknown name is a decode error. `value` is the tagged
/// `nodedb_types::Value` encoding, the same form `SqlExpr::Literal` uses:
/// every variant, including `DateTime` / `NaiveDateTime` / `Bytes` /
/// `Decimal`, arrives typed. The serde derive is a JSON rendering; it never
/// crosses the bridge.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        self.value.write(writer)?;
        self.clauses.write(writer)?;
        self.expr.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for ScanFilter {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(5)?;
        let field = String::read(reader)?;
        let op = FilterOp::parse(&reader.read_string()?)?;
        let value = nodedb_types::Value::read(reader)?;
        let clauses = Vec::<Vec<ScanFilter>>::read(reader)?;
        let expr = Option::<SqlExpr>::read(reader)?;
        Ok(Self {
            field,
            op,
            value,
            clauses,
            expr,
        })
    }
}

impl ScanFilter {
    /// Evaluate an AND-group of filters, short-circuiting on the first
    /// `false` or the first evaluation error — same semantics as
    /// `group.iter().all(|f| f.matches_value(doc))` had before filter
    /// evaluation could fail. `pub` so the many call
    /// sites across the `nodedb` crate that would otherwise write
    /// `filters.iter().all(|f| f.matches_value(doc))` have a drop-in
    /// replacement instead of each hand-rolling the same short-circuit loop.
    pub fn all_match_value(
        group: &[ScanFilter],
        doc: &nodedb_types::Value,
    ) -> Result<bool, EvalError> {
        for f in group {
            if !f.matches_value(doc)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Evaluate this filter against a `nodedb_types::Value` document.
    ///
    /// Same semantics as `matches()` but operates on the native Value type
    /// instead of serde_json::Value, avoiding lossy JSON roundtrips.
    ///
    /// Returns `Err(EvalError::DivisionByZero)` when the filter is (or
    /// contains, via an `OR` group) a `FilterOp::Expr` predicate whose
    /// expression divides or takes a modulus by zero.
    /// This is the deliberate WHERE-clause behavior: a predicate like
    /// `10 / denom > 1` must fail the whole query with SQLSTATE `22012`,
    /// matching Postgres, rather than evaluate to `Value::Null`/`false`
    /// (silently filtering the row out) when `denom` is `0`.
    pub fn matches_value(&self, doc: &nodedb_types::Value) -> Result<bool, EvalError> {
        match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => return Ok(true),
            FilterOp::Or => {
                for clause in &self.clauses {
                    if Self::all_match_value(clause, doc)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            FilterOp::Expr => {
                return match &self.expr {
                    Some(expr) => Ok(crate::value_ops::is_truthy(&expr.eval(doc)?)),
                    None => Ok(false),
                };
            }
            FilterOp::Eq
            | FilterOp::Ne
            | FilterOp::Gt
            | FilterOp::Gte
            | FilterOp::Lt
            | FilterOp::Lte
            | FilterOp::Contains
            | FilterOp::Like
            | FilterOp::NotLike
            | FilterOp::Ilike
            | FilterOp::NotIlike
            | FilterOp::In
            | FilterOp::NotIn
            | FilterOp::IsNull
            | FilterOp::IsNotNull
            | FilterOp::ArrayContains
            | FilterOp::ArrayContainsAll
            | FilterOp::ArrayOverlap
            | FilterOp::GtColumn
            | FilterOp::GteColumn
            | FilterOp::LtColumn
            | FilterOp::LteColumn
            | FilterOp::EqColumn
            | FilterOp::NeColumn => {}
        }

        let field_val = match doc.get(&self.field) {
            Some(v) => v,
            None => return Ok(self.op == FilterOp::IsNull),
        };

        Ok(match self.op {
            FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => true,
            FilterOp::Or | FilterOp::Expr => false,
            FilterOp::Eq => self.value.eq_coerced(field_val),
            FilterOp::Ne => !self.value.eq_coerced(field_val),
            FilterOp::Gt => {
                field_val.partial_cmp_coerced(&self.value) == Some(std::cmp::Ordering::Greater)
            }
            FilterOp::Gte => matches!(
                field_val.partial_cmp_coerced(&self.value),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            FilterOp::Lt => {
                field_val.partial_cmp_coerced(&self.value) == Some(std::cmp::Ordering::Less)
            }
            FilterOp::Lte => matches!(
                field_val.partial_cmp_coerced(&self.value),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
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
                    _ => return Ok(false),
                };
                let other_val = match doc.get(other_col) {
                    Some(v) => v,
                    None => return Ok(false),
                };
                column_compare(self.op, field_val, other_val)
            }
        })
    }
}

/// Evaluate a column-vs-column operator over two cells of the same row.
/// Any other operator has no column form and matches nothing.
pub(crate) fn column_compare(
    op: FilterOp,
    left: &nodedb_types::Value,
    right: &nodedb_types::Value,
) -> bool {
    use std::cmp::Ordering;
    match op {
        FilterOp::GtColumn => left.partial_cmp_coerced(right) == Some(Ordering::Greater),
        FilterOp::GteColumn => matches!(
            left.partial_cmp_coerced(right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        FilterOp::LtColumn => left.partial_cmp_coerced(right) == Some(Ordering::Less),
        FilterOp::LteColumn => matches!(
            left.partial_cmp_coerced(right),
            Some(Ordering::Less | Ordering::Equal)
        ),
        FilterOp::EqColumn => left.eq_coerced(right),
        FilterOp::NeColumn => !left.eq_coerced(right),
        FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte
        | FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::Ilike
        | FilterOp::NotIlike
        | FilterOp::In
        | FilterOp::NotIn
        | FilterOp::IsNull
        | FilterOp::IsNotNull
        | FilterOp::ArrayContains
        | FilterOp::ArrayContainsAll
        | FilterOp::ArrayOverlap
        | FilterOp::MatchAll
        | FilterOp::Exists
        | FilterOp::NotExists
        | FilterOp::Or
        | FilterOp::Expr => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use nodedb_types::datetime::NdbDateTime;

    fn round_trip(filter: &ScanFilter) -> ScanFilter {
        let bytes = zerompk::to_msgpack_vec(filter).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    fn compare(op: FilterOp, value: Value) -> ScanFilter {
        ScanFilter {
            field: "captured_at".into(),
            op,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    #[test]
    fn naive_instant_crosses_the_bridge_typed() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let back = round_trip(&compare(FilterOp::Gte, Value::NaiveDateTime(at)));
        assert_eq!(back.op, FilterOp::Gte);
        assert_eq!(back.field, "captured_at");
        assert_eq!(back.value, Value::NaiveDateTime(at));
    }

    #[test]
    fn utc_instant_crosses_the_bridge_typed() {
        let at = NdbDateTime::from_micros(-86_400_000_000);
        let back = round_trip(&compare(FilterOp::Lt, Value::DateTime(at)));
        assert_eq!(back.value, Value::DateTime(at));
    }

    #[test]
    fn bytes_cross_the_bridge_as_bytes() {
        let back = round_trip(&compare(FilterOp::Eq, Value::Bytes(vec![0, 159, 146, 150])));
        assert_eq!(back.value, Value::Bytes(vec![0, 159, 146, 150]));
    }

    #[test]
    fn nested_clauses_keep_typed_values() {
        let at = NdbDateTime::from_micros(1_000_000);
        let filter = ScanFilter {
            field: String::new(),
            op: FilterOp::Or,
            value: Value::Null,
            clauses: vec![
                vec![compare(FilterOp::Gt, Value::NaiveDateTime(at))],
                vec![compare(
                    FilterOp::In,
                    Value::Array(vec![Value::DateTime(at)]),
                )],
            ],
            expr: None,
        };
        let back = round_trip(&filter);
        assert_eq!(back.op, FilterOp::Or);
        assert_eq!(back.clauses[0][0].value, Value::NaiveDateTime(at));
        assert_eq!(
            back.clauses[1][0].value,
            Value::Array(vec![Value::DateTime(at)])
        );
    }

    #[test]
    fn every_operator_name_round_trips_through_the_wire() {
        for name in [
            "eq",
            "ne",
            "gt",
            "gte",
            "lt",
            "lte",
            "contains",
            "like",
            "not_like",
            "ilike",
            "not_ilike",
            "in",
            "not_in",
            "is_null",
            "is_not_null",
            "array_contains",
            "array_contains_all",
            "array_overlap",
            "match_all",
            "exists",
            "not_exists",
            "or",
            "expr",
            "gt_col",
            "gte_col",
            "lt_col",
            "lte_col",
            "eq_col",
            "ne_col",
        ] {
            let op = FilterOp::parse(name).expect(name);
            let back = round_trip(&compare(op, Value::Null));
            assert_eq!(back.op, op, "{name}");
            assert_eq!(back.op.as_str(), name);
        }
    }

    /// Msgpack `fixarray` header for `len` elements (`len < 16`).
    fn fixarray(len: u8) -> u8 {
        0x90 | len
    }

    /// The wire bytes of a filter carrying operator name `op`, built from
    /// the same fragments `ScanFilter::write` emits so the layout matches.
    fn wire_with_op(op: &str) -> Vec<u8> {
        let mut buf = vec![fixarray(5)];
        buf.extend(zerompk::to_msgpack_vec(&"tags".to_string()).expect("field"));
        buf.extend(zerompk::to_msgpack_vec(&op.to_string()).expect("op"));
        buf.extend(zerompk::to_msgpack_vec(&Value::Null).expect("value"));
        buf.push(fixarray(0));
        buf.extend(zerompk::to_msgpack_vec(&None::<SqlExpr>).expect("expr"));
        buf
    }

    #[test]
    fn unknown_operator_fails_the_decode_naming_the_operator() {
        let err =
            zerompk::from_msgpack::<ScanFilter>(&wire_with_op("any_in")).expect_err("unknown");
        let text = err.to_string();
        assert!(text.contains("any_in"), "{text}");
        assert!(text.contains("unknown filter operator"), "{text}");

        let ok: ScanFilter = zerompk::from_msgpack(&wire_with_op("array_overlap")).expect("known");
        assert_eq!(ok.op, FilterOp::ArrayOverlap);
    }

    #[test]
    fn unknown_operator_inside_a_clause_fails_the_whole_set() {
        let mut buf = vec![fixarray(1), fixarray(5)];
        buf.extend(zerompk::to_msgpack_vec(&String::new()).expect("field"));
        buf.extend(zerompk::to_msgpack_vec(&"or".to_string()).expect("op"));
        buf.extend(zerompk::to_msgpack_vec(&Value::Null).expect("value"));
        buf.push(fixarray(1));
        buf.push(fixarray(1));
        buf.extend_from_slice(&wire_with_op("any_in"));
        buf.extend(zerompk::to_msgpack_vec(&None::<SqlExpr>).expect("expr"));
        let err = zerompk::from_msgpack::<Vec<ScanFilter>>(&buf).expect_err("unknown");
        assert!(err.to_string().contains("any_in"), "{err}");
    }

    /// The hand-built bytes decode when the operator is known, which pins
    /// the layout the malformed cases rely on.
    #[test]
    fn hand_built_wire_matches_the_codec_layout() {
        let filter = ScanFilter {
            field: "tags".into(),
            op: FilterOp::Eq,
            value: Value::Null,
            clauses: Vec::new(),
            expr: None,
        };
        let encoded = zerompk::to_msgpack_vec(&filter).expect("encode");
        assert_eq!(encoded, wire_with_op("eq"));
    }
}

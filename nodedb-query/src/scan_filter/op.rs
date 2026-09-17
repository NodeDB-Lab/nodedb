// SPDX-License-Identifier: Apache-2.0

//! `FilterOp` enum and its string/serde conversions.
//!
//! `FilterOp` is an O(1)-dispatch discriminant used by the scan filter
//! evaluator. On-wire it travels as a lowercase string tag so physical
//! plans remain debuggable by hand. Decoding a tag is fallible: a name
//! `FilterOp::parse` does not know is an error, never a default operator.

/// An operator name no `FilterOp` variant carries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown filter operator `{op}`")]
pub struct UnknownFilterOp {
    /// The operator text as it arrived.
    pub op: String,
}

impl From<UnknownFilterOp> for zerompk::Error {
    fn from(e: UnknownFilterOp) -> Self {
        zerompk::Error::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

/// Filter operator enum for O(1) dispatch instead of string comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Every row passes. Emitted by policy lowering for a predicate decided
    /// true at plan time and by the planner for a filter it cannot lower.
    MatchAll,
    Exists,
    NotExists,
    Or,
    /// Arbitrary expression predicate: the filter's `expr` field holds a
    /// `nodedb_query::expr::SqlExpr`. The scan evaluator runs the expression
    /// against the full row and treats truthy results as a match. Used when
    /// the planner cannot reduce the WHERE clause to a simple `(field, op, value)`
    /// — e.g. `LOWER(col) = 'x'`, `qty + 1 = 5`, `NOT (col = 'x')`.
    Expr,
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
    /// Parse a wire operator name. Every name `as_str` emits parses back to
    /// the same variant. `ne`/`neq`, `gte`/`ge`, and `lte`/`le` are aliases.
    pub fn parse(name: &str) -> Result<Self, UnknownFilterOp> {
        Ok(match name {
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
            "expr" => Self::Expr,
            "gt_col" => Self::GtColumn,
            "gte_col" => Self::GteColumn,
            "lt_col" => Self::LtColumn,
            "lte_col" => Self::LteColumn,
            "eq_col" => Self::EqColumn,
            "ne_col" => Self::NeColumn,
            other => {
                return Err(UnknownFilterOp {
                    op: other.to_string(),
                });
            }
        })
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
            Self::Expr => "expr",
            Self::GtColumn => "gt_col",
            Self::GteColumn => "gte_col",
            Self::LtColumn => "lt_col",
            Self::LteColumn => "lte_col",
            Self::EqColumn => "eq_col",
            Self::NeColumn => "ne_col",
        }
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
        FilterOp::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [FilterOp; 29] = [
        FilterOp::Eq,
        FilterOp::Ne,
        FilterOp::Gt,
        FilterOp::Gte,
        FilterOp::Lt,
        FilterOp::Lte,
        FilterOp::Contains,
        FilterOp::Like,
        FilterOp::NotLike,
        FilterOp::Ilike,
        FilterOp::NotIlike,
        FilterOp::In,
        FilterOp::NotIn,
        FilterOp::IsNull,
        FilterOp::IsNotNull,
        FilterOp::ArrayContains,
        FilterOp::ArrayContainsAll,
        FilterOp::ArrayOverlap,
        FilterOp::MatchAll,
        FilterOp::Exists,
        FilterOp::NotExists,
        FilterOp::Or,
        FilterOp::Expr,
        FilterOp::GtColumn,
        FilterOp::GteColumn,
        FilterOp::LtColumn,
        FilterOp::LteColumn,
        FilterOp::EqColumn,
        FilterOp::NeColumn,
    ];

    #[test]
    fn every_name_round_trips() {
        for op in ALL {
            assert_eq!(FilterOp::parse(op.as_str()), Ok(op), "{op:?}");
        }
    }

    #[test]
    fn aliases_parse_to_their_canonical_variant() {
        assert_eq!(FilterOp::parse("neq"), Ok(FilterOp::Ne));
        assert_eq!(FilterOp::parse("ge"), Ok(FilterOp::Gte));
        assert_eq!(FilterOp::parse("le"), Ok(FilterOp::Lte));
    }

    #[test]
    fn unknown_name_is_an_error_naming_the_text() {
        let err = FilterOp::parse("any_in").expect_err("unknown");
        assert_eq!(err.op, "any_in");
        assert_eq!(err.to_string(), "unknown filter operator `any_in`");
        assert!(FilterOp::parse("").is_err());
        assert!(FilterOp::parse("EQ").is_err());
    }

    #[test]
    fn serde_rejects_an_unknown_name() {
        let ok: FilterOp = serde_json::from_str("\"array_overlap\"").expect("known");
        assert_eq!(ok, FilterOp::ArrayOverlap);
        let err = serde_json::from_str::<FilterOp>("\"any_in\"").expect_err("unknown");
        assert!(err.to_string().contains("any_in"), "{err}");
    }
}

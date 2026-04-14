//! `FilterOp` enum and its string/serde conversions.
//!
//! `FilterOp` is an O(1)-dispatch discriminant used by the scan filter
//! evaluator. On-wire it travels as a lowercase string tag so physical
//! plans remain debuggable by hand.

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
            "expr" => Self::Expr,
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

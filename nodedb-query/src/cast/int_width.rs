// SPDX-License-Identifier: Apache-2.0

//! The integer widths a cast targets: `smallint`, `integer`, `bigint`.
//!
//! Every integer is an `i64` at run time. A width lives in the type layer
//! only. The planner types integer arithmetic as PostgreSQL does and casts
//! a `smallint` or `integer` result to its width, so the evaluator checks
//! the `i64` result there. An `i64` holds every exact `+ - * / %` result of
//! two `integer` operands, so checking after the operation raises exactly
//! where PostgreSQL's `int4` and `int2` operators raise.

use nodedb_types::columnar::IntWidth as DeclaredIntWidth;

use crate::expr::{CastType, EvalError, OutOfRange};

/// The width of an integer cast target. Every integer is an `i64` at run
/// time; a narrower width bounds the value. Ordered narrow to wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IntWidth {
    /// `smallint`, the `i16` range.
    Int2,
    /// `integer`, the `i32` range.
    Int4,
    /// `bigint`, the `i64` range.
    Int8,
}

impl IntWidth {
    /// The PostgreSQL type name.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Int2 => "smallint",
            Self::Int4 => "integer",
            Self::Int8 => "bigint",
        }
    }

    /// The least and greatest value of the width.
    pub fn bounds(self) -> (i64, i64) {
        match self {
            Self::Int2 => (i64::from(i16::MIN), i64::from(i16::MAX)),
            Self::Int4 => (i64::from(i32::MIN), i64::from(i32::MAX)),
            Self::Int8 => (i64::MIN, i64::MAX),
        }
    }

    /// The error for a value past the width: "smallint out of range",
    /// "integer out of range" or "bigint out of range".
    pub fn out_of_range(self) -> EvalError {
        EvalError::OutOfRange(match self {
            Self::Int2 => OutOfRange::Smallint,
            Self::Int4 => OutOfRange::Integer,
            Self::Int8 => OutOfRange::Bigint,
        })
    }

    /// `i` when the width holds it, else [`Self::out_of_range`].
    pub fn fit(self, i: i64) -> Result<i64, EvalError> {
        let (min, max) = self.bounds();
        if (min..=max).contains(&i) {
            Ok(i)
        } else {
            Err(self.out_of_range())
        }
    }

    /// The width PostgreSQL gives an integer literal: `integer` when the
    /// `i32` range holds it, else `bigint`. No literal is `smallint`.
    pub fn of_literal(i: i64) -> Self {
        if i32::try_from(i).is_ok() {
            Self::Int4
        } else {
            Self::Int8
        }
    }

    /// The result width of an integer operator over operands of `self` and
    /// `other`: the wider of the two. `smallint + integer` is `integer`, and
    /// `integer * bigint` is `bigint`.
    pub fn wider(self, other: Self) -> Self {
        self.max(other)
    }

    /// The width a cast to `cast` yields. `None` for a target that is not
    /// an integer type.
    pub fn of_cast(cast: &CastType) -> Option<Self> {
        match cast {
            CastType::Int2 => Some(Self::Int2),
            CastType::Int4 => Some(Self::Int4),
            CastType::Int => Some(Self::Int8),
            CastType::Float
            | CastType::String
            | CastType::Bool
            | CastType::Decimal(_)
            | CastType::Uuid
            | CastType::Timestamp
            | CastType::Timestamptz
            | CastType::Duration => None,
        }
    }

    /// The cast to this width.
    pub fn cast_type(self) -> CastType {
        match self {
            Self::Int2 => CastType::Int2,
            Self::Int4 => CastType::Int4,
            Self::Int8 => CastType::Int,
        }
    }
}

impl From<DeclaredIntWidth> for IntWidth {
    /// The width of a column declared `SMALLINT`, `INTEGER` or `BIGINT`.
    fn from(declared: DeclaredIntWidth) -> Self {
        match declared {
            DeclaredIntWidth::I16 => Self::Int2,
            DeclaredIntWidth::I32 => Self::Int4,
            DeclaredIntWidth::I64 => Self::Int8,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crs::CrsContext;
    use nodedb_types::Value;

    use super::*;
    use crate::expr::{BinaryOp, SqlExpr};

    const WIDTHS: [IntWidth; 3] = [IntWidth::Int2, IntWidth::Int4, IntWidth::Int8];

    fn lit(i: i64) -> SqlExpr {
        SqlExpr::Literal(Value::Integer(i))
    }

    /// `left op right` cast to `width`: the plan the planner builds for an
    /// integer operator whose result is `width`.
    fn at_width(left: i64, op: BinaryOp, right: i64, width: IntWidth) -> Result<Value, EvalError> {
        SqlExpr::Cast {
            expr: Box::new(SqlExpr::BinaryOp {
                left: Box::new(lit(left)),
                op,
                right: Box::new(lit(right)),
            }),
            to_type: width.cast_type(),
        }
        .eval(&Value::Null, &CrsContext::builtin_only())
    }

    /// `-operand` cast to `width`.
    fn negated_at_width(operand: i64, width: IntWidth) -> Result<Value, EvalError> {
        SqlExpr::Cast {
            expr: Box::new(SqlExpr::Negate(Box::new(lit(operand)))),
            to_type: width.cast_type(),
        }
        .eval(&Value::Null, &CrsContext::builtin_only())
    }

    #[test]
    fn mixed_widths_promote_to_the_wider() {
        for a in WIDTHS {
            for b in WIDTHS {
                assert_eq!(a.wider(b), a.max(b), "{a:?} {b:?}");
                assert_eq!(a.wider(b), b.wider(a), "{a:?} {b:?}");
            }
        }
        assert_eq!(IntWidth::Int2.wider(IntWidth::Int2), IntWidth::Int2);
        assert_eq!(IntWidth::Int2.wider(IntWidth::Int4), IntWidth::Int4);
        assert_eq!(IntWidth::Int2.wider(IntWidth::Int8), IntWidth::Int8);
        assert_eq!(IntWidth::Int4.wider(IntWidth::Int4), IntWidth::Int4);
        assert_eq!(IntWidth::Int4.wider(IntWidth::Int8), IntWidth::Int8);
        assert_eq!(IntWidth::Int8.wider(IntWidth::Int8), IntWidth::Int8);
    }

    #[test]
    fn a_literal_is_integer_when_it_fits() {
        assert_eq!(IntWidth::of_literal(0), IntWidth::Int4);
        assert_eq!(IntWidth::of_literal(1), IntWidth::Int4);
        assert_eq!(IntWidth::of_literal(i64::from(i32::MAX)), IntWidth::Int4);
        assert_eq!(IntWidth::of_literal(i64::from(i32::MIN)), IntWidth::Int4);
        assert_eq!(
            IntWidth::of_literal(i64::from(i32::MAX) + 1),
            IntWidth::Int8
        );
        assert_eq!(
            IntWidth::of_literal(i64::from(i32::MIN) - 1),
            IntWidth::Int8
        );
        assert_eq!(IntWidth::of_literal(i64::MAX), IntWidth::Int8);
    }

    #[test]
    fn cast_types_map_to_widths_and_back() {
        for width in WIDTHS {
            assert_eq!(IntWidth::of_cast(&width.cast_type()), Some(width));
        }
        assert_eq!(IntWidth::of_cast(&CastType::Float), None);
        assert_eq!(IntWidth::of_cast(&CastType::Decimal(None)), None);
        assert_eq!(IntWidth::from(DeclaredIntWidth::I16), IntWidth::Int2);
        assert_eq!(IntWidth::from(DeclaredIntWidth::I32), IntWidth::Int4);
        assert_eq!(IntWidth::from(DeclaredIntWidth::I64), IntWidth::Int8);
    }

    /// `+`, `-` and `*` raise at the result width, one past each bound, with
    /// PostgreSQL's message for that width. The bound itself is in range.
    #[test]
    fn arithmetic_overflows_at_each_boundary() {
        for width in WIDTHS {
            let (min, max) = width.bounds();
            let message = format!("{} out of range", width.type_name());
            assert_eq!(
                at_width(max - 1, BinaryOp::Add, 1, width),
                Ok(Value::Integer(max))
            );
            assert_eq!(
                at_width(min + 1, BinaryOp::Sub, 1, width),
                Ok(Value::Integer(min))
            );
            for (a, op, b) in [
                (max, BinaryOp::Add, 1),
                (min, BinaryOp::Sub, 1),
                (max, BinaryOp::Mul, 2),
                (min, BinaryOp::Mul, -1),
            ] {
                let err = at_width(a, op, b, width).unwrap_err();
                assert_eq!(err, width.out_of_range(), "{width:?} {a} {op:?} {b}");
                assert_eq!(err.to_string(), message, "{width:?} {a} {op:?} {b}");
            }
        }
    }

    /// `integer` arithmetic raises at 2^31, and `smallint` arithmetic at
    /// 2^15, where a `bigint` result does not.
    #[test]
    fn narrow_results_raise_where_bigint_does_not() {
        let int4_max = i64::from(i32::MAX);
        let int2_max = i64::from(i16::MAX);
        assert_eq!(
            at_width(int4_max, BinaryOp::Add, 1, IntWidth::Int4)
                .unwrap_err()
                .to_string(),
            "integer out of range"
        );
        assert_eq!(
            at_width(int4_max, BinaryOp::Add, 1, IntWidth::Int8),
            Ok(Value::Integer(int4_max + 1))
        );
        assert_eq!(
            at_width(int2_max, BinaryOp::Add, 1, IntWidth::Int2)
                .unwrap_err()
                .to_string(),
            "smallint out of range"
        );
        assert_eq!(
            at_width(int2_max, BinaryOp::Add, 1, IntWidth::Int4),
            Ok(Value::Integer(int2_max + 1))
        );
        assert_eq!(
            at_width(65_536, BinaryOp::Mul, 32_768, IntWidth::Int4)
                .unwrap_err()
                .to_string(),
            "integer out of range"
        );
    }

    /// `/` truncates toward zero and `%` keeps the dividend's sign at every
    /// width. `MIN / -1` overflows the width, `MIN % -1` is 0, and a zero
    /// divisor is `22012`.
    #[test]
    fn division_and_modulo_follow_postgres() {
        for width in WIDTHS {
            let (min, _) = width.bounds();
            assert_eq!(at_width(7, BinaryOp::Div, 2, width), Ok(Value::Integer(3)));
            assert_eq!(
                at_width(-7, BinaryOp::Div, 2, width),
                Ok(Value::Integer(-3))
            );
            assert_eq!(at_width(1, BinaryOp::Div, 3, width), Ok(Value::Integer(0)));
            assert_eq!(at_width(7, BinaryOp::Mod, 3, width), Ok(Value::Integer(1)));
            assert_eq!(
                at_width(-7, BinaryOp::Mod, 3, width),
                Ok(Value::Integer(-1))
            );
            assert_eq!(at_width(7, BinaryOp::Mod, -3, width), Ok(Value::Integer(1)));
            assert_eq!(
                at_width(min, BinaryOp::Div, -1, width),
                Err(width.out_of_range()),
                "{width:?}"
            );
            assert_eq!(
                at_width(min, BinaryOp::Mod, -1, width),
                Ok(Value::Integer(0)),
                "{width:?}"
            );
            assert_eq!(
                at_width(1, BinaryOp::Div, 0, width),
                Err(EvalError::DivisionByZero)
            );
            assert_eq!(
                at_width(1, BinaryOp::Mod, 0, width),
                Err(EvalError::DivisionByZero)
            );
        }
    }

    /// Unary minus of the least value of a width overflows it. Of the
    /// greatest value it is in range.
    #[test]
    fn unary_minus_of_the_minimum_overflows() {
        for width in WIDTHS {
            let (min, max) = width.bounds();
            assert_eq!(negated_at_width(max, width), Ok(Value::Integer(-max)));
            let err = negated_at_width(min, width).unwrap_err();
            assert_eq!(err, width.out_of_range(), "{width:?}");
            assert_eq!(
                err.to_string(),
                format!("{} out of range", width.type_name())
            );
        }
    }

    /// A NULL operand gives NULL at every width.
    #[test]
    fn null_stays_null() {
        for width in WIDTHS {
            let expr = SqlExpr::Cast {
                expr: Box::new(SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Literal(Value::Null)),
                    op: BinaryOp::Add,
                    right: Box::new(lit(1)),
                }),
                to_type: width.cast_type(),
            };
            assert_eq!(
                expr.eval(&Value::Null, &CrsContext::builtin_only()),
                Ok(Value::Null)
            );
        }
    }
}

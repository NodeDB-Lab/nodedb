//! Manual zerompk wire format for [`SqlExpr`].
//!
//! Each variant encodes as an array `[tag_u8, field1, field2, ...]`. Tags
//! are stable and MUST NOT be renumbered — they are on-wire values in
//! physical-plan envelopes. `Value`, `BinaryOp`, and `CastType` implement
//! zerompk natively so they nest transparently.
//!
//! Tags: Column=0, Literal=1, BinaryOp=2, Negate=3, Function=4, Cast=5,
//!       Case=6, Coalesce=7, NullIf=8, IsNull=9, OldColumn=10.

use nodedb_types::Value;

use super::types::{BinaryOp, CastType, SqlExpr};

impl zerompk::ToMessagePack for SqlExpr {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            SqlExpr::Column(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(0)?;
                writer.write_string(s)
            }
            SqlExpr::Literal(v) => {
                writer.write_array_len(2)?;
                writer.write_u8(1)?;
                v.write(writer)
            }
            SqlExpr::BinaryOp { left, op, right } => {
                writer.write_array_len(4)?;
                writer.write_u8(2)?;
                left.write(writer)?;
                op.write(writer)?;
                right.write(writer)
            }
            SqlExpr::Negate(inner) => {
                writer.write_array_len(2)?;
                writer.write_u8(3)?;
                inner.write(writer)
            }
            SqlExpr::Function { name, args } => {
                writer.write_array_len(3)?;
                writer.write_u8(4)?;
                writer.write_string(name)?;
                args.write(writer)
            }
            SqlExpr::Cast { expr, to_type } => {
                writer.write_array_len(3)?;
                writer.write_u8(5)?;
                expr.write(writer)?;
                to_type.write(writer)
            }
            SqlExpr::Case {
                operand,
                when_thens,
                else_expr,
            } => {
                writer.write_array_len(4)?;
                writer.write_u8(6)?;
                operand.write(writer)?;
                writer.write_array_len(when_thens.len())?;
                for (cond, val) in when_thens {
                    writer.write_array_len(2)?;
                    cond.write(writer)?;
                    val.write(writer)?;
                }
                else_expr.write(writer)
            }
            SqlExpr::Coalesce(exprs) => {
                writer.write_array_len(2)?;
                writer.write_u8(7)?;
                exprs.write(writer)
            }
            SqlExpr::NullIf(e1, e2) => {
                writer.write_array_len(3)?;
                writer.write_u8(8)?;
                e1.write(writer)?;
                e2.write(writer)
            }
            SqlExpr::IsNull { expr, negated } => {
                writer.write_array_len(3)?;
                writer.write_u8(9)?;
                expr.write(writer)?;
                writer.write_boolean(*negated)
            }
            SqlExpr::OldColumn(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(10)?;
                writer.write_string(s)
            }
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for SqlExpr {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len == 0 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 1,
                actual: 0,
            });
        }
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(SqlExpr::Column(reader.read_string()?.into_owned())),
            1 => {
                let v = Value::read(reader)?;
                Ok(SqlExpr::Literal(v))
            }
            2 => {
                let left = SqlExpr::read(reader)?;
                let op = BinaryOp::read(reader)?;
                let right = SqlExpr::read(reader)?;
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            3 => {
                let inner = SqlExpr::read(reader)?;
                Ok(SqlExpr::Negate(Box::new(inner)))
            }
            4 => {
                let name = reader.read_string()?.into_owned();
                let args = Vec::<SqlExpr>::read(reader)?;
                Ok(SqlExpr::Function { name, args })
            }
            5 => {
                let expr = SqlExpr::read(reader)?;
                let to_type = CastType::read(reader)?;
                Ok(SqlExpr::Cast {
                    expr: Box::new(expr),
                    to_type,
                })
            }
            6 => {
                let operand = Option::<Box<SqlExpr>>::read(reader)?;
                let wt_len = reader.read_array_len()?;
                let mut when_thens = Vec::with_capacity(wt_len);
                for _ in 0..wt_len {
                    let pair_len = reader.read_array_len()?;
                    if pair_len != 2 {
                        return Err(zerompk::Error::ArrayLengthMismatch {
                            expected: 2,
                            actual: pair_len,
                        });
                    }
                    let cond = SqlExpr::read(reader)?;
                    let val = SqlExpr::read(reader)?;
                    when_thens.push((cond, val));
                }
                let else_expr = Option::<Box<SqlExpr>>::read(reader)?;
                Ok(SqlExpr::Case {
                    operand,
                    when_thens,
                    else_expr,
                })
            }
            7 => {
                let exprs = Vec::<SqlExpr>::read(reader)?;
                Ok(SqlExpr::Coalesce(exprs))
            }
            8 => {
                let e1 = SqlExpr::read(reader)?;
                let e2 = SqlExpr::read(reader)?;
                Ok(SqlExpr::NullIf(Box::new(e1), Box::new(e2)))
            }
            9 => {
                let expr = SqlExpr::read(reader)?;
                let negated = reader.read_boolean()?;
                Ok(SqlExpr::IsNull {
                    expr: Box::new(expr),
                    negated,
                })
            }
            10 => Ok(SqlExpr::OldColumn(reader.read_string()?.into_owned())),
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

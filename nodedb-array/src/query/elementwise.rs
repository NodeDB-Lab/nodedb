// SPDX-License-Identifier: Apache-2.0

//! Pairwise binary ops between two coord-aligned sparse tiles.
//!
//! Both inputs must share the same schema (same dim arity, same attr
//! columns, same dtypes). We outer-join on coordinates: cells present
//! in only one operand contribute a Null on the other side, which
//! propagates as Null through the op (SQL-style null semantics).
//!
//! A cell present in both operands with a NULL op attr yields Null. A cell
//! present in one operand only takes that operand's value.
//!
//! The op reads one named attr. Every other attr passes through from the
//! left operand. A non-numeric value (String, Bytes) in the named attr also
//! passes through, since the op is undefined for it. Division by zero yields
//! Null rather than infinity, so downstream aggregates stay finite.
//!
//! Integer Add, Sub, and Mul are exact `Int64` and refuse past the `i64`
//! range with SQLSTATE `22003`. Div, and any op over a float attr, yields
//! `Float64`.

use std::collections::BTreeMap;

use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::tile::sparse_tile::{RowKind, SparseTile, SparseTileBuilder};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Apply `op` to attr `attr_idx` of every aligned pair of cells. The result
/// tile carries the union of both coord sets. Every other attr passes
/// through, from the left operand when it has the cell.
pub fn elementwise(
    schema: &ArraySchema,
    a: &SparseTile,
    b: &SparseTile,
    op: BinaryOp,
    attr_idx: usize,
) -> ArrayResult<SparseTile> {
    let n_attrs = schema.attrs.len();
    if attr_idx >= n_attrs {
        return Err(ArrayError::InvalidOp {
            detail: format!(
                "elementwise: attr index {attr_idx} is out of range for {n_attrs} attrs of '{}'",
                schema.name
            ),
        });
    }
    let by_coord_a = index_rows(a)?;
    let by_coord_b = index_rows(b)?;
    let mut keys: BTreeMap<Vec<CoordKey>, ()> = BTreeMap::new();
    for k in by_coord_a.keys().chain(by_coord_b.keys()) {
        keys.insert(k.clone(), ());
    }

    let mut builder = SparseTileBuilder::new(schema);
    for key in keys.keys() {
        let coord = decode_key(key);
        let lhs = by_coord_a.get(key);
        let rhs = by_coord_b.get(key);
        let mut out = Vec::with_capacity(n_attrs);
        for i in 0..n_attrs {
            let l = lhs.map(|v| v[i].clone()).unwrap_or(CellValue::Null);
            let r = rhs.map(|v| v[i].clone()).unwrap_or(CellValue::Null);
            out.push(if i == attr_idx {
                if lhs.is_some() && rhs.is_some() && (l.is_null() || r.is_null()) {
                    CellValue::Null
                } else {
                    apply(&l, &r, op)?
                }
            } else {
                passthrough(&l, &r)
            });
        }
        builder.push(&coord, &out)?;
    }
    Ok(builder.build())
}

/// Add, Sub, and Mul of two integer cells is an exact `Int64`, refused past
/// the `i64` range. Div, and any op with a float operand, is a `Float64`.
fn apply(l: &CellValue, r: &CellValue, op: BinaryOp) -> ArrayResult<CellValue> {
    if let (Some(a), Some(b)) = (l.as_i64(), r.as_i64()) {
        let exact = match op {
            BinaryOp::Add => Some(a.checked_add(b)),
            BinaryOp::Sub => Some(a.checked_sub(b)),
            BinaryOp::Mul => Some(a.checked_mul(b)),
            BinaryOp::Div => None,
        };
        if let Some(v) = exact {
            return v
                .map(CellValue::Int64)
                .ok_or_else(|| ArrayError::NumericOverflow {
                    detail: format!("elementwise {op:?} of {a} and {b} is out of range for bigint"),
                });
        }
    }
    let (lf, rf) = match (l.as_f64(), r.as_f64()) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(widen_to_result(passthrough(l, r), op)),
    };
    let v = match op {
        BinaryOp::Add => lf + rf,
        BinaryOp::Sub => lf - rf,
        BinaryOp::Mul => lf * rf,
        BinaryOp::Div => {
            if rf == 0.0 {
                return Ok(CellValue::Null);
            }
            lf / rf
        }
    };
    Ok(CellValue::Float64(v))
}

/// A passed-through numeric value of the op attr takes the type the op
/// yields, so the result column holds one type: `Int64` for an integer
/// Add, Sub, or Mul, and `Float64` otherwise.
fn widen_to_result(v: CellValue, op: BinaryOp) -> CellValue {
    let int_op = match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul => true,
        BinaryOp::Div => false,
    };
    match (v.as_i64(), v.as_float()) {
        (Some(i), _) if int_op => CellValue::Int64(i),
        (Some(i), _) => CellValue::Float64(i as f64),
        (None, Some(f)) => CellValue::Float64(f),
        (None, None) => v,
    }
}

fn passthrough(l: &CellValue, r: &CellValue) -> CellValue {
    if !l.is_null() {
        l.clone()
    } else if !r.is_null() {
        r.clone()
    } else {
        CellValue::Null
    }
}

/// Hashable, totally-ordered key for a coordinate tuple.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CoordKey {
    I(i64),
    Ts(i64),
    F(u64), // f64 bits
    S(String),
}

fn encode_key(c: &[CoordValue]) -> Vec<CoordKey> {
    c.iter()
        .map(|v| match v {
            CoordValue::Int64(x) => CoordKey::I(*x),
            CoordValue::TimestampMs(x) => CoordKey::Ts(*x),
            CoordValue::Float64(x) => CoordKey::F(x.to_bits()),
            CoordValue::String(s) => CoordKey::S(s.clone()),
        })
        .collect()
}

fn decode_key(k: &[CoordKey]) -> Vec<CoordValue> {
    k.iter()
        .map(|v| match v {
            CoordKey::I(x) => CoordValue::Int64(*x),
            CoordKey::Ts(x) => CoordValue::TimestampMs(*x),
            CoordKey::F(b) => CoordValue::Float64(f64::from_bits(*b)),
            CoordKey::S(s) => CoordValue::String(s.clone()),
        })
        .collect()
}

/// The live cells of `tile` by coordinate. Tombstone and erasure rows hold
/// no attribute entries, so attributes are read at the live-row index.
fn index_rows(tile: &SparseTile) -> ArrayResult<BTreeMap<Vec<CoordKey>, Vec<CellValue>>> {
    let corrupt = |row: usize| ArrayError::SegmentCorruption {
        detail: format!("elementwise: sparse tile row {row} is out of range"),
    };
    let mut out = BTreeMap::new();
    let mut live_row = 0usize;
    for row in 0..tile.row_count() {
        if tile.row_kind(row)? != RowKind::Live {
            continue;
        }
        let coord = tile
            .dim_dicts
            .iter()
            .map(|d| {
                d.indices
                    .get(row)
                    .and_then(|&i| d.values.get(i as usize))
                    .cloned()
                    .ok_or_else(|| corrupt(row))
            })
            .collect::<ArrayResult<Vec<CoordValue>>>()?;
        let attrs = tile
            .attr_cols
            .iter()
            .map(|col| col.get(live_row).cloned().ok_or_else(|| corrupt(row)))
            .collect::<ArrayResult<Vec<CellValue>>>()?;
        live_row += 1;
        out.insert(encode_key(&coord), attrs);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    fn tile(rows: &[(i64, i64)]) -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for (k, v) in rows {
            b.push(&[CoordValue::Int64(*k)], &[CellValue::Int64(*v)])
                .unwrap();
        }
        b.build()
    }

    #[test]
    fn add_aligned_cells() {
        let s = schema();
        let a = tile(&[(0, 1), (1, 2)]);
        let b = tile(&[(0, 10), (1, 20)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Add, 0).unwrap();
        assert_eq!(out.nnz(), 2);
        assert_eq!(out.attr_cols[0][0], CellValue::Int64(11));
        assert_eq!(out.attr_cols[0][1], CellValue::Int64(22));
    }

    #[test]
    fn outer_join_propagates_null() {
        let s = schema();
        let a = tile(&[(0, 1)]);
        let b = tile(&[(1, 2)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Add, 0).unwrap();
        assert_eq!(out.nnz(), 2);
        // Both rows are missing one operand → both become Null+something
        // → passthrough gives the present operand.
        let v0 = &out.attr_cols[0][0];
        let v1 = &out.attr_cols[0][1];
        assert!(matches!(v0, CellValue::Int64(1) | CellValue::Int64(2)));
        assert!(matches!(v1, CellValue::Int64(1) | CellValue::Int64(2)));
    }

    #[test]
    fn a_null_cell_present_in_both_operands_yields_null() {
        let s = schema();
        let mut ab = SparseTileBuilder::new(&s);
        ab.push(&[CoordValue::Int64(0)], &[CellValue::Null])
            .unwrap();
        let a = ab.build();
        let b = tile(&[(0, 10)]);
        for op in [BinaryOp::Add, BinaryOp::Sub, BinaryOp::Mul, BinaryOp::Div] {
            let out = elementwise(&s, &a, &b, op, 0).unwrap();
            assert_eq!(out.attr_cols[0][0], CellValue::Null, "{op:?}, left NULL");
            let out = elementwise(&s, &b, &a, op, 0).unwrap();
            assert_eq!(out.attr_cols[0][0], CellValue::Null, "{op:?}, right NULL");
        }
    }

    #[test]
    fn div_by_zero_returns_null() {
        let s = schema();
        let a = tile(&[(0, 5)]);
        let b = tile(&[(0, 0)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Div, 0).unwrap();
        assert_eq!(out.attr_cols[0][0], CellValue::Null);
    }

    /// A tombstone row before a live row neither joins the result nor
    /// shifts the live row's attributes.
    #[test]
    fn a_tombstone_row_is_skipped() {
        use crate::tile::sparse_tile::SparseRow;
        use nodedb_types::{OPEN_UPPER, Surrogate};

        let s = schema();
        let mut builder = SparseTileBuilder::new(&s);
        builder
            .push_row(SparseRow {
                coord: &[CoordValue::Int64(0)],
                attrs: &[],
                surrogate: None,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Tombstone,
            })
            .unwrap();
        builder
            .push_row(SparseRow {
                coord: &[CoordValue::Int64(1)],
                attrs: &[CellValue::Int64(4)],
                surrogate: Some(Surrogate::new(1)),
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        let a = builder.build();
        let b = tile(&[(1, 6)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Add, 0).unwrap();
        assert_eq!(out.nnz(), 1);
        assert_eq!(out.attr_cols[0][0], CellValue::Int64(10));
    }

    #[test]
    fn sub_and_mul() {
        let s = schema();
        let a = tile(&[(0, 10)]);
        let b = tile(&[(0, 3)]);
        let s1 = elementwise(&s, &a, &b, BinaryOp::Sub, 0).unwrap();
        let s2 = elementwise(&s, &a, &b, BinaryOp::Mul, 0).unwrap();
        assert_eq!(s1.attr_cols[0][0], CellValue::Int64(7));
        assert_eq!(s2.attr_cols[0][0], CellValue::Int64(30));
    }

    fn two_attr_schema() -> ArraySchema {
        ArraySchemaBuilder::new("g2")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("a", AttrType::Int64, true))
            .attr(AttrSpec::new("b", AttrType::Int64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    fn two_attr_tile(s: &ArraySchema, k: i64, a: i64, b: i64) -> SparseTile {
        let mut builder = SparseTileBuilder::new(s);
        builder
            .push(
                &[CoordValue::Int64(k)],
                &[CellValue::Int64(a), CellValue::Int64(b)],
            )
            .unwrap();
        builder.build()
    }

    /// Only the named attr takes the op. The other attr keeps the left
    /// operand's value.
    #[test]
    fn the_op_reads_the_named_attr_only() {
        let s = two_attr_schema();
        let left = two_attr_tile(&s, 0, 1, 2);
        let right = two_attr_tile(&s, 0, 10, 20);
        let on_b = elementwise(&s, &left, &right, BinaryOp::Add, 1).unwrap();
        assert_eq!(on_b.attr_cols[1][0], CellValue::Int64(22));
        assert_eq!(on_b.attr_cols[0][0], CellValue::Int64(1));
        let on_a = elementwise(&s, &left, &right, BinaryOp::Add, 0).unwrap();
        assert_eq!(on_a.attr_cols[0][0], CellValue::Int64(11));
        assert_eq!(on_a.attr_cols[1][0], CellValue::Int64(2));
    }

    fn typed_tile(dtype: AttrType, rows: &[(i64, CellValue)]) -> (ArraySchema, SparseTile) {
        let s = ArraySchemaBuilder::new("t")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", dtype, true))
            .tile_extents(vec![16])
            .build()
            .unwrap();
        let mut b = SparseTileBuilder::new(&s);
        for (k, v) in rows {
            b.push(&[CoordValue::Int64(*k)], std::slice::from_ref(v))
                .unwrap();
        }
        let t = b.build();
        (s, t)
    }

    /// Narrow integers add exactly into `Int64`, and a passed-through cell
    /// takes the same type.
    #[test]
    fn narrow_integers_add_into_int64() {
        let (s, a) = typed_tile(
            AttrType::UInt16,
            &[(0, CellValue::UInt16(65535)), (1, CellValue::UInt16(3))],
        );
        let (_, b) = typed_tile(AttrType::UInt16, &[(0, CellValue::UInt16(65535))]);
        let out = elementwise(&s, &a, &b, BinaryOp::Add, 0).unwrap();
        assert_eq!(out.attr_cols[0][0], CellValue::Int64(131_070));
        assert_eq!(out.attr_cols[0][1], CellValue::Int64(3));
    }

    #[test]
    fn integer_overflow_is_refused() {
        let s = schema();
        let a = tile(&[(0, i64::MAX)]);
        let b = tile(&[(0, 2)]);
        assert!(matches!(
            elementwise(&s, &a, &b, BinaryOp::Mul, 0),
            Err(ArrayError::NumericOverflow { .. })
        ));
    }

    /// Div of integers, and any op over a float attr, yields `Float64`.
    #[test]
    fn div_and_floats_yield_float64() {
        let s = schema();
        let a = tile(&[(0, 6), (1, 7)]);
        let b = tile(&[(0, 3)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Div, 0).unwrap();
        assert_eq!(out.attr_cols[0][0], CellValue::Float64(2.0));
        assert_eq!(out.attr_cols[0][1], CellValue::Float64(7.0));
        let (fs, fa) = typed_tile(AttrType::Float32, &[(0, CellValue::Float32(0.5))]);
        let out = elementwise(&fs, &fa, &fa, BinaryOp::Add, 0).unwrap();
        assert_eq!(out.attr_cols[0][0], CellValue::Float64(1.0));
    }

    #[test]
    fn an_attr_index_out_of_range_is_refused() {
        let s = two_attr_schema();
        let t = two_attr_tile(&s, 0, 1, 2);
        assert!(elementwise(&s, &t, &t, BinaryOp::Add, 2).is_err());
    }
}

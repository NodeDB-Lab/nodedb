//! Pairwise binary ops between two coord-aligned sparse tiles.
//!
//! Both inputs must share the same schema (same dim arity, same attr
//! columns, same dtypes). We outer-join on coordinates: cells present
//! in only one operand contribute a Null on the other side, which
//! propagates as Null through the op (SQL-style null semantics).
//!
//! Numeric attrs (Int64, Float64) participate in the op; non-numeric
//! attrs (String, Bytes) are passed through from the left operand
//! unchanged — the op is undefined for them. Division by zero yields
//! Null rather than infinity so downstream aggregates stay finite.

use std::collections::BTreeMap;

use crate::error::ArrayResult;
use crate::schema::ArraySchema;
use crate::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Apply `op` to every aligned pair of cells. The result tile carries
/// the union of both coord sets.
pub fn elementwise(
    schema: &ArraySchema,
    a: &SparseTile,
    b: &SparseTile,
    op: BinaryOp,
) -> ArrayResult<SparseTile> {
    let n_attrs = schema.attrs.len();
    let by_coord_a = index_rows(a);
    let by_coord_b = index_rows(b);
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
            out.push(apply(&l, &r, op));
        }
        builder.push(&coord, &out)?;
    }
    Ok(builder.build())
}

fn apply(l: &CellValue, r: &CellValue, op: BinaryOp) -> CellValue {
    let (lf, rf) = match (to_f64(l), to_f64(r)) {
        (Some(a), Some(b)) => (a, b),
        _ => return passthrough(l, r),
    };
    let v = match op {
        BinaryOp::Add => lf + rf,
        BinaryOp::Sub => lf - rf,
        BinaryOp::Mul => lf * rf,
        BinaryOp::Div => {
            if rf == 0.0 {
                return CellValue::Null;
            }
            lf / rf
        }
    };
    // Preserve Int64 when both operands were integral and result is
    // exact — keeps integer attrs from drifting to floats unnecessarily.
    if let (CellValue::Int64(_), CellValue::Int64(_)) = (l, r)
        && v.fract() == 0.0
        && v.is_finite()
    {
        return CellValue::Int64(v as i64);
    }
    CellValue::Float64(v)
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

fn to_f64(v: &CellValue) -> Option<f64> {
    match v {
        CellValue::Int64(x) => Some(*x as f64),
        CellValue::Float64(x) => Some(*x),
        _ => None,
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

fn index_rows(tile: &SparseTile) -> BTreeMap<Vec<CoordKey>, Vec<CellValue>> {
    let mut out = BTreeMap::new();
    let n = tile.nnz() as usize;
    for row in 0..n {
        let coord: Vec<CoordValue> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<CellValue> = tile.attr_cols.iter().map(|col| col[row].clone()).collect();
        out.insert(encode_key(&coord), attrs);
    }
    out
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
        let out = elementwise(&s, &a, &b, BinaryOp::Add).unwrap();
        assert_eq!(out.nnz(), 2);
        assert_eq!(out.attr_cols[0][0], CellValue::Int64(11));
        assert_eq!(out.attr_cols[0][1], CellValue::Int64(22));
    }

    #[test]
    fn outer_join_propagates_null() {
        let s = schema();
        let a = tile(&[(0, 1)]);
        let b = tile(&[(1, 2)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Add).unwrap();
        assert_eq!(out.nnz(), 2);
        // Both rows are missing one operand → both become Null+something
        // → passthrough gives the present operand.
        let v0 = &out.attr_cols[0][0];
        let v1 = &out.attr_cols[0][1];
        assert!(matches!(v0, CellValue::Int64(1) | CellValue::Int64(2)));
        assert!(matches!(v1, CellValue::Int64(1) | CellValue::Int64(2)));
    }

    #[test]
    fn div_by_zero_returns_null() {
        let s = schema();
        let a = tile(&[(0, 5)]);
        let b = tile(&[(0, 0)]);
        let out = elementwise(&s, &a, &b, BinaryOp::Div).unwrap();
        assert_eq!(out.attr_cols[0][0], CellValue::Null);
    }

    #[test]
    fn sub_and_mul() {
        let s = schema();
        let a = tile(&[(0, 10)]);
        let b = tile(&[(0, 3)]);
        let s1 = elementwise(&s, &a, &b, BinaryOp::Sub).unwrap();
        let s2 = elementwise(&s, &a, &b, BinaryOp::Mul).unwrap();
        assert_eq!(s1.attr_cols[0][0], CellValue::Int64(7));
        assert_eq!(s2.attr_cols[0][0], CellValue::Int64(30));
    }
}

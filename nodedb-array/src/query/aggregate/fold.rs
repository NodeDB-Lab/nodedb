// SPDX-License-Identifier: Apache-2.0

//! Folding a tile's cells into reducer partials.
//!
//! A NULL cell and a cell equal to the attr's nodata contribute to no
//! reducer, `COUNT` included: nodata is not data. With `physical` set, a
//! numeric cell of an attr with a scale or offset contributes
//! `raw * scale + offset` as a float.

use std::collections::HashMap;

use nodedb_types::array_agg::{DistAcc, DistValue, Moments};

use super::reducer::Reducer;
use super::result::AggregateResult;
use crate::query::agg_number::AggNumber;
use crate::schema::attr_meta::AttrMeta;
use crate::tile::sparse_tile::{RowKind, SparseTile};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// How a reducer reads one attr's cells.
#[derive(Debug, Clone, Copy)]
pub struct AggRead<'a> {
    pub reducer: Reducer,
    /// The attr's metadata: its nodata, scale and offset.
    pub meta: &'a AttrMeta,
    /// Whether numeric cells read as `raw * scale + offset`.
    pub physical: bool,
}

impl AggRead<'_> {
    /// The number `v` contributes, or `None` for a NULL, nodata or
    /// non-numeric cell.
    fn value(&self, v: &CellValue) -> Option<DistValue> {
        if v.is_null() || self.meta.is_nodata(v) {
            return None;
        }
        if self.physical
            && let (Some((scale, offset)), Some(raw)) = (self.meta.transform(), v.as_f64())
        {
            return Some(DistValue::Float(raw * scale + offset));
        }
        match v.as_i64() {
            Some(i) => Some(DistValue::Int(i)),
            None => v.as_float().map(DistValue::Float),
        }
    }

    /// Fold cell `v` into `acc`.
    fn fold(&self, acc: &mut AggregateResult, v: &CellValue) {
        if matches!(self.reducer, Reducer::Count) {
            if !v.is_null() && !self.meta.is_nodata(v) {
                acc.absorb(AggregateResult::Count { count: 1 });
            }
            return;
        }
        let Some(value) = self.value(v) else {
            return;
        };
        let number = match value {
            DistValue::Int(i) => AggNumber::Int(i128::from(i)),
            DistValue::Float(f) => AggNumber::Float(f),
        };
        let as_f64 = number.to_f64();
        match self.reducer {
            Reducer::Count => {}
            Reducer::Sum => acc.absorb(AggregateResult::Sum {
                value: number,
                count: 1,
            }),
            Reducer::Min => acc.absorb(AggregateResult::Min {
                value: number,
                count: 1,
            }),
            Reducer::Max => acc.absorb(AggregateResult::Max {
                value: number,
                count: 1,
            }),
            Reducer::Mean => acc.absorb(AggregateResult::Mean {
                sum: number,
                count: 1,
            }),
            Reducer::Median | Reducer::Percentile(_) | Reducer::Mode => match acc {
                AggregateResult::Dist { acc: dist, .. } => dist.insert(value, 1),
                other => {
                    let mut dist = DistAcc::default();
                    dist.insert(value, 1);
                    other.absorb(AggregateResult::Dist {
                        reducer: self.reducer,
                        acc: dist,
                    });
                }
            },
            Reducer::StdDevPop | Reducer::StdDevSamp | Reducer::VarPop | Reducer::VarSamp => acc
                .absorb(AggregateResult::Moments {
                    reducer: self.reducer,
                    moments: Moments::one(as_f64),
                }),
            Reducer::Histogram(spec) => {
                if let AggregateResult::Empty(_) = acc {
                    *acc = AggregateResult::Histogram {
                        spec,
                        counts: spec.empty_counts(),
                    };
                }
                if let (AggregateResult::Histogram { counts, .. }, Some(bin)) =
                    (acc, spec.bin_of(as_f64))
                    && let Some(slot) = counts.get_mut(bin)
                {
                    *slot += 1;
                }
            }
        }
    }
}

/// Reduce attr `attr_idx` over every live row of `tile`.
pub fn aggregate_attr(tile: &SparseTile, attr_idx: usize, read: &AggRead<'_>) -> AggregateResult {
    let mut acc = AggregateResult::empty(read.reducer);
    if let Some(col) = tile.attr_cols.get(attr_idx) {
        for v in col {
            read.fold(&mut acc, v);
        }
    }
    acc
}

/// One bucket of a group-by aggregate: the dim value the rows share and
/// the reducer's partial over them.
#[derive(Debug, Clone)]
pub struct GroupAggregate {
    pub key: CoordValue,
    pub result: AggregateResult,
}

/// Group rows by dim `dim_idx` and reduce `attr_idx` per group. A group
/// exists once any live row holds its key, even when every cell of it is
/// NULL or nodata. Returns groups in first-seen order.
pub fn group_by_dim(
    tile: &SparseTile,
    dim_idx: usize,
    attr_idx: usize,
    read: &AggRead<'_>,
) -> Vec<GroupAggregate> {
    let Some(dict) = tile.dim_dicts.get(dim_idx) else {
        return Vec::new();
    };
    let Some(col) = tile.attr_cols.get(attr_idx) else {
        return Vec::new();
    };
    let mut order: Vec<CoordValue> = Vec::new();
    let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
    // attr_cols holds entries for Live rows only, so the live index runs
    // apart from the physical row.
    let mut live_idx = 0usize;
    for row in 0..tile.row_count() {
        let Ok(kind) = tile.row_kind(row) else {
            break;
        };
        if kind != RowKind::Live {
            continue;
        }
        let Some(cell) = col.get(live_idx) else {
            break;
        };
        live_idx += 1;
        let Some(key) = dict
            .indices
            .get(row)
            .and_then(|&i| dict.values.get(i as usize))
        else {
            break;
        };
        if !by_key.contains_key(key) {
            order.push(key.clone());
            by_key.insert(key.clone(), AggregateResult::empty(read.reducer));
        }
        if let Some(slot) = by_key.get_mut(key) {
            read.fold(slot, cell);
        }
    }
    order
        .into_iter()
        .map(|k| GroupAggregate {
            result: by_key
                .remove(&k)
                .unwrap_or(AggregateResult::empty(read.reducer)),
            key: k,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_types::array_agg::HistogramSpec;

    use super::*;
    use crate::error::ArrayError;
    use crate::query::agg_number::AggScalar;
    use crate::query::aggregate::result::AggOutput;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::domain::{Domain, DomainBound};

    static NO_META: AttrMeta = AttrMeta {
        nodata: None,
        scale: None,
        offset: None,
        units: None,
    };

    fn read(reducer: Reducer) -> AggRead<'static> {
        AggRead {
            reducer,
            meta: &NO_META,
            physical: false,
        }
    }

    /// A tile of `values` in attr `v` of type `dtype`, cell `k` at dim `k`
    /// modulo `groups`.
    fn tile_of(dtype: AttrType, values: &[CellValue], groups: i64) -> SparseTile {
        let s = ArraySchemaBuilder::new("n")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            ))
            .dim(DimSpec::new(
                "i",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            ))
            .attr(AttrSpec::new("v", dtype, true))
            .tile_extents(vec![1024, 1024])
            .build()
            .unwrap();
        let mut b = SparseTileBuilder::new(&s);
        for (i, v) in values.iter().enumerate() {
            let i = i as i64;
            b.push(
                &[CoordValue::Int64(i % groups), CoordValue::Int64(i)],
                std::slice::from_ref(v),
            )
            .unwrap();
        }
        b.build()
    }

    fn floats(values: &[Option<f64>]) -> SparseTile {
        let cells: Vec<CellValue> = values
            .iter()
            .map(|v| v.map(CellValue::Float64).unwrap_or(CellValue::Null))
            .collect();
        tile_of(AttrType::Float64, &cells, 1)
    }

    fn scalar(r: AggregateResult) -> Option<AggScalar> {
        match r.finalize().expect("finalize") {
            AggOutput::Scalar(v) => v,
            AggOutput::Bins(b) => panic!("scalar reducer answered bins: {b:?}"),
        }
    }

    fn float(v: f64) -> Option<AggScalar> {
        Some(AggScalar::Float(v))
    }

    fn close(got: Option<AggScalar>, want: f64) {
        match got {
            Some(AggScalar::Float(v)) => assert!((v - want).abs() < 1e-9, "{v} != {want}"),
            other => panic!("expected float {want}, got {other:?}"),
        }
    }

    #[test]
    fn sum_count_min_max_mean_skip_nulls() {
        let t = floats(&[Some(1.0), None, Some(5.0), Some(3.0)]);
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Sum))),
            float(9.0)
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Count))),
            Some(AggScalar::Int(3))
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Min))),
            float(1.0)
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Max))),
            float(5.0)
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Mean))),
            float(3.0)
        );
    }

    #[test]
    fn integer_reducers_are_exact() {
        let big = (1i64 << 53) + 1;
        let t = tile_of(
            AttrType::Int64,
            &[CellValue::Int64(big), CellValue::Null, CellValue::Int64(0)],
            1,
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Sum))),
            Some(AggScalar::Int(big))
        );
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Max))),
            Some(AggScalar::Int(big))
        );
        let over = tile_of(
            AttrType::Int64,
            &[CellValue::Int64(i64::MAX), CellValue::Int64(1)],
            1,
        );
        assert!(matches!(
            aggregate_attr(&over, 0, &read(Reducer::Sum)).finalize(),
            Err(ArrayError::NumericOverflow { .. })
        ));
    }

    #[test]
    fn median_percentile_and_mode() {
        let t = floats(&[Some(4.0), Some(1.0), Some(3.0), Some(2.0), None]);
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Median))),
            float(2.5)
        );
        close(
            scalar(aggregate_attr(&t, 0, &read(Reducer::Percentile(0.9)))),
            3.7,
        );
        let ints = tile_of(
            AttrType::Int8,
            &[
                CellValue::Int8(7),
                CellValue::Int8(-2),
                CellValue::Int8(7),
                CellValue::Int8(-2),
                CellValue::Int8(5),
            ],
            1,
        );
        // -2 and 7 tie at two cells each; the smaller wins.
        assert_eq!(
            scalar(aggregate_attr(&ints, 0, &read(Reducer::Mode))),
            Some(AggScalar::Int(-2))
        );
        assert_eq!(
            scalar(aggregate_attr(&ints, 0, &read(Reducer::Median))),
            float(5.0)
        );
    }

    #[test]
    fn variance_and_standard_deviation() {
        let t = floats(&[
            Some(2.0),
            Some(4.0),
            Some(4.0),
            Some(4.0),
            Some(5.0),
            Some(5.0),
            Some(7.0),
            Some(9.0),
        ]);
        close(scalar(aggregate_attr(&t, 0, &read(Reducer::VarPop))), 4.0);
        close(
            scalar(aggregate_attr(&t, 0, &read(Reducer::StdDevPop))),
            2.0,
        );
        close(
            scalar(aggregate_attr(&t, 0, &read(Reducer::VarSamp))),
            32.0 / 7.0,
        );
        close(
            scalar(aggregate_attr(&t, 0, &read(Reducer::StdDevSamp))),
            (32.0f64 / 7.0).sqrt(),
        );
        let one = floats(&[Some(3.0)]);
        assert_eq!(
            scalar(aggregate_attr(&one, 0, &read(Reducer::VarSamp))),
            None
        );
    }

    #[test]
    fn histogram_counts_in_range_values() {
        let spec = HistogramSpec::new(2, 0.0, 10.0).expect("valid");
        let t = floats(&[
            Some(0.0),
            Some(4.9),
            Some(5.0),
            Some(10.0),
            Some(11.0),
            None,
        ]);
        let AggOutput::Bins(bins) = aggregate_attr(&t, 0, &read(Reducer::Histogram(spec)))
            .finalize()
            .expect("finalize")
        else {
            panic!("histogram answers bins");
        };
        let counts: Vec<u64> = bins.iter().map(|b| b.count).collect();
        assert_eq!(counts, vec![2, 2]);
        assert_eq!((bins[1].lo, bins[1].hi), (5.0, 10.0));
    }

    /// Every reducer: two partials of a split column merge to the result of
    /// one pass over the whole column.
    #[test]
    fn merged_partials_equal_a_single_pass() {
        let values: Vec<Option<f64>> = [3.0, -1.5, 8.0, 3.0, 0.25, 9.5, 3.0, -4.0, 2.0]
            .iter()
            .map(|v| Some(*v))
            .collect();
        let spec = HistogramSpec::new(4, -4.0, 10.0).expect("valid");
        let whole = floats(&values);
        let (left, right) = values.split_at(4);
        let (a, b) = (floats(left), floats(right));
        for reducer in [
            Reducer::Sum,
            Reducer::Count,
            Reducer::Min,
            Reducer::Max,
            Reducer::Mean,
            Reducer::Median,
            Reducer::Percentile(0.3),
            Reducer::Mode,
            Reducer::VarPop,
            Reducer::VarSamp,
            Reducer::StdDevPop,
            Reducer::StdDevSamp,
            Reducer::Histogram(spec),
        ] {
            let r = read(reducer);
            let merged = aggregate_attr(&a, 0, &r)
                .merge(aggregate_attr(&b, 0, &r))
                .finalize()
                .expect("merged");
            let single = aggregate_attr(&whole, 0, &r).finalize().expect("single");
            match (merged, single) {
                (
                    AggOutput::Scalar(Some(AggScalar::Float(x))),
                    AggOutput::Scalar(Some(AggScalar::Float(y))),
                ) => {
                    assert!((x - y).abs() < 1e-9, "{reducer:?}: {x} != {y}")
                }
                (x, y) => assert_eq!(x, y, "{reducer:?}"),
            }
        }
    }

    /// An all-NULL or all-nodata column answers NULL for every scalar
    /// reducer, `COUNT` included.
    #[test]
    fn all_nodata_answers_null() {
        let meta = AttrMeta {
            nodata: Some(0.0),
            ..AttrMeta::default()
        };
        let t = tile_of(
            AttrType::UInt16,
            &[CellValue::UInt16(0), CellValue::Null, CellValue::UInt16(0)],
            1,
        );
        for reducer in [
            Reducer::Sum,
            Reducer::Count,
            Reducer::Min,
            Reducer::Mean,
            Reducer::Median,
            Reducer::Mode,
            Reducer::VarPop,
        ] {
            let r = AggRead {
                reducer,
                meta: &meta,
                physical: false,
            };
            assert_eq!(scalar(aggregate_attr(&t, 0, &r)), None, "{reducer:?}");
        }
    }

    #[test]
    fn nodata_is_excluded_and_physical_applies_scale_offset() {
        let meta = AttrMeta {
            nodata: Some(0.0),
            scale: Some(0.5),
            offset: Some(-1.0),
            units: None,
        };
        let t = tile_of(
            AttrType::UInt16,
            &[
                CellValue::UInt16(0),
                CellValue::UInt16(4),
                CellValue::UInt16(8),
            ],
            1,
        );
        let raw = AggRead {
            reducer: Reducer::Sum,
            meta: &meta,
            physical: false,
        };
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &raw)),
            Some(AggScalar::Int(12))
        );
        let count = AggRead {
            reducer: Reducer::Count,
            ..raw
        };
        assert_eq!(
            scalar(aggregate_attr(&t, 0, &count)),
            Some(AggScalar::Int(2))
        );
        let physical = AggRead {
            physical: true,
            ..raw
        };
        // (4 * 0.5 - 1) + (8 * 0.5 - 1) = 4.
        assert_eq!(scalar(aggregate_attr(&t, 0, &physical)), float(4.0));
        let mode = AggRead {
            reducer: Reducer::Max,
            ..physical
        };
        assert_eq!(scalar(aggregate_attr(&t, 0, &mode)), float(3.0));
    }

    #[test]
    fn group_by_works_with_every_reducer_kind() {
        // Cell k sits in group k % 2: group 0 holds 1, 3, 5; group 1 holds 2, 4.
        let cells: Vec<CellValue> = (1..=5).map(CellValue::Int64).collect();
        let t = tile_of(AttrType::Int64, &cells, 2);
        let spec = HistogramSpec::new(1, 0.0, 10.0).expect("valid");
        let by = |reducer| group_by_dim(&t, 0, 0, &read(reducer));
        let g = by(Reducer::Median);
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].key, CoordValue::Int64(0));
        assert_eq!(scalar(g[0].result.clone()), float(3.0));
        assert_eq!(scalar(g[1].result.clone()), float(3.0));
        let g = by(Reducer::Sum);
        assert_eq!(scalar(g[0].result.clone()), Some(AggScalar::Int(9)));
        let g = by(Reducer::VarPop);
        close(scalar(g[1].result.clone()), 1.0);
        let g = by(Reducer::Histogram(spec));
        let AggOutput::Bins(bins) = g[0].result.clone().finalize().expect("bins") else {
            panic!("histogram answers bins");
        };
        assert_eq!(bins[0].count, 3);
    }

    #[test]
    fn narrow_float_max_keeps_nan_high() {
        let f = tile_of(
            AttrType::Float32,
            &[CellValue::Float32(0.5), CellValue::Float32(f32::NAN)],
            1,
        );
        assert!(matches!(
            scalar(aggregate_attr(&f, 0, &read(Reducer::Max))),
            Some(AggScalar::Float(v)) if v.is_nan()
        ));
        assert_eq!(
            scalar(aggregate_attr(&f, 0, &read(Reducer::Min))),
            float(0.5)
        );
    }
}

// SPDX-License-Identifier: BUSL-1.1

use std::collections::BTreeMap;

use nodedb_array::ArrayError;
use nodedb_array::query::agg_number::{AggNumber, AggScalar};
use nodedb_array::query::aggregate::{AggOutput, AggregateResult, Reducer};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TilePayload;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::distributed_array::{ArrayAggGroupKey, ArrayAggNumber};
use nodedb_types::array_agg::ValueDist;
use nodedb_types::{InstantKind, NdbDateTime, SurrogateBitmap};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ArrayReducer;

/// Standard-msgpack-friendly cell value for aggregate rows.
#[derive(Clone)]
pub(super) enum AggCell {
    Float(f64),
    Int(i64),
    Str(String),
    Bool(bool),
    /// A timestamp dim value in microseconds. It writes the naive-instant
    /// ext a slice row writes for the same dim.
    Timestamp(i64),
    Null,
}

impl zerompk::ToMessagePack for AggCell {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggCell::Float(f) => writer.write_f64(*f),
            AggCell::Int(i) => writer.write_i64(*i),
            AggCell::Str(s) => writer.write_string(s),
            AggCell::Bool(b) => writer.write_boolean(*b),
            AggCell::Timestamp(micros) => {
                writer.write_ext(InstantKind::Naive.ext_type(), &micros.to_be_bytes())
            }
            AggCell::Null => writer.write_nil(),
        }
    }
}

/// The `group` cell of a grouped row, typed as its dim: a timestamp dim's
/// milliseconds become a timestamp, as in a slice row.
pub(super) fn coord_to_agg_cell(c: &CoordValue) -> Result<AggCell, ErrorCode> {
    Ok(match c {
        CoordValue::Int64(v) => AggCell::Int(*v),
        CoordValue::TimestampMs(ms) => {
            let dt = NdbDateTime::from_millis(*ms).map_err(|e| ErrorCode::Internal {
                detail: format!("array aggregate: timestamp group {ms} ms: {e}"),
            })?;
            AggCell::Timestamp(dt.micros)
        }
        CoordValue::Float64(v) => AggCell::Float(*v),
        CoordValue::String(v) => AggCell::Str(v.clone()),
    })
}

/// One aggregate row: its columns by name.
pub(super) type AggRow = BTreeMap<&'static str, AggCell>;

/// The rows of one finalized partial, after `group` when it groups.
///
/// A scalar reducer gives one row holding `result`. A histogram gives one
/// row per bin holding `bin_lo`, `bin_hi` and `count`, zero-count bins
/// included. A SUM past `i64` is SQLSTATE `22003` and a median, percentile
/// or mode past its distinct-value budget is `54000`, the codes a
/// single-node and a cluster read both answer.
pub(super) fn result_rows(
    group: Option<AggCell>,
    result: AggregateResult,
) -> Result<Vec<AggRow>, ErrorCode> {
    let with_group = |mut row: AggRow| {
        if let Some(g) = &group {
            row.insert("group", g.clone());
        }
        row
    };
    Ok(match result.finalize().map_err(finalize_error)? {
        AggOutput::Scalar(v) => {
            let cell = match v {
                Some(AggScalar::Int(i)) => AggCell::Int(i),
                Some(AggScalar::Float(f)) => AggCell::Float(f),
                None => AggCell::Null,
            };
            vec![with_group(AggRow::from([("result", cell)]))]
        }
        AggOutput::Bins(bins) => bins
            .into_iter()
            .map(|bin| {
                let count =
                    i64::try_from(bin.count).map_err(|_| ErrorCode::NumericValueOutOfRange {
                        detail: format!(
                            "array aggregate: bin count {} is out of range for bigint",
                            bin.count
                        ),
                    })?;
                Ok(with_group(AggRow::from([
                    ("bin_lo", AggCell::Float(bin.lo)),
                    ("bin_hi", AggCell::Float(bin.hi)),
                    ("count", AggCell::Int(count)),
                ])))
            })
            .collect::<Result<_, ErrorCode>>()?,
    })
}

/// The Data-Plane code of a finalize refusal.
fn finalize_error(e: ArrayError) -> ErrorCode {
    match e {
        ArrayError::AggregateBudgetExceeded { .. } => ErrorCode::ProgramLimitExceeded {
            detail: format!("array aggregate: {e}"),
        },
        other => ErrorCode::NumericValueOutOfRange {
            detail: format!("array aggregate: {other}"),
        },
    }
}

/// The engine reducer of a plan reducer. A malformed histogram spec is a
/// bad request.
pub(super) fn map_reducer(r: ArrayReducer) -> Result<Reducer, ErrorCode> {
    r.engine().map_err(|e| ErrorCode::BadRequest {
        detail: format!("array aggregate: {e}"),
    })
}

pub(super) fn unwrap_sparse(t: TilePayload) -> Result<SparseTile, ErrorCode> {
    match t {
        TilePayload::Sparse(s) => Ok(s),
        TilePayload::Dense(_) => Err(ErrorCode::Unsupported {
            detail: "dense tile payload in aggregate".to_string(),
        }),
    }
}

pub(super) fn apply_surrogate_filter(
    schema: &ArraySchema,
    tile: SparseTile,
    filter: Option<&SurrogateBitmap>,
) -> Result<SparseTile, ErrorCode> {
    let f = match filter {
        None => return Ok(tile),
        Some(f) => f,
    };
    let n = tile.row_count();
    let mut live_idx = 0usize;
    let mut b = SparseTileBuilder::new(schema);
    for row in 0..n {
        let kind = tile.row_kind(row).map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter row_kind: {e}"),
        })?;
        if kind != RowKind::Live {
            continue;
        }
        let attr_row = live_idx;
        live_idx += 1;
        // A stored live row always holds its bound surrogate.
        let sur = tile.live_surrogate(row).map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter: {e}"),
        })?;
        if !f.contains(sur) {
            continue;
        }
        let coord: Vec<_> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[attr_row].clone()).collect();
        let valid_from_ms = tile.valid_from_ms.get(row).copied().unwrap_or(0);
        let valid_until_ms = tile
            .valid_until_ms
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::OPEN_UPPER);
        b.push_row(SparseRow::live(
            &coord,
            &attrs,
            sur,
            valid_from_ms,
            valid_until_ms,
        ))
        .map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter: {e}"),
        })?;
    }
    Ok(b.build())
}

fn wire_number(n: AggNumber) -> ArrayAggNumber {
    match n {
        AggNumber::Int(v) => ArrayAggNumber::int(v),
        AggNumber::Float(v) => ArrayAggNumber::Float(v),
    }
}

/// The distributed partial of one reducer's result. The coordinator reads
/// the field its reducer names, so each reducer fills that field exactly.
pub(super) fn agg_result_to_partial(
    group_key: ArrayAggGroupKey,
    result: AggregateResult,
) -> ArrayAggPartial {
    match result {
        AggregateResult::Sum { value, count }
        | AggregateResult::Min { value, count }
        | AggregateResult::Max { value, count } => {
            let value = wire_number(value);
            ArrayAggPartial {
                group_key,
                count,
                sum: value,
                min: value,
                max: value,
                welford_mean: if count > 0 {
                    value.as_f64() / count as f64
                } else {
                    0.0
                },
                welford_m2: 0.0,
                dist: ValueDist::Empty,
                bins: Vec::new(),
            }
        }
        AggregateResult::Count { count } => {
            let n = ArrayAggNumber::int(i128::from(count));
            ArrayAggPartial {
                group_key,
                count,
                sum: n,
                min: n,
                max: n,
                welford_mean: if count > 0 { 1.0 } else { 0.0 },
                welford_m2: 0.0,
                dist: ValueDist::Empty,
                bins: Vec::new(),
            }
        }
        AggregateResult::Mean { sum, count } => {
            let mean = if count > 0 {
                sum.to_f64() / count as f64
            } else {
                0.0
            };
            ArrayAggPartial {
                group_key,
                count,
                sum: wire_number(sum),
                min: ArrayAggNumber::Float(mean),
                max: ArrayAggNumber::Float(mean),
                welford_mean: mean,
                welford_m2: 0.0,
                dist: ValueDist::Empty,
                bins: Vec::new(),
            }
        }
        AggregateResult::Dist { acc, .. } => {
            let dist = acc.into_dist();
            ArrayAggPartial {
                // An over-budget distribution counts no value, yet must not
                // merge as empty, or the refusal is lost.
                count: dist.count().max(dist.distinct()),
                dist,
                ..ArrayAggPartial::empty(group_key)
            }
        }
        AggregateResult::Moments { moments, .. } => ArrayAggPartial {
            count: moments.count,
            welford_mean: moments.mean,
            welford_m2: moments.m2,
            ..ArrayAggPartial::empty(group_key)
        },
        AggregateResult::Histogram { counts, .. } => ArrayAggPartial {
            count: counts.iter().sum(),
            bins: counts,
            ..ArrayAggPartial::empty(group_key)
        },
        AggregateResult::Empty(_) => ArrayAggPartial::empty(group_key),
    }
}

/// The typed group key a distributed partial carries for dim value `c`.
pub(super) fn coord_to_group_key(c: &CoordValue) -> ArrayAggGroupKey {
    match c {
        CoordValue::Int64(v) => ArrayAggGroupKey::Int64(*v),
        CoordValue::TimestampMs(v) => ArrayAggGroupKey::TimestampMs(*v),
        CoordValue::Float64(v) => ArrayAggGroupKey::Float64(*v),
        CoordValue::String(s) => ArrayAggGroupKey::String(s.clone()),
    }
}

pub(super) fn encode_agg_rows(
    core: &CoreLoop,
    task: &ExecutionTask,
    rows: &[BTreeMap<&'static str, AggCell>],
) -> Response {
    let owned: Vec<&BTreeMap<&'static str, AggCell>> = rows.iter().collect();
    match zerompk::to_msgpack_vec(&owned) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("array aggregate encode: {e}"),
            },
        ),
    }
}

pub(super) fn encode_bitemporal_agg_partial(
    core: &CoreLoop,
    task: &ExecutionTask,
    partials: &[ArrayAggPartial],
    truncated_before_horizon: bool,
) -> Response {
    let owned: Vec<&ArrayAggPartial> = partials.iter().collect();
    match zerompk::to_msgpack_vec(&(&owned, truncated_before_horizon)) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("bitemporal aggregate encode: {e}"),
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `group` cell of each dim type decodes as the value a slice row
    /// holds for that dim. A timestamp dim reads as a naive instant.
    #[test]
    fn a_group_cell_takes_its_dim_type() {
        let decode = |c: &CoordValue| {
            let cell = coord_to_agg_cell(c).expect("cell");
            let bytes = zerompk::to_msgpack_vec(&cell).expect("encode");
            nodedb_types::value_from_msgpack(&bytes).expect("decode")
        };
        let ms = 1_768_435_200_000;
        let instant = NdbDateTime::from_millis(ms).expect("in range");
        assert_eq!(
            decode(&CoordValue::TimestampMs(ms)),
            nodedb_types::Value::NaiveDateTime(instant)
        );
        assert_eq!(
            decode(&CoordValue::Int64(7)),
            nodedb_types::Value::Integer(7)
        );
        assert_eq!(
            decode(&CoordValue::Float64(102.5)),
            nodedb_types::Value::Float(102.5)
        );
        assert_eq!(
            decode(&CoordValue::String("chrX".into())),
            nodedb_types::Value::String("chrX".into())
        );
    }
}

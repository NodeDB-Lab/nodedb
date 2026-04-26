//! `ArrayOp::Aggregate` handler.
//!
//! Cross-tile reduction with optional group-by-dim. The tile-local
//! reducers in `nodedb-array::query::aggregate` produce
//! `AggregateResult` partials that merge exactly across tiles (Mean
//! carries `(sum, count)`); we fold them here and finalize once.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::query::aggregate::{
    AggregateResult, GroupAggregate, Reducer, aggregate_attr, group_by_dim,
};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::ArrayReducer;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Standard-msgpack-friendly cell value for aggregate rows. Encoded via
/// zerompk's `c_enum`-style tagging is unsuitable here (the pgwire
/// `decode_payload_to_json` transcoder needs untagged scalar shapes), so
/// we hand-roll a `ToMessagePack` impl that writes the inner scalar
/// directly — same wire shape as the previous `serde(untagged)` form.
enum AggCell {
    Float(f64),
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl zerompk::ToMessagePack for AggCell {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggCell::Float(f) => writer.write_f64(*f),
            AggCell::Int(i) => writer.write_i64(*i),
            AggCell::Str(s) => writer.write_string(s),
            AggCell::Bool(b) => writer.write_boolean(*b),
            AggCell::Null => writer.write_nil(),
        }
    }
}

fn coord_to_agg_cell(c: &CoordValue) -> AggCell {
    match c {
        CoordValue::Int64(v) | CoordValue::TimestampMs(v) => AggCell::Int(*v),
        CoordValue::Float64(v) => AggCell::Float(*v),
        CoordValue::String(v) => AggCell::Str(v.clone()),
    }
}

fn float_or_null(v: Option<f64>) -> AggCell {
    match v {
        Some(f) => AggCell::Float(f),
        None => AggCell::Null,
    }
}

/// Aggregate query parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct AggParams<'a> {
    pub array_id: &'a ArrayId,
    pub attr_idx: u32,
    pub reducer: ArrayReducer,
    pub group_by_dim_idx: i32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    pub return_partial: bool,
    /// Optional Hilbert-prefix range `[lo, hi]` for shard-level partitioning.
    /// When set, only tiles whose Hilbert prefix falls within this range
    /// contribute to the aggregate. Used by the distributed shard handler to
    /// prevent double-counting when all vShards share a single Data Plane.
    pub hilbert_range: Option<(u64, u64)>,
    /// Bitemporal system-time cutoff. `None` = live read.
    pub system_as_of: Option<i64>,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_aggregate(
        &mut self,
        task: &ExecutionTask,
        p: AggParams<'_>,
    ) -> Response {
        let AggParams {
            array_id,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            hilbert_range,
            system_as_of,
            valid_at_ms,
        } = p;
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }

        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        // Bitemporal path.
        if system_as_of.is_some() || valid_at_ms.is_some() {
            let cutoff = system_as_of.unwrap_or(i64::MAX);
            let store = match self.array_engine.store(array_id) {
                Ok(s) => s,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array '{}' not open: {e}", array_id.name),
                        },
                    );
                }
            };
            let (resolved_tiles, truncated_before_horizon) =
                match store.scan_tiles_at(cutoff, valid_at_ms) {
                    Ok(r) => r,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array bitemporal aggregate scan: {e}"),
                            },
                        );
                    }
                };

            let r = map_reducer(reducer);
            let attr = attr_idx as usize;

            let all_tiles_resolved: Vec<TilePayload> = resolved_tiles
                .into_iter()
                .filter(|(hp, _)| match hilbert_range {
                    Some((lo, hi)) => *hp >= lo && *hp <= hi,
                    None => true,
                })
                .map(|(_, tile)| TilePayload::Sparse(tile))
                .collect();

            if group_by_dim_idx < 0 {
                let mut acc: Option<AggregateResult> = None;
                for tile in all_tiles_resolved {
                    let sparse = match unwrap_sparse(tile) {
                        Ok(s) => s,
                        Err(code) => return self.response_error(task, code),
                    };
                    let sparse = match apply_surrogate_filter(&schema, sparse, cell_filter) {
                        Ok(s) => s,
                        Err(code) => return self.response_error(task, code),
                    };
                    let part = aggregate_attr(&sparse, attr, r);
                    acc = Some(match acc {
                        Some(prev) => prev.merge(part),
                        None => part,
                    });
                }
                if return_partial {
                    let partial = acc.map(|a| agg_result_to_partial(0, a)).unwrap_or_else(|| {
                        ArrayAggPartial {
                            group_key: 0,
                            count: 0,
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            welford_mean: 0.0,
                            welford_m2: 0.0,
                        }
                    });
                    return encode_bitemporal_agg_partial(
                        self,
                        task,
                        &[partial],
                        truncated_before_horizon,
                    );
                }
                let final_val = acc.and_then(|a| a.finalize());
                let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
                row.insert("result", float_or_null(final_val));
                row.insert(
                    "truncated_before_horizon",
                    AggCell::Bool(truncated_before_horizon),
                );
                return encode_agg_rows(self, task, &[row]);
            }

            let dim = group_by_dim_idx as usize;
            let mut order: Vec<CoordValue> = Vec::new();
            let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
            for tile in all_tiles_resolved {
                let sparse = match unwrap_sparse(tile) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let sparse = match apply_surrogate_filter(&schema, sparse, cell_filter) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let groups: Vec<GroupAggregate> = group_by_dim(&sparse, dim, attr, r);
                for g in groups {
                    match by_key.get_mut(&g.key) {
                        Some(prev) => *prev = prev.merge(g.result),
                        None => {
                            order.push(g.key.clone());
                            by_key.insert(g.key, g.result);
                        }
                    }
                }
            }
            if return_partial {
                let partials: Vec<ArrayAggPartial> = order
                    .iter()
                    .filter_map(|key| {
                        by_key
                            .remove(key)
                            .map(|agg| agg_result_to_partial(coord_to_group_key(key), agg))
                    })
                    .collect();
                return encode_bitemporal_agg_partial(
                    self,
                    task,
                    &partials,
                    truncated_before_horizon,
                );
            }
            let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::with_capacity(order.len());
            for key in order {
                let result_val = by_key.remove(&key).and_then(|r| r.finalize());
                let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
                row.insert("group", coord_to_agg_cell(&key));
                row.insert("result", float_or_null(result_val));
                rows.push(row);
            }
            let mut summary: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            summary.insert(
                "truncated_before_horizon",
                AggCell::Bool(truncated_before_horizon),
            );
            rows.push(summary);
            return encode_agg_rows(self, task, &rows);
        }

        let all_tiles_with_prefix = match self
            .array_engine
            .scan_tiles_with_hilbert_prefix(array_id, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array aggregate scan: {e}"),
                    },
                );
            }
        };

        // Apply per-shard Hilbert-range pre-filter when set. This prevents
        // double-counting in harnesses where all vShards share one Data Plane.
        let all_tiles: Vec<TilePayload> = match hilbert_range {
            Some((lo, hi)) => all_tiles_with_prefix
                .into_iter()
                .filter_map(|(hp, tile)| {
                    if hp >= lo && hp <= hi {
                        Some(tile)
                    } else {
                        None
                    }
                })
                .collect(),
            None => all_tiles_with_prefix
                .into_iter()
                .map(|(_, tile)| tile)
                .collect(),
        };

        let r = map_reducer(reducer);
        let attr = attr_idx as usize;

        if group_by_dim_idx < 0 {
            // Scalar fold across all tiles.
            let mut acc: Option<AggregateResult> = None;
            for tile in all_tiles {
                let sparse = match unwrap_sparse(tile) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let sparse = match apply_surrogate_filter(&schema, sparse, cell_filter) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let part = aggregate_attr(&sparse, attr, r);
                acc = Some(match acc {
                    Some(prev) => prev.merge(part),
                    None => part,
                });
            }
            if return_partial {
                let partial =
                    acc.map(|a| agg_result_to_partial(0, a))
                        .unwrap_or_else(|| ArrayAggPartial {
                            group_key: 0,
                            count: 0,
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                            welford_mean: 0.0,
                            welford_m2: 0.0,
                        });
                return encode_partials(self, task, &[partial]);
            }
            let final_val = acc.and_then(|a| a.finalize());
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("result", float_or_null(final_val));
            return encode_agg_rows(self, task, &[row]);
        }

        // Group-by fold. Preserve first-seen group order across tiles.
        let dim = group_by_dim_idx as usize;
        let mut order: Vec<CoordValue> = Vec::new();
        let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
        for tile in all_tiles {
            let sparse = match unwrap_sparse(tile) {
                Ok(s) => s,
                Err(code) => return self.response_error(task, code),
            };
            let sparse = match apply_surrogate_filter(&schema, sparse, cell_filter) {
                Ok(s) => s,
                Err(code) => return self.response_error(task, code),
            };
            let groups: Vec<GroupAggregate> = group_by_dim(&sparse, dim, attr, r);
            for g in groups {
                match by_key.get_mut(&g.key) {
                    Some(prev) => *prev = prev.merge(g.result),
                    None => {
                        order.push(g.key.clone());
                        by_key.insert(g.key, g.result);
                    }
                }
            }
        }

        if return_partial {
            let partials: Vec<ArrayAggPartial> = order
                .iter()
                .filter_map(|key| {
                    by_key
                        .remove(key)
                        .map(|agg| agg_result_to_partial(coord_to_group_key(key), agg))
                })
                .collect();
            return encode_partials(self, task, &partials);
        }

        let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::with_capacity(order.len());
        for key in order {
            let result_val = by_key.remove(&key).and_then(|r| r.finalize());
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("group", coord_to_agg_cell(&key));
            row.insert("result", float_or_null(result_val));
            rows.push(row);
        }
        encode_agg_rows(self, task, &rows)
    }
}

fn map_reducer(r: ArrayReducer) -> Reducer {
    match r {
        ArrayReducer::Sum => Reducer::Sum,
        ArrayReducer::Count => Reducer::Count,
        ArrayReducer::Min => Reducer::Min,
        ArrayReducer::Max => Reducer::Max,
        ArrayReducer::Mean => Reducer::Mean,
    }
}

fn unwrap_sparse(t: TilePayload) -> Result<SparseTile, ErrorCode> {
    match t {
        TilePayload::Sparse(s) => Ok(s),
        TilePayload::Dense(_) => Err(ErrorCode::Unsupported {
            detail: "dense tile payload in aggregate".to_string(),
        }),
    }
}

/// Return a new tile containing only the rows whose surrogate is present in
/// `filter`. When `filter` is `None` the original tile is returned unchanged.
fn apply_surrogate_filter(
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
        let sur = tile
            .surrogates
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::Surrogate::ZERO);
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
        b.push_row(SparseRow {
            coord: &coord,
            attrs: &attrs,
            surrogate: sur,
            valid_from_ms,
            valid_until_ms,
            kind: RowKind::Live,
        })
        .map_err(|e| ErrorCode::Internal {
            detail: format!("array surrogate filter: {e}"),
        })?;
    }
    Ok(b.build())
}

/// Convert a local `AggregateResult` into the wire-safe `ArrayAggPartial`
/// understood by the cluster coordinator. The `group_key` is an `i64`
/// encoding of the group-by dimension value (see `coord_to_group_key`).
fn agg_result_to_partial(group_key: i64, result: AggregateResult) -> ArrayAggPartial {
    match result {
        AggregateResult::Sum { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Count { count } => ArrayAggPartial {
            group_key,
            count,
            sum: count as f64,
            min: count as f64,
            max: count as f64,
            welford_mean: if count > 0 { 1.0 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Min { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Max { value, count } => ArrayAggPartial {
            group_key,
            count,
            sum: value,
            min: value,
            max: value,
            welford_mean: if count > 0 { value / count as f64 } else { 0.0 },
            welford_m2: 0.0,
        },
        AggregateResult::Mean { sum, count } => {
            let mean = if count > 0 { sum / count as f64 } else { 0.0 };
            ArrayAggPartial {
                group_key,
                count,
                sum,
                min: mean,
                max: mean,
                welford_mean: mean,
                welford_m2: 0.0,
            }
        }
        AggregateResult::Empty(_) => ArrayAggPartial {
            group_key,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            welford_mean: 0.0,
            welford_m2: 0.0,
        },
    }
}

/// Map a `CoordValue` to a stable `i64` group key for use in `ArrayAggPartial`.
///
/// Int64 and TimestampMs values are used directly. Float64 is bit-cast.
/// String values are hashed to a `u64` and reinterpreted as `i64` — collisions
/// are theoretically possible but rare; the coordinator merges by this key, not
/// by the original string, so groups that collide would be incorrectly merged.
/// In practice string group-by dims use low-cardinality category values, making
/// collisions extremely unlikely. A future version could use string interning or
/// a wider key type.
fn coord_to_group_key(c: &CoordValue) -> i64 {
    match c {
        CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v,
        CoordValue::Float64(v) => v.to_bits() as i64,
        CoordValue::String(s) => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            s.hash(&mut h);
            h.finish() as i64
        }
    }
}

/// Encode a slice of `ArrayAggPartial` as a zerompk array payload.
///
/// The coordinator decodes this payload on the other side of the SPSC bridge
/// via `exec_agg` in `DataPlaneArrayExecutor`.
fn encode_partials(
    core: &CoreLoop,
    task: &ExecutionTask,
    partials: &[ArrayAggPartial],
) -> Response {
    let owned: Vec<&ArrayAggPartial> = partials.iter().collect();
    match zerompk::to_msgpack_vec(&owned) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("array aggregate partial encode: {e}"),
            },
        ),
    }
}

fn encode_agg_rows(
    core: &CoreLoop,
    task: &ExecutionTask,
    rows: &[BTreeMap<&'static str, AggCell>],
) -> Response {
    // zerompk map encoding — pgwire `decode_payload_to_json` transcodes
    // this directly to a clean JSON array of objects.
    // zerompk's blanket impls cover `Vec<T>` and `BTreeMap<K, V>` but
    // not `&[T]`; clone into a Vec ref so the call resolves.
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

/// Encode a bitemporal aggregate partial response as `(partials, truncated)`.
fn encode_bitemporal_agg_partial(
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

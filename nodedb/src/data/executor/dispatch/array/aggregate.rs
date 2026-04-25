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
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
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
    Null,
}

impl zerompk::ToMessagePack for AggCell {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggCell::Float(f) => writer.write_f64(*f),
            AggCell::Int(i) => writer.write_i64(*i),
            AggCell::Str(s) => writer.write_string(s),
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

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_aggregate(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        attr_idx: u32,
        reducer: ArrayReducer,
        group_by_dim_idx: i32,
        cell_filter: Option<&SurrogateBitmap>,
    ) -> Response {
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }

        let tiles = match self
            .array_engine
            .scan_tiles(array_id, &MbrQueryPredicate::default())
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

        let r = map_reducer(reducer);
        let attr = attr_idx as usize;

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

        if group_by_dim_idx < 0 {
            // Scalar fold across all tiles.
            let mut acc: Option<AggregateResult> = None;
            for tile in tiles {
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
            let final_val = acc.and_then(|a| a.finalize());
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("result", float_or_null(final_val));
            return encode_agg_rows(self, task, &[row]);
        }

        // Group-by fold. Preserve first-seen group order across tiles.
        let dim = group_by_dim_idx as usize;
        let mut order: Vec<CoordValue> = Vec::new();
        let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
        for tile in tiles {
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
    let n = tile.nnz() as usize;
    let mut b = SparseTileBuilder::new(schema);
    for row in 0..n {
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
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[row].clone()).collect();
        b.push_with_surrogate(&coord, &attrs, sur)
            .map_err(|e| ErrorCode::Internal {
                detail: format!("array surrogate filter: {e}"),
            })?;
    }
    Ok(b.build())
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

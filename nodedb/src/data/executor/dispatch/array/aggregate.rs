// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Aggregate` handler.
//!
//! Cross-tile reduction with optional group-by-dim. The tile-local
//! reducers in `nodedb-array::query::aggregate` produce
//! `AggregateResult` partials that merge exactly across tiles (Mean
//! carries `(sum, count)`); we fold them here and finalize once.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::query::aggregate::{GroupAggregate, aggregate_attr, group_by_dim};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TilePayload;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ArrayOverlayMergeParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;
use nodedb_physical::physical_plan::ArrayReducer;

use super::aggregate_helpers::{
    AggCell, agg_result_to_partial, apply_surrogate_filter, coord_to_agg_cell, coord_to_group_key,
    encode_agg_rows, encode_bitemporal_agg_partial, float_or_null, map_reducer, unwrap_sparse,
};

/// Aggregate query parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct AggParams<'a> {
    pub array_id: &'a ArrayId,
    pub attr_idx: u32,
    pub reducer: ArrayReducer,
    pub group_by_dim_idx: i32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    pub return_partial: bool,
    /// Optional Hilbert-prefix range `[lo, hi]` for shard-level partitioning.
    pub hilbert_range: Option<(u64, u64)>,
    /// Bitemporal system-time cutoff. `None` = live read.
    pub system_as_of: Option<i64>,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

/// Bundled inputs for [`CoreLoop::collect_agg_tiles`].
struct AggTileScan<'a> {
    array_id: &'a ArrayId,
    schema: &'a ArraySchema,
    hilbert_range: Option<(u64, u64)>,
    system_as_of: Option<i64>,
    valid_at_ms: Option<i64>,
    /// The transaction whose staged cells merge into the scan, or `None`
    /// for an autocommit read, a distributed partial, or a temporal read.
    overlay_txn: Option<TxnId>,
}

/// Bundled inputs for [`CoreLoop::reduce_and_encode_agg`] — keeps the kernel
/// to a single argument and avoids the 7-argument clippy lint.
struct AggEmit<'a> {
    task: &'a ExecutionTask,
    schema: &'a ArraySchema,
    all_tiles: Vec<TilePayload>,
    attr_idx: u32,
    reducer: ArrayReducer,
    group_by_dim_idx: i32,
    cell_filter: Option<&'a SurrogateBitmap>,
    return_partial: bool,
    /// Below-horizon signal computed by the tile scan (always `false` for
    /// non-temporal current-state reads).
    truncated_before_horizon: bool,
    /// When `true`, surface `truncated_before_horizon` as a trailing
    /// `{"truncated_before_horizon": bool}` summary row. Only set for temporal
    /// queries — the signal is meaningless for current-state reads, and
    /// emitting it there would change the long-standing non-temporal row shape.
    emit_horizon: bool,
}

fn hilbert_prefix_in_range(hp: u64, range: Option<(u64, u64)>) -> bool {
    match range {
        Some((lo, hi)) => hp >= lo && hp <= hi,
        None => true,
    }
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

        // Resolve the tile set + below-horizon signal uniformly across temporal
        // and current-state reads. The two differ only in how tiles are sourced;
        // the reduce/encode kernel below is identical for both.
        let temporal = system_as_of.is_some() || valid_at_ms.is_some();
        // Read-your-own-writes folds the transaction's overlay into this
        // shard's result, for the finalizing single-node read and for the
        // distributed partial alike. Each shard's overlay holds only the
        // cells partitioned to that shard, and `collect_agg_tiles` applies
        // the shard's `hilbert_range` to overlay tiles as to base tiles, so
        // folding them into the shard's partial counts every staged cell
        // exactly once at the coordinator's merge. `AS OF SYSTEM TIME` is a
        // snapshot of committed history and skips the overlay.
        let overlay_txn = if system_as_of.is_some() {
            None
        } else {
            task.request.txn_id
        };
        let (all_tiles, truncated_before_horizon) = match self.collect_agg_tiles(AggTileScan {
            array_id,
            schema: &schema,
            hilbert_range,
            system_as_of,
            valid_at_ms,
            overlay_txn,
        }) {
            Ok(v) => v,
            Err(detail) => {
                return self.response_error(task, ErrorCode::Internal { detail });
            }
        };

        self.reduce_and_encode_agg(AggEmit {
            task,
            schema: &schema,
            all_tiles,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            truncated_before_horizon,
            emit_horizon: temporal,
        })
    }

    /// Gather the tiles an aggregate must reduce over, plus the below-horizon
    /// flag, via the cell-ceiling resolver `scan_tiles_at`.
    ///
    /// Live reads (no `AS OF`) resolve at the open horizon `i64::MAX`, so each
    /// cell contributes exactly once at its latest version — identical to the
    /// slice path's `Current` handling. Scanning *raw* tile versions here would
    /// double-count any cell overwritten across segments in a bitemporal array
    /// (e.g. v1 in one sealed segment, v2 in another). `system_as_of`/
    /// `valid_at_ms` narrow the cutoff for point-in-time queries; the same
    /// `hilbert_range` shard filter applies in all cases.
    ///
    /// Returns an error *detail* string (wrapped into `ErrorCode::Internal` by
    /// the caller) rather than a `Response`, so it can borrow `&self` cleanly.
    fn collect_agg_tiles(&self, scan: AggTileScan<'_>) -> Result<(Vec<TilePayload>, bool), String> {
        let AggTileScan {
            array_id,
            schema,
            hilbert_range,
            system_as_of,
            valid_at_ms,
            overlay_txn,
        } = scan;
        let cutoff = system_as_of.unwrap_or(i64::MAX);
        let store = self
            .array_engine
            .store(array_id)
            .map_err(|e| format!("array '{}' not open: {e}", array_id.name))?;
        let (mut resolved_tiles, truncated_before_horizon) = store
            .scan_tiles_at(cutoff, valid_at_ms)
            .map_err(|e| format!("array aggregate scan: {e}"))?;
        // Staged cells join the SAME tile set the base cells reduce through,
        // ahead of the shard-range filter, so they fold into the same partial
        // accumulator under the same surrogate filter.
        self.merge_array_overlay_tiles(
            ArrayOverlayMergeParams {
                txn_id: overlay_txn,
                array_id,
                schema,
                valid_at_ms,
            },
            &mut resolved_tiles,
        )
        .map_err(|e| format!("array aggregate overlay merge: {e}"))?;
        let tiles = resolved_tiles
            .into_iter()
            .filter(|(hp, _)| hilbert_prefix_in_range(*hp, hilbert_range))
            .map(|(_, tile)| TilePayload::Sparse(tile))
            .collect();
        Ok((tiles, truncated_before_horizon))
    }

    /// Reduce the resolved tiles into a scalar or grouped aggregate and encode
    /// the response. Shared by current-state and temporal queries so the wire
    /// shape can never diverge between them.
    ///
    /// - `return_partial` (distributed shards): emits the
    ///   `(Vec<ArrayAggPartial>, truncated_before_horizon)` tuple via
    ///   `encode_bitemporal_agg_partial` — the single partial wire shape the
    ///   cluster `exec_agg` decodes.
    /// - otherwise: emits finalized `{"result"}` / `{"group","result"}` rows,
    ///   plus a trailing `{"truncated_before_horizon": bool}` summary row when
    ///   `emit_horizon` is set (temporal queries only). The cluster
    ///   `finalize_agg_partials` produces this exact same shape.
    fn reduce_and_encode_agg(&self, e: AggEmit<'_>) -> Response {
        let AggEmit {
            task,
            schema,
            all_tiles,
            attr_idx,
            reducer,
            group_by_dim_idx,
            cell_filter,
            return_partial,
            truncated_before_horizon,
            emit_horizon,
        } = e;

        let r = map_reducer(reducer);
        let attr = attr_idx as usize;

        if group_by_dim_idx < 0 {
            let mut acc = None;
            for tile in all_tiles {
                let sparse = match unwrap_sparse(tile) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let sparse = match apply_surrogate_filter(schema, sparse, cell_filter) {
                    Ok(s) => s,
                    Err(code) => return self.response_error(task, code),
                };
                let part = aggregate_attr(&sparse, attr, r);
                acc = Some(match acc {
                    Some(prev) => {
                        nodedb_array::query::aggregate::AggregateResult::merge(prev, part)
                    }
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
                return encode_bitemporal_agg_partial(
                    self,
                    task,
                    &[partial],
                    truncated_before_horizon,
                );
            }
            let final_val = acc.and_then(|a| a.finalize());
            let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::new();
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("result", float_or_null(final_val));
            rows.push(row);
            push_horizon_summary(&mut rows, emit_horizon, truncated_before_horizon);
            return encode_agg_rows(self, task, &rows);
        }

        let dim = group_by_dim_idx as usize;
        let mut order: Vec<CoordValue> = Vec::new();
        let mut by_key: HashMap<CoordValue, nodedb_array::query::aggregate::AggregateResult> =
            HashMap::new();
        for tile in all_tiles {
            let sparse = match unwrap_sparse(tile) {
                Ok(s) => s,
                Err(code) => return self.response_error(task, code),
            };
            let sparse = match apply_surrogate_filter(schema, sparse, cell_filter) {
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
            return encode_bitemporal_agg_partial(self, task, &partials, truncated_before_horizon);
        }

        let mut rows: Vec<BTreeMap<&'static str, AggCell>> = Vec::with_capacity(order.len() + 1);
        for key in order {
            let result_val = by_key.remove(&key).and_then(|r| r.finalize());
            let mut row: BTreeMap<&'static str, AggCell> = BTreeMap::new();
            row.insert("group", coord_to_agg_cell(&key));
            row.insert("result", float_or_null(result_val));
            rows.push(row);
        }
        push_horizon_summary(&mut rows, emit_horizon, truncated_before_horizon);
        encode_agg_rows(self, task, &rows)
    }
}

/// Append the trailing `{"truncated_before_horizon": bool}` summary row when
/// `emit_horizon` is set. No-op otherwise so non-temporal aggregates keep their
/// long-standing row shape.
fn push_horizon_summary(
    rows: &mut Vec<BTreeMap<&'static str, AggCell>>,
    emit_horizon: bool,
    truncated_before_horizon: bool,
) {
    if !emit_horizon {
        return;
    }
    let mut summary: BTreeMap<&'static str, AggCell> = BTreeMap::new();
    summary.insert(
        "truncated_before_horizon",
        AggCell::Bool(truncated_before_horizon),
    );
    rows.push(summary);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_types::Surrogate;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::*;
    use nodedb_physical::physical_plan::{ArrayOp, ArrayReducer};

    use super::*;

    fn make_request(plan: PhysicalPlan, id: u64) -> Request {
        Request {
            request_id: RequestId::new(id),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        }
    }

    fn schema_2d_f64(name: &str) -> ArraySchema {
        ArraySchemaBuilder::new(name)
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap()
    }

    struct Harness {
        core: CoreLoop,
        req_tx: Producer<BridgeRequest>,
        resp_rx: Consumer<BridgeResponse>,
        next_id: u64,
        _dir: tempfile::TempDir,
    }

    impl Harness {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
            let core = CoreLoop::open(
                0,
                req_rx,
                resp_tx,
                dir.path(),
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            )
            .unwrap();
            Harness {
                core,
                req_tx,
                resp_rx,
                next_id: 1,
                _dir: dir,
            }
        }

        fn send(&mut self, op: ArrayOp) -> crate::bridge::envelope::Response {
            self.send_plan(PhysicalPlan::Array(op))
        }

        fn send_plan(&mut self, plan: PhysicalPlan) -> crate::bridge::envelope::Response {
            let id = self.next_id;
            self.next_id += 1;
            self.req_tx
                .try_push(BridgeRequest::unfloored(make_request(plan, id)))
                .unwrap();
            self.core.tick();
            let resp = self.resp_rx.try_pop().unwrap();
            resp.inner
        }

        fn open(&mut self, aid: &ArrayId, schema: &ArraySchema, schema_hash: u64) {
            let bytes = zerompk::to_msgpack_vec(schema).unwrap();
            let r = self.send(ArrayOp::OpenArray {
                array_id: aid.clone(),
                schema_msgpack: bytes,
                schema_hash,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            });
            assert_eq!(r.status, Status::Ok, "open failed: {r:?}");
        }

        fn put(&mut self, aid: &ArrayId, cells: Vec<ArrayPutCell>, lsn: u64) {
            let bytes = zerompk::to_msgpack_vec(&cells).unwrap();
            let r = self.send(ArrayOp::Put {
                array_id: aid.clone(),
                cells_msgpack: bytes,
                wal_lsn: lsn,
                provenance: None,
            });
            assert_eq!(r.status, Status::Ok, "put failed: {r:?}");
        }

        fn flush(&mut self, aid: &ArrayId) {
            let r = self.send(ArrayOp::Flush {
                array_id: aid.clone(),
                wal_lsn: 0,
            });
            assert_eq!(r.status, Status::Ok, "flush failed: {r:?}");
        }
    }

    fn cell(x: i64, y: i64, v: f64) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn cell_sur(x: i64, y: i64, v: f64, sur: u32) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: Surrogate(sur),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn decode_agg_rows(bytes: &[u8]) -> Vec<std::collections::BTreeMap<String, serde_json::Value>> {
        // Aggregate payloads are zerompk maps; transcode to JSON via the
        // shared msgpack→JSON streamer (same path pgwire uses), then parse.
        let json = nodedb_types::msgpack_to_json_string(bytes).expect("agg msgpack→json");
        serde_json::from_str(&json).expect("agg json parse")
    }

    #[test]
    fn aggregate_sum_scalar_across_multiple_tiles() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_agg");
        let aid = ArrayId::new(TenantId::new(1), "t6_agg");
        h.open(&aid, &s, 0xA2);

        // Two batches forced into separate sealed segments via Flush.
        h.put(&aid, vec![cell(0, 0, 1.0), cell(1, 1, 2.0)], 1);
        h.flush(&aid);
        h.put(&aid, vec![cell(2, 2, 3.0), cell(3, 3, 4.0)], 2);
        h.flush(&aid);

        let r = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: false,
            hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "agg failed: {r:?}");
        let rows = decode_agg_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 1);
        let f = rows[0]
            .get("result")
            .and_then(|v| v.as_f64())
            .expect("result f64");
        assert!((f - 10.0).abs() < 1e-9, "sum got {f}");
    }

    #[test]
    fn aggregate_return_partial_emits_tuple_wire_shape() {
        // Distributed shard aggregates set `return_partial = true`. The Data Plane
        // MUST emit the `(partials, truncated_before_horizon)` tuple — the same
        // shape the bitemporal path uses — so the cluster `exec_agg` (which always
        // decodes a tuple) can read non-temporal aggregates too. A bare `Vec` here
        // (the old `encode_partials` shape) would fail that decode.
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_partial");
        let aid = ArrayId::new(TenantId::new(1), "t6_partial");
        h.open(&aid, &s, 0xA9);
        h.put(&aid, vec![cell(0, 0, 1.0), cell(1, 1, 2.0)], 1);

        let r = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: true,
            hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "agg failed: {r:?}");

        // Must decode as a 2-tuple (partials, flag), NOT a bare Vec.
        let (partials, truncated): (
            Vec<nodedb_cluster::distributed_array::merge::ArrayAggPartial>,
            bool,
        ) = zerompk::from_msgpack(r.payload.as_ref())
            .expect("return_partial payload must be a (Vec<ArrayAggPartial>, bool) tuple");
        assert_eq!(partials.len(), 1);
        assert!(
            (partials[0].sum - 3.0).abs() < 1e-9,
            "sum got {}",
            partials[0].sum
        );
        assert!(
            !truncated,
            "non-temporal current-state read is never below-horizon"
        );
    }

    #[test]
    fn aggregate_temporal_appends_horizon_summary_row() {
        // Temporal aggregates surface the below-horizon signal as a trailing
        // {"truncated_before_horizon": bool} summary row — the same shape the
        // cluster `finalize_agg_partials` produces. Non-temporal aggregates (tested
        // above) carry no such row, so the two modes stay distinguishable.
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_agg_ts");
        let aid = ArrayId::new(TenantId::new(1), "t6_agg_ts");
        h.open(&aid, &s, 0xAB);
        let mk = |x: i64, y: i64, v: f64, sys: i64| ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: Surrogate::ZERO,
            system_from_ms: sys,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        };
        h.put(&aid, vec![mk(0, 0, 10.0, 100), mk(1, 1, 20.0, 100)], 1);
        h.flush(&aid);

        // AS OF after the writes: both cells visible, not below horizon.
        let r = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: false,
            hilbert_range: None,
            system_as_of: Some(150),
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "temporal agg failed: {r:?}");
        let rows = decode_agg_rows(r.payload.as_ref());
        assert_eq!(
            rows.len(),
            2,
            "temporal scalar agg = result row + summary row"
        );
        let sum = rows[0]
            .get("result")
            .and_then(|v| v.as_f64())
            .expect("result");
        assert!((sum - 30.0).abs() < 1e-9, "sum got {sum}");
        assert_eq!(
            rows[1]
                .get("truncated_before_horizon")
                .and_then(|v| v.as_bool()),
            Some(false),
            "cutoff after all data is not below-horizon"
        );

        // AS OF before the writes: below horizon → trailing flag is true.
        let r2 = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: None,
            return_partial: false,
            hilbert_range: None,
            system_as_of: Some(50),
            valid_at_ms: None,
        });
        assert_eq!(r2.status, Status::Ok, "below-horizon agg failed: {r2:?}");
        let rows2 = decode_agg_rows(r2.payload.as_ref());
        assert_eq!(
            rows2
                .last()
                .and_then(|row| row.get("truncated_before_horizon"))
                .and_then(|v| v.as_bool()),
            Some(true),
            "cutoff before all data must report below-horizon"
        );
    }

    #[test]
    fn aggregate_group_by_dim_buckets_per_x() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_grp");
        let aid = ArrayId::new(TenantId::new(1), "t6_grp");
        h.open(&aid, &s, 0xA3);
        // Two cells per x-row across two tiles.
        h.put(&aid, vec![cell(0, 0, 1.0), cell(0, 1, 2.0)], 1);
        h.flush(&aid);
        h.put(&aid, vec![cell(1, 0, 10.0), cell(1, 1, 20.0)], 2);
        h.flush(&aid);

        let r = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: 0,
            cell_filter: None,
            return_partial: false,
            hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "group agg failed: {r:?}");
        let rows = decode_agg_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 2);
        let mut totals: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
        for row in rows {
            let g = row
                .get("group")
                .and_then(|v| v.as_i64())
                .expect("group i64");
            let r = row
                .get("result")
                .and_then(|v| v.as_f64())
                .expect("result f64");
            totals.insert(g, r);
        }
        assert!((totals[&0] - 3.0).abs() < 1e-9);
        assert!((totals[&1] - 30.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_cell_filter_excludes_non_member_surrogates() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_sf_agg");
        let aid = ArrayId::new(TenantId::new(1), "t6_sf_agg");
        h.open(&aid, &s, 0xC2);
        // Four cells; surrogates 1..=4.
        h.put(
            &aid,
            vec![
                cell_sur(0, 0, 1.0, 1),
                cell_sur(1, 1, 2.0, 2),
                cell_sur(2, 2, 4.0, 3),
                cell_sur(3, 3, 8.0, 4),
            ],
            1,
        );
        h.flush(&aid);

        // Allow only surrogates 1 and 4 — sum should be 1+8=9, not 15.
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate(1));
        bm.insert(Surrogate(4));

        let r = h.send(ArrayOp::Aggregate {
            array_id: aid.clone(),
            attr_idx: 0,
            reducer: ArrayReducer::Sum,
            group_by_dim: -1,
            cell_filter: Some(bm),
            return_partial: false,
            hilbert_range: None,
            system_as_of: None,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "agg+filter failed: {r:?}");
        let rows = decode_agg_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 1);
        let f = rows[0]
            .get("result")
            .and_then(|v| v.as_f64())
            .expect("result f64");
        assert!((f - 9.0).abs() < 1e-9, "filtered sum got {f}");
    }
}

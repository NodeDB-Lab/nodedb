// SPDX-License-Identifier: BUSL-1.1

//! Distributed GROUP BY shuffle PRODUCER (`QueryOp::PartialAggregateState`).
//!
//! Accumulates the named collection's documents into per-group `GroupState`
//! accumulators exactly like the partial-aggregate path, but instead of
//! finalizing it emits ONE flat row PER GROUP:
//!
//! ```text
//! { <group_by[0]>: value_0, ..., "__agg_state": <bytes> }
//! ```
//!
//! where `__agg_state` carries the serialized partial `GroupState`. A
//! downstream [`crate::data::executor::handlers::aggregate::shuffle_merge`]
//! consumer merges these partial states from every producer and finalizes them.
//!
//! Rows are flat maps (NO `{id, data}` storage wrapper) so the consume side can
//! re-derive the group key from the row's own GROUP BY columns
//! byte-identically.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_types::Value;

/// The field name carrying a group's serialized partial `GroupState` in a
/// producer-emitted row. The consume side looks this exact key up.
pub(in crate::data::executor) const AGG_STATE_FIELD: &str = "__agg_state";

/// Borrowed inputs to [`CoreLoop::execute_partial_aggregate_state`]: the
/// source-doc selector (sub-plan or named collection) plus the GROUP BY /
/// aggregate / filter specs the producer accumulates over.
pub(in crate::data::executor) struct PartialAggregateStateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    /// The scope the per-shard scan of `collection` reads at. Ignored when
    /// `input` is set.
    pub system_time: nodedb_types::SystemTimeScope,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
}

impl CoreLoop {
    /// Execute a `PartialAggregateState` producer: acquire the source documents
    /// (the `input` sub-plan's rows when present, else a per-shard scan of the
    /// named `collection`), accumulate them, then emit one serialized
    /// partial-state row per group.
    pub(in crate::data::executor) fn execute_partial_aggregate_state(
        &mut self,
        params: PartialAggregateStateParams<'_>,
    ) -> Response {
        let PartialAggregateStateParams {
            task,
            tid,
            collection,
            input,
            system_time,
            group_by,
            aggregates,
            filters,
        } = params;
        // The rows come from the input sub-plan when present, else from a
        // per-shard scan of the named collection. Both read exactly as
        // `execute_aggregate` reads them: a child error or an undecodable
        // payload fails the producer.
        // A scope other than `Current` folds the engine's own scan at it.
        let docs = match input {
            Some(sub_plan) => self.input_docs(task, sub_plan),
            None if !system_time.is_current() => {
                let scan = match self.scoped_aggregate_scan(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    system_time,
                ) {
                    Ok(plan) => plan,
                    Err(e) => return self.response_error(task, ErrorCode::from(e)),
                };
                self.input_docs(task, &scan)
            }
            None => self.scan_for_aggregate(task, tid, collection),
        };
        let docs = match docs {
            Ok(docs) => docs,
            Err(resp) => return *resp,
        };

        // Plain GROUP BY: no sub-groups for the shuffle producer.
        let (groups, _sub) =
            match self.accumulate_groups(super::streaming::accumulate::AccumulateGroupsParams {
                docs: &docs,
                group_by,
                aggregates,
                filters,
                sub_group_by: &[],
                sub_aggregates: &[],
                database_id: task.request.database_id,
                tenant_id: task.request.tenant_id,
            }) {
                Ok(g) => g,
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            };

        let rows = match Self::partial_state_rows(groups, group_by) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        match crate::data::executor::response_codec::encode_value_vec(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Build the flat per-group rows `{<gb cols>, "__agg_state": <bytes>}` from
    /// the consolidated group map.
    ///
    /// The GROUP BY column values are recovered from the JSON-array group key
    /// (the same encoding `build_group_key` produces) so that the consume side,
    /// which re-derives the key from these same row fields via `build_group_key`,
    /// reconstructs a byte-identical key.
    fn partial_state_rows(
        groups: std::collections::HashMap<String, GroupState>,
        group_by: &[GroupKeySpec],
    ) -> crate::Result<Vec<Value>> {
        let mut rows: Vec<Value> = Vec::with_capacity(groups.len());
        for (group_key, state) in groups {
            let mut map: std::collections::HashMap<String, Value> =
                std::collections::HashMap::with_capacity(group_by.len() + 1);

            if !group_by.is_empty() {
                let parts: Vec<serde_json::Value> =
                    sonic_rs::from_str(&group_key).map_err(|e| crate::Error::Codec {
                        detail: format!("partial-state group key decode: {e}"),
                    })?;
                // One positional slot per key that contributed to the key bytes
                // (a bare `field` or a computed `expr`); re-emit each under its
                // `output_name` so the consume side can rebuild a byte-identical
                // key from these flat row fields.
                let mut part_idx = 0usize;
                for spec in group_by {
                    if spec.field.is_none() && spec.expr.is_none() {
                        continue;
                    }
                    let jv = parts
                        .get(part_idx)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    map.insert(spec.output_name.clone(), Value::from(jv));
                    part_idx += 1;
                }
            }

            // GroupState encodes as MessagePack, the same encoding
            // `GroupBySpiller` uses to persist it. NaN and ±Infinity stay exact.
            let state_bytes = zerompk::to_msgpack_vec(&state).map_err(|e| crate::Error::Codec {
                detail: format!("partial-state serialize: {e}"),
            })?;
            map.insert(AGG_STATE_FIELD.to_string(), Value::Bytes(state_bytes));

            rows.push(Value::Object(map));
        }
        Ok(rows)
    }
}

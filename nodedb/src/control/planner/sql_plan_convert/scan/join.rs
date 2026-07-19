// SPDX-License-Identifier: BUSL-1.1

//! Hash-join converter and the filter/condition merger and bitmap-hint plan
//! synthesis it depends on.

use nodedb_sql::planner::bitmap_emit::predicate::BitmapHint;
use nodedb_sql::types::SqlPlan;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{
    extract_join_projection_specs, extract_scan_alias, serialize_join_computed_projection,
};
use super::super::convert::convert_one;
use super::super::filter::{expr_filter_qualified, serialize_join_post_filters};
use super::super::scan_params::JoinPlanParams;
use super::super::value::sql_value_to_string;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Serialize a residual `ON` predicate separately from post-join `WHERE`
/// filters. Outer joins must decide whether a candidate matched using the ON
/// predicate before emitting a null-extended row; applying it as a WHERE
/// predicate would incorrectly discard that row.
fn serialize_join_condition(
    condition: &Option<nodedb_sql::types::SqlExpr>,
) -> crate::Result<Vec<u8>> {
    let Some(condition) = condition else {
        return Ok(Vec::new());
    };
    zerompk::to_msgpack_vec(&vec![expr_filter_qualified(condition)]).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("join condition serialization: {e}"),
        }
    })
}

fn shuffle_supports_join_tail(
    projection: &[JoinProjection],
    computed_projection: &[u8],
    join_filters: &[u8],
    post_filters: &[u8],
) -> bool {
    projection.is_empty()
        && computed_projection.is_empty()
        && join_filters.is_empty()
        && post_filters.is_empty()
}

/// Build a `PhysicalPlan` bitmap-producer sub-plan from a `BitmapHint`.
///
/// Returns `None` for hint shapes that cannot be represented as an
/// `IndexedFetch` (e.g. non-string primary values that have no reasonable
/// index-path encoding). The caller treats `None` as "no bitmap pushdown".
fn bitmap_hint_to_plan(hint: &BitmapHint, database_id: DatabaseId) -> Option<Box<PhysicalPlan>> {
    if !hint.extra_values.is_empty() {
        return None;
    }
    let collection = super::super::convert::db_qualified(database_id, &hint.collection);
    let value_str = sql_value_to_string(&hint.primary_value);
    Some(Box::new(PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection,
        path: hint.field.clone(),
        value: value_str,
        filters: Vec::new(),
        projection: Vec::new(),
        limit: 10_000,
        offset: 0,
    })))
}

pub(in crate::control::planner::sql_plan_convert) fn convert_join(
    p: JoinPlanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let JoinPlanParams {
        left,
        right,
        on,
        join_type,
        condition,
        limit,
        projection,
        filters,
        tenant_id,
        ctx,
    } = p;
    let mut left_collection =
        super::super::aggregate::join_side_collection(left, p.ctx.database_id);
    let mut right_collection =
        super::super::aggregate::join_side_collection(right, p.ctx.database_id);
    // RAW (non-db-qualified) names for the cost-model stats lookup: `ANALYZE`
    // keys its persisted column stats by the bare collection name, so the
    // shuffle cost model must look them up by the same raw name (not the
    // db-qualified token used for storage routing).
    let mut left_raw = super::super::aggregate::extract_collection_name(left);
    let mut right_raw = super::super::aggregate::extract_collection_name(right);
    let mut left_alias = extract_scan_alias(left);
    let mut right_alias = extract_scan_alias(right);
    let join_projection = extract_join_projection_specs(projection);
    let computed_projection = serialize_join_computed_projection(projection)?;
    let join_filter_bytes = serialize_join_condition(condition)?;
    let filter_bytes = serialize_join_post_filters(filters)?;

    // Check if the left side is a nested join (multi-way join).
    // If so, convert the inner join to a physical plan and pass it
    // as `left_input` so the executor runs it first. Sharded nested
    // joins are wrapped in Exchange{Broadcast} so the coordinator
    // gathers the nested join result and embeds it.
    let left_input = if matches!(left, SqlPlan::Join { .. }) {
        let inner_tasks = convert_one(left, tenant_id, ctx)?;
        inner_tasks.into_iter().next().map(|t| {
            let plan = t.plan;
            if plan.is_sharded_source() {
                Box::new(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                    child: Box::new(plan),
                    mode: ExchangeMode::Broadcast,
                })))
            } else {
                Box::new(plan)
            }
        })
    } else {
        // Catalog left scans lower to an embedded `ProviderScan` (returns
        // `Some`); plain user-collection scans stay `None` and are scanned by
        // name via `left_collection`.
        super::super::aggregate::inline_join_side(left, tenant_id, ctx)?
    };
    let right_input = super::super::aggregate::inline_join_side(right, tenant_id, ctx)?;

    // RIGHT JOIN → swap sides and convert to LEFT JOIN.
    let mut on_keys = on.to_vec();
    let mut left_input = left_input;
    let mut right_input = right_input;
    let effective_join_type = if join_type.as_str() == "right" {
        std::mem::swap(&mut left_collection, &mut right_collection);
        std::mem::swap(&mut left_raw, &mut right_raw);
        std::mem::swap(&mut left_alias, &mut right_alias);
        std::mem::swap(&mut left_input, &mut right_input);
        on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
        "left".to_string()
    } else {
        join_type.as_str().to_string()
    };

    // Analyze join children for selective-predicate bitmap pushdown.
    // The analysis runs on the *original* (pre-swap) children since it inspects
    // SqlPlan shape. After the RIGHT→LEFT swap, we swap the resulting hints too.
    let bitmap_hints = nodedb_sql::planner::bitmap_emit::hashjoin::analyze_join_sides(left, right);
    let (mut raw_left_bm, mut raw_right_bm) = (bitmap_hints.left, bitmap_hints.right);
    if join_type.as_str() == "right" {
        std::mem::swap(&mut raw_left_bm, &mut raw_right_bm);
    }
    let db_id = p.ctx.database_id;
    let left_bitmap = raw_left_bm.and_then(|h| bitmap_hint_to_plan(&h, db_id));
    let right_bitmap = raw_right_bm.and_then(|h| bitmap_hint_to_plan(&h, db_id));

    let vshard = VShardId::from_collection_in_database(p.ctx.database_id, &left_collection);

    // Shuffle eligibility. A whole-join shuffle is only *structurally* valid
    // when BOTH sides are plain sharded user collections scanned by name — i.e.
    // both `*_input` slots are `None` (no embedded catalog `ProviderScan` and no
    // nested-join sub-plan) — and the join is a real equi-join (`on_keys`
    // non-empty). Anything else (nested joins, catalog sides, cross/keyless
    // joins) keeps the default broadcast/local plan unchanged.
    //
    // Shuffle is honored only in cluster mode: single-node has no peers to
    // repartition across, so the local broadcast plan is both correct and
    // cheaper. The two inputs are left as BARE name scans (`left_input` /
    // `right_input` stay `None`); the coordinator resolver builds the per-side
    // scan fragments from the collection names and drives the producers.
    //
    // Given structural eligibility, shuffle is selected when EITHER:
    //   1. the operator forced it via `nodedb.force_shuffle_join` (manual
    //      override always wins), OR
    //   2. the ANALYZE-driven cost model picks it (both sides large enough that
    //      neither is cheap to broadcast). Un-analyzed collections fall back to
    //      broadcast — see `join_cost::cost_model_picks_shuffle`.
    let structurally_shufflable = p.ctx.cluster_enabled
        && !on_keys.is_empty()
        && left_input.is_none()
        && right_input.is_none()
        // Shuffle consumers currently execute only the bare equi-join. Keep
        // joins with coordinator-side semantics on the broadcast/local path
        // until the shuffle protocol carries and reapplies the full tail.
        && shuffle_supports_join_tail(
            &join_projection,
            &computed_projection,
            &join_filter_bytes,
            &filter_bytes,
        );
    let shuffle_eligible = structurally_shufflable
        && (p.ctx.force_shuffle_join
            || super::join_cost::cost_model_picks_shuffle(p.ctx, &left_raw, &right_raw));

    // Shuffle hash keys mirror the resolver's per-side split: the LEFT column of
    // each `on` pair partitions the probe side, the RIGHT column the build side
    // (carried as `(left, right)` so the resolver does not re-derive them).
    let shuffle_keys = on_keys.clone();

    let hash_join = PhysicalPlan::Query(QueryOp::HashJoin {
        left_collection,
        right_collection,
        left_alias,
        right_alias,
        on: on_keys,
        join_type: effective_join_type,
        // `QueryOp::HashJoin.limit` stays `usize`: `usize::MAX` is the
        // sentinel for "no SQL LIMIT". The handler distinguishes this from
        // an explicit limit and bounds a no-LIMIT join by the memory byte
        // budget (surfacing `ResourcesExhausted`) rather than truncating.
        limit: limit.unwrap_or(usize::MAX),
        post_group_by: Vec::new(),
        post_aggregates: Vec::new(),
        projection: join_projection,
        computed_projection,
        join_filters: join_filter_bytes,
        post_filters: filter_bytes,
        left_input,
        right_input,
        left_bitmap,
        right_bitmap,
    });

    let plan = if shuffle_eligible {
        // `num_parts == 0` is the "unset" sentinel: the operator left
        // `nodedb.shuffle_num_parts` unset, so the coordinator resolver defaults
        // it to the cluster data-node count (the convert layer has no view of
        // the routing table). A non-zero value is the operator's explicit
        // partition count and is used verbatim.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(hash_join),
            mode: ExchangeMode::Shuffle {
                keys: shuffle_keys,
                num_parts: p.ctx.shuffle_num_parts,
            },
        }))
    } else {
        hash_join
    };

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: p.ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shuffle_rejects_join_tail_semantics() {
        assert!(shuffle_supports_join_tail(&[], &[], &[], &[]));
        assert!(!shuffle_supports_join_tail(
            &[JoinProjection {
                source: "left.id".into(),
                output: "id".into(),
            }],
            &[],
            &[],
            &[],
        ));
        assert!(!shuffle_supports_join_tail(&[], &[1], &[], &[]));
        assert!(!shuffle_supports_join_tail(&[], &[], &[1], &[]));
        assert!(!shuffle_supports_join_tail(&[], &[], &[], &[1]));
    }
}

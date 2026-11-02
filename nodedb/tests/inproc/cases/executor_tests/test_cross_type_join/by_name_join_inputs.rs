// SPDX-License-Identifier: BUSL-1.1

//! Nested-loop and sort-merge joins read a side's resolved input instead of
//! scanning the side's collection by name.
//!
//! Inside a transaction the coordinator sets that input to the side's engine
//! scan, which folds the transaction's staged rows. Here each `*_all`
//! collection stands in for that scan: it holds the committed row plus a
//! second row the by-name collection lacks. A join that reads the input sees
//! both rows, on the left side, the right side, or both.

use super::super::helpers::{doc_surrogate, make_ctx, payload_value, send_ok};
use super::basic_scans::build_msgpack_map;
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, QueryOp};

fn coll(name: &str) -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, name)
}

fn put(ctx: &mut super::super::helpers::TestCtx, collection: &str, id: &str) {
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: coll(collection),
            document_id: id.into(),
            value: build_msgpack_map(&[("id", id), ("k", "1")]),
            surrogate: doc_surrogate(id),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
}

/// The rows of `collection` as a coordinator-resolved input.
fn input_of(ctx: &mut super::super::helpers::TestCtx, collection: &str) -> Box<PhysicalPlan> {
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: coll(collection),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );
    Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: response_codec::flatten_to_relational_rows(&payload),
        sources: Vec::new(),
        filters: Vec::new(),
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        system_time: nodedb_types::SystemTimeScope::Current,
    }))
}

#[derive(Clone, Copy, Debug)]
enum Strategy {
    NestedLoop,
    SortMerge,
}

fn join(
    strategy: Strategy,
    left_input: Option<Box<PhysicalPlan>>,
    right_input: Option<Box<PhysicalPlan>>,
) -> PhysicalPlan {
    match strategy {
        Strategy::NestedLoop => PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: coll("bnj_l"),
            right_collection: coll("bnj_r"),
            // Cross join: every left row pairs with every right row.
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 1_000,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_input,
            right_input,
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
        Strategy::SortMerge => PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: coll("bnj_l"),
            right_collection: coll("bnj_r"),
            on: vec![("k".into(), "k".into())],
            join_type: "inner".into(),
            limit: 1_000,
            pre_sorted: false,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_input,
            right_input,
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
    }
}

fn row_count(payload: &[u8]) -> usize {
    payload_value(payload).as_array().map_or(0, Vec::len)
}

fn check(strategy: Strategy) {
    let mut ctx = make_ctx();
    put(&mut ctx, "bnj_l", "l1");
    put(&mut ctx, "bnj_r", "r1");
    for (collection, ids) in [("bnj_l_all", ["l1", "l2"]), ("bnj_r_all", ["r1", "r2"])] {
        for id in ids {
            put(&mut ctx, collection, id);
        }
    }

    let by_name = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        join(strategy, None, None),
    );
    assert_eq!(row_count(&by_name), 1, "{strategy:?}: committed rows only");

    let left = input_of(&mut ctx, "bnj_l_all");
    let plan = join(strategy, Some(left), None);
    let payload = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, plan);
    assert_eq!(row_count(&payload), 2, "{strategy:?}: left input read");

    let right = input_of(&mut ctx, "bnj_r_all");
    let plan = join(strategy, None, Some(right));
    let payload = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, plan);
    assert_eq!(row_count(&payload), 2, "{strategy:?}: right input read");

    let left = input_of(&mut ctx, "bnj_l_all");
    let right = input_of(&mut ctx, "bnj_r_all");
    let plan = join(strategy, Some(left), Some(right));
    let payload = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, plan);
    assert_eq!(row_count(&payload), 4, "{strategy:?}: both inputs read");
}

#[test]
fn nested_loop_join_reads_side_inputs() {
    check(Strategy::NestedLoop);
}

#[test]
fn sort_merge_join_reads_side_inputs() {
    check(Strategy::SortMerge);
}

// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Event Plane streaming materialized views.
//!
//! Tests: incremental aggregation (COUNT/SUM/MIN/MAX), watermark-driven
//! finalization, backfill from buffer, state persistence + restore.

use nodedb::event::streaming_mv::persist::MvPersistence;
use nodedb::event::streaming_mv::state::{AggInput, GroupState, MvState};
use nodedb::event::streaming_mv::types::{AggDef, AggFunction};
use nodedb::types::DatabaseId;
use nodedb_types::Value;

fn count_def() -> Vec<AggDef> {
    vec![AggDef {
        output_name: "cnt".to_string(),
        function: AggFunction::Count,
        input_expr: String::new(),
    }]
}

fn int(v: i64) -> AggInput {
    AggInput::Value(Value::Integer(v))
}

#[test]
fn incremental_count() {
    let state = MvState::new(
        "order_counts".to_string(),
        vec!["op".to_string()],
        count_def(),
    );

    state.update_with_time("group_a", &[AggInput::Event], 0);
    state.update_with_time("group_a", &[AggInput::Event], 0);
    state.update_with_time("group_b", &[AggInput::Event], 0);

    let results = state.read_results_with_status().unwrap();
    assert_eq!(results.len(), 2);

    // MvResultRow = (group_key, AggRow, finalized)
    // AggRow = Vec<(output_name, value)>
    let group_a = results.iter().find(|r| r.0 == "group_a").unwrap();
    assert_eq!(group_a.1[0].1, Value::Integer(2));

    let group_b = results.iter().find(|r| r.0 == "group_b").unwrap();
    assert_eq!(group_b.1[0].1, Value::Integer(1));
}

#[test]
fn incremental_sum_min_max() {
    let state = MvState::new(
        "revenue_stats".to_string(),
        vec!["bucket".to_string()],
        vec![
            AggDef {
                output_name: "total".to_string(),
                function: AggFunction::Sum,
                input_expr: "total".to_string(),
            },
            AggDef {
                output_name: "min_total".to_string(),
                function: AggFunction::Min,
                input_expr: "total".to_string(),
            },
            AggDef {
                output_name: "max_total".to_string(),
                function: AggFunction::Max,
                input_expr: "total".to_string(),
            },
        ],
    );

    // Pass the same value to all three aggregate slots.
    for v in [10, 30, 20] {
        state.update_with_time("bucket", &[int(v), int(v), int(v)], 0);
    }

    let results = state.read_results_with_status().unwrap();
    let bucket = results.iter().find(|r| r.0 == "bucket").unwrap();

    assert_eq!(bucket.1[0].1, Value::Integer(60));
    assert_eq!(bucket.1[1].1, Value::Integer(10));
    assert_eq!(bucket.1[2].1, Value::Integer(30));
}

#[test]
fn incremental_avg() {
    let state = MvState::new(
        "score_avg".to_string(),
        vec!["group".to_string()],
        vec![AggDef {
            output_name: "avg_score".to_string(),
            function: AggFunction::Avg,
            input_expr: "score".to_string(),
        }],
    );

    for v in [10, 20, 30] {
        state.update_with_time("g", &[int(v)], 0);
    }

    let results = state.read_results_with_status().unwrap();
    let g = results.iter().find(|r| r.0 == "g").unwrap();
    // AVG = SUM / COUNT = 60 / 3 = 20.
    assert_eq!(g.1[0].1, Value::Float(20.0));
}

#[test]
fn watermark_finalization() {
    let state = MvState::new(
        "event_counts".to_string(),
        vec!["group".to_string()],
        count_def(),
    );

    // Use update_with_time so latest_event_time is populated for finalization.
    state.update_with_time("early", &[AggInput::Event], 1000);
    state.update_with_time("late", &[AggInput::Event], 5000);

    // Finalize groups with latest_event_time < 3000.
    let finalized = state.finalize_buckets(3000);
    assert_eq!(finalized, 1); // Only "early" finalized.

    let results = state.read_results_with_status().unwrap();
    // MvResultRow = (group_key, AggRow, finalized_bool)
    let early = results.iter().find(|r| r.0 == "early").unwrap();
    assert!(early.2); // finalized = true

    let late = results.iter().find(|r| r.0 == "late").unwrap();
    assert!(!late.2); // finalized = false
}

#[test]
fn snapshot_and_restore() {
    let state = MvState::new(
        "snap_mv".to_string(),
        vec!["group".to_string()],
        count_def(),
    );
    state.update_with_time("g1", &[AggInput::Event], 0);
    state.update_with_time("g1", &[AggInput::Event], 0);
    state.update_with_time("g2", &[AggInput::Event], 0);

    let snapshot = state.snapshot();
    assert_eq!(snapshot.len(), 2);

    // Restore into fresh state with matching aggregate definitions.
    let restored = MvState::new(
        "snap_mv".to_string(),
        vec!["group".to_string()],
        count_def(),
    );
    restored.restore(snapshot);

    let results = restored.read_results_with_status().unwrap();
    let g1 = results.iter().find(|r| r.0 == "g1").unwrap();
    assert_eq!(g1.1[0].1, Value::Integer(2));
}

/// A group state that took `values` for `func`, at `event_time`.
fn state_of(func: AggFunction, values: &[i64], event_time: u64) -> GroupState {
    let mut state = GroupState::default();
    for v in values {
        state.update(func, &int(*v));
    }
    state.update_event_time(event_time);
    state
}

#[test]
fn persistence_save_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let persist = MvPersistence::open(dir.path()).unwrap();

    let mut update = state_of(AggFunction::Sum, &[5, 10, 15], 3000);
    update.finalized = true;
    let snapshot = vec![
        (
            "INSERT".to_string(),
            vec![state_of(AggFunction::Sum, &[10, 20, 30, 40], 5000)],
        ),
        ("UPDATE".to_string(), vec![update]),
    ];

    persist
        .save(DatabaseId::DEFAULT, 1, "order_stats", &snapshot)
        .unwrap();
    let loaded = persist
        .load(DatabaseId::DEFAULT, 1, "order_stats")
        .unwrap()
        .unwrap();
    assert_eq!(loaded, snapshot);
    assert_eq!(loaded[0].1[0].count, 4);
    assert!(loaded[1].1[0].finalized);
}

#[test]
fn persistence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let persist = MvPersistence::open(dir.path()).unwrap();
        let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
        persist
            .save(DatabaseId::DEFAULT, 1, "mv1", &snapshot)
            .unwrap();
    }
    let persist = MvPersistence::open(dir.path()).unwrap();
    assert!(
        persist
            .load(DatabaseId::DEFAULT, 1, "mv1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn persistence_delete() {
    let dir = tempfile::tempdir().unwrap();
    let persist = MvPersistence::open(dir.path()).unwrap();
    let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
    persist
        .save(DatabaseId::DEFAULT, 1, "mv1", &snapshot)
        .unwrap();
    persist.delete(DatabaseId::DEFAULT, 1, "mv1").unwrap();
    assert!(
        persist
            .load(DatabaseId::DEFAULT, 1, "mv1")
            .unwrap()
            .is_none()
    );
}

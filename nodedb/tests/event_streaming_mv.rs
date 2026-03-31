//! Integration tests for Event Plane streaming materialized views.
//!
//! Tests: incremental aggregation (COUNT/SUM/MIN/MAX), watermark-driven
//! finalization, backfill from buffer, state persistence + restore.

use nodedb::event::streaming_mv::persist::MvPersistence;
use nodedb::event::streaming_mv::state::{GroupState, MvState};
use nodedb::event::streaming_mv::types::{AggDef, AggFunction};

#[test]
fn incremental_count() {
    let state = MvState::new(
        "order_counts".to_string(),
        vec!["op".to_string()],
        vec![AggDef {
            output_name: "cnt".to_string(),
            function: AggFunction::Count,
            input_expr: String::new(),
        }],
    );

    // Each update passes &[1.0]; GroupState::update increments count by 1 per call.
    state.update("group_a", &[1.0]);
    state.update("group_a", &[1.0]);
    state.update("group_b", &[1.0]);

    let results = state.read_results_with_status();
    assert_eq!(results.len(), 2);

    // MvResultRow = (group_key, AggRow, finalized)
    // AggRow = Vec<(output_name, value)>
    let group_a = results.iter().find(|r| r.0 == "group_a").unwrap();
    assert_eq!(group_a.1[0].1, 2.0); // COUNT = 2

    let group_b = results.iter().find(|r| r.0 == "group_b").unwrap();
    assert_eq!(group_b.1[0].1, 1.0); // COUNT = 1
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
    state.update("bucket", &[10.0, 10.0, 10.0]);
    state.update("bucket", &[30.0, 30.0, 30.0]);
    state.update("bucket", &[20.0, 20.0, 20.0]);

    let results = state.read_results_with_status();
    let bucket = results.iter().find(|r| r.0 == "bucket").unwrap();

    // Sum aggregate (index 0): SUM of 10+30+20 = 60.
    assert!((bucket.1[0].1 - 60.0).abs() < f64::EPSILON);
    // Min aggregate (index 1): MIN of 10, 30, 20 = 10.
    assert!((bucket.1[1].1 - 10.0).abs() < f64::EPSILON);
    // Max aggregate (index 2): MAX of 10, 30, 20 = 30.
    assert!((bucket.1[2].1 - 30.0).abs() < f64::EPSILON);
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

    state.update("g", &[10.0]);
    state.update("g", &[20.0]);
    state.update("g", &[30.0]);

    let results = state.read_results_with_status();
    let g = results.iter().find(|r| r.0 == "g").unwrap();
    // AVG = SUM / COUNT = 60 / 3 = 20.
    assert!((g.1[0].1 - 20.0).abs() < f64::EPSILON);
}

#[test]
fn watermark_finalization() {
    let state = MvState::new(
        "event_counts".to_string(),
        vec!["group".to_string()],
        vec![AggDef {
            output_name: "cnt".to_string(),
            function: AggFunction::Count,
            input_expr: String::new(),
        }],
    );

    // Use update_with_time so latest_event_time is populated for finalization.
    state.update_with_time("early", &[1.0], 1000);
    state.update_with_time("late", &[1.0], 5000);

    // Finalize groups with latest_event_time < 3000.
    let finalized = state.finalize_buckets(3000);
    assert_eq!(finalized, 1); // Only "early" finalized.

    let results = state.read_results_with_status();
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
        vec![AggDef {
            output_name: "cnt".to_string(),
            function: AggFunction::Count,
            input_expr: String::new(),
        }],
    );
    state.update("g1", &[1.0]);
    state.update("g1", &[1.0]);
    state.update("g2", &[1.0]);

    let snapshot = state.snapshot();
    assert_eq!(snapshot.len(), 2);

    // Restore into fresh state with matching aggregate definitions.
    let restored = MvState::new(
        "snap_mv".to_string(),
        vec!["group".to_string()],
        vec![AggDef {
            output_name: "cnt".to_string(),
            function: AggFunction::Count,
            input_expr: String::new(),
        }],
    );
    restored.restore(snapshot);

    let results = restored.read_results_with_status();
    let g1 = results.iter().find(|r| r.0 == "g1").unwrap();
    assert_eq!(g1.1[0].1, 2.0); // COUNT = 2
}

#[test]
fn persistence_save_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let persist = MvPersistence::open(dir.path()).unwrap();

    let snapshot = vec![
        (
            "INSERT".to_string(),
            vec![GroupState {
                count: 5,
                sum: 100.0,
                min: Some(10.0),
                max: Some(50.0),
                finalized: false,
                latest_event_time: 5000,
            }],
        ),
        (
            "UPDATE".to_string(),
            vec![GroupState {
                count: 3,
                sum: 30.0,
                min: Some(5.0),
                max: Some(15.0),
                finalized: true,
                latest_event_time: 3000,
            }],
        ),
    ];

    persist.save(1, "order_stats", &snapshot).unwrap();
    let loaded = persist.load(1, "order_stats").unwrap().unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].1[0].count, 5);
    assert!(loaded[1].1[0].finalized);
}

#[test]
fn persistence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let persist = MvPersistence::open(dir.path()).unwrap();
        let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
        persist.save(1, "mv1", &snapshot).unwrap();
    }
    let persist = MvPersistence::open(dir.path()).unwrap();
    assert!(persist.load(1, "mv1").unwrap().is_some());
}

#[test]
fn persistence_delete() {
    let dir = tempfile::tempdir().unwrap();
    let persist = MvPersistence::open(dir.path()).unwrap();
    let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
    persist.save(1, "mv1", &snapshot).unwrap();
    persist.delete(1, "mv1").unwrap();
    assert!(persist.load(1, "mv1").unwrap().is_none());
}

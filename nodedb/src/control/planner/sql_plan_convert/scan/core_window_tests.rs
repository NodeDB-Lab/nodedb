// SPDX-License-Identifier: BUSL-1.1

//! Lowering of a scan that carries window functions, per engine.

use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, ExchangeOp, KvOp, PhysicalPlan, QueryOp,
};
use nodedb_sql::types::{EngineType, Projection, SortKey, SqlExpr, WindowSpec};

use super::convert_scan;
use crate::control::planner::sql_plan_convert::scan_params::ScanParams;
use crate::types::{DatabaseId, TenantId};

fn id_key() -> SortKey {
    SortKey {
        expr: SqlExpr::Column {
            table: None,
            name: "id".into(),
        },
        ascending: true,
        nulls_first: false,
    }
}

/// Lower `SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM events
/// ORDER BY id LIMIT <limit>` on `engine`.
fn lower(engine: EngineType, limit: Option<usize>) -> PhysicalPlan {
    let projection = vec![
        Projection::Column("id".into()),
        Projection::Computed {
            expr: SqlExpr::Function {
                name: "row_number".into(),
                args: Vec::new(),
                distinct: false,
            },
            alias: "rn".into(),
        },
    ];
    let windows = vec![WindowSpec {
        function: "row_number".into(),
        args: Vec::new(),
        partition_by: Vec::new(),
        order_by: vec![id_key()],
        alias: "rn".into(),
        frame: Default::default(),
    }];
    let sort = vec![id_key()];
    let temporal = nodedb_sql::TemporalScope::default();
    let mut tasks = convert_scan(ScanParams {
        collection: "events",
        engine: &engine,
        filters: &[],
        projection: &projection,
        sort_keys: &sort,
        limit: &limit,
        offset: &0,
        distinct: &false,
        window_functions: &windows,
        tenant_id: TenantId::new(1),
        temporal: &temporal,
        database_id: DatabaseId::DEFAULT,
    })
    .expect("lower windowed scan");
    assert_eq!(tasks.len(), 1);
    tasks.remove(0).plan
}

fn unwrap_gather(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => child,
        other => other,
    }
}

/// Columnar, spatial and KV scan ops have no window slot. The windows,
/// sort and limit move to the tail, and the body reads every row.
#[test]
fn windowed_scan_without_window_slot_runs_windows_in_the_tail() {
    for engine in [
        EngineType::Columnar,
        EngineType::Spatial,
        EngineType::KeyValue,
    ] {
        let plan = lower(engine, Some(3));
        let PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            window_functions,
            sort_keys,
            limit,
            projection,
            ..
        }) = &plan
        else {
            panic!("{engine:?}: expected a PostProcess tail, got {plan:?}");
        };
        assert!(!window_functions.is_empty(), "{engine:?}: window specs");
        assert_eq!(sort_keys.len(), 1, "{engine:?}: ORDER BY in the tail");
        assert_eq!(*limit, Some(3), "{engine:?}: LIMIT in the tail");
        assert!(projection.contains(&"rn".to_string()), "{engine:?}");
        match unwrap_gather(input) {
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                limit,
                sort_keys,
                projection,
                ..
            }) => {
                assert_eq!(*limit, usize::MAX, "{engine:?}: body reads every row");
                assert!(sort_keys.is_empty(), "{engine:?}");
                assert!(projection.is_empty(), "{engine:?}: body keeps every column");
            }
            PhysicalPlan::Kv(KvOp::Scan {
                count, sort_keys, ..
            }) => {
                assert_eq!(*count, usize::MAX, "{engine:?}: body reads every row");
                assert!(sort_keys.is_empty(), "{engine:?}");
            }
            other => panic!("{engine:?}: unexpected body {other:?}"),
        }
    }
}

/// The document scan evaluates windows itself and keeps its own shape.
#[test]
fn windowed_document_scan_keeps_windows_on_the_scan() {
    for engine in [EngineType::DocumentStrict, EngineType::DocumentSchemaless] {
        let plan = lower(engine, Some(3));
        let PhysicalPlan::Document(DocumentOp::Scan {
            window_functions,
            limit,
            ..
        }) = &plan
        else {
            panic!("{engine:?}: expected a document scan, got {plan:?}");
        };
        assert!(!window_functions.is_empty(), "{engine:?}");
        assert_eq!(*limit, 3, "{engine:?}");
    }
}

/// A columnar scan with no window keeps the direct scan op.
#[test]
fn columnar_scan_without_windows_stays_a_scan() {
    let temporal = nodedb_sql::TemporalScope::default();
    let mut tasks = convert_scan(ScanParams {
        collection: "events",
        engine: &EngineType::Columnar,
        filters: &[],
        projection: &[Projection::Column("id".into())],
        sort_keys: &[],
        limit: &Some(3),
        offset: &0,
        distinct: &false,
        window_functions: &[],
        tenant_id: TenantId::new(1),
        temporal: &temporal,
        database_id: DatabaseId::DEFAULT,
    })
    .expect("lower plain scan");
    assert!(matches!(
        tasks.remove(0).plan,
        PhysicalPlan::Columnar(ColumnarOp::Scan { limit: 3, .. })
    ));
}

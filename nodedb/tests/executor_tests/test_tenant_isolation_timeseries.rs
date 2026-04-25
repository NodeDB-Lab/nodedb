//! Cross-tenant isolation: Timeseries engine.
//!
//! Tenant A ingests metrics. Tenant B scans — must get zero rows.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::TimeseriesOp;

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn timeseries_scan_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A ingests ILP-format timeseries data.
    let ilp_data = "cpu,host=server01 value=0.64 1000000000\n\
                    cpu,host=server01 value=0.72 2000000000\n\
                    cpu,host=server01 value=0.55 3000000000\n";
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "cpu".into(),
            payload: ilp_data.as_bytes().to_vec(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
        }),
    );

    // Tenant A can scan and see data.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "cpu".into(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 100,
            filters: vec![],
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            rls_filters: vec![],
            system_as_of_ms: None,
            valid_at_ms: None,
            computed_columns: vec![],
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    assert!(
        !resp_a.payload.is_empty(),
        "Tenant A should see timeseries data"
    );

    // Tenant B scans the same collection — must get zero rows.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "cpu".into(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 100,
            filters: vec![],
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            rls_filters: vec![],
            system_as_of_ms: None,
            valid_at_ms: None,
            computed_columns: vec![],
        }),
    );
    assert_eq!(resp_b.status, Status::Ok);
    let json_b = payload_json(&resp_b.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json_b).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "Tenant B timeseries scan must return 0 rows, got {}: {json_b}",
        arr.len()
    );
}

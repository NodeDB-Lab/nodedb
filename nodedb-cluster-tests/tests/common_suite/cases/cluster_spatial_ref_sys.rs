// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster test for `CREATE SPATIAL_REF_SYS` and `DROP
//! SPATIAL_REF_SYS`.
//!
//! The test matches `binary(/cluster/)` in nextest.toml, so it runs in the
//! serial `cluster` test group. A user-defined CRS defined through one node
//! replicates through the metadata group: every node resolves it, transforms
//! to it alike and lists it, and every node refuses it once it is dropped.

use std::time::Duration;

use nodedb_crs::{CrsContext, CustomCrsSet, Srid, parse_proj_string};

use crate::common;

use common::cluster_harness::TestCluster;

const CODE: u32 = 910_201;

const TEST_TM: &str =
    "+proj=tmerc +lat_0=0 +lon_0=101.25 +k=0.99991 +x_0=123456 +y_0=0 +ellps=GRS80 +units=m";

/// `(x, y)` of `(103.8, 1.45)` in [`TEST_TM`], computed in the test.
fn expected() -> (f64, f64) {
    let srid = Srid::new(CODE).expect("srid");
    let mut set = CustomCrsSet::new();
    set.define(parse_proj_string(srid, "c", TEST_TM).expect("parses"))
        .expect("defines");
    let t = CrsContext::new(&set)
        .transformer(Srid::WGS84, srid)
        .expect("defined");
    let [x, y, _] = t.apply([103.8, 1.45, 0.0]).expect("inside");
    (x, y)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_defined_crs_resolves_on_every_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE SPATIAL_REF_SYS {CODE} (NAME 'cluster tm', PROJ '{TEST_TM}')"
        ))
        .await
        .expect("CREATE SPATIAL_REF_SYS");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    let (want_x, want_y) = expected();
    let sql = format!(
        "SELECT ST_X(ST_Transform(ST_Point(103.8, 1.45), {CODE})) AS x, \
         ST_Y(ST_Transform(ST_Point(103.8, 1.45), {CODE})) AS y"
    );
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = node
            .query_named_rows(&sql)
            .await
            .unwrap_or_else(|e| panic!("node {idx}: {sql}: {e}"));
        assert_eq!(rows.len(), 1, "node {idx}: {rows:?}");
        let num = |c: &str| -> f64 {
            rows[0][c]
                .parse()
                .unwrap_or_else(|e| panic!("node {idx}: {c} in {rows:?}: {e}"))
        };
        assert!(
            (num("x") - want_x).abs() < 1e-3 && (num("y") - want_y).abs() < 1e-3,
            "node {idx}: got ({}, {}), want ({want_x}, {want_y})",
            num("x"),
            num("y")
        );
        let listed = node
            .query_named_rows(&format!(
                "SELECT name FROM nodedb.spatial_ref_sys WHERE srid = {CODE}"
            ))
            .await
            .unwrap_or_else(|e| panic!("node {idx}: view: {e}"));
        assert_eq!(listed.len(), 1, "node {idx}: {listed:?}");
        assert_eq!(listed[0]["name"], "cluster tm", "node {idx}");
    }

    cluster
        .exec_ddl_on_any_leader(&format!("DROP SPATIAL_REF_SYS {CODE}"))
        .await
        .expect("DROP SPATIAL_REF_SYS");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let err = node
            .exec(&format!(
                "SELECT ST_Transform(ST_Point(103.8, 1.45), {CODE})"
            ))
            .await
            .expect_err("a dropped code is refused");
        assert!(
            err.contains(&format!("SRID {CODE} was dropped")),
            "node {idx}: {err}"
        );
    }
    cluster.shutdown().await;
}

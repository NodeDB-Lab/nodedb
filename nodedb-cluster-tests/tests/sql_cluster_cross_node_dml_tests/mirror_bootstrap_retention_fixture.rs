// SPDX-License-Identifier: BUSL-1.1

//! Helpers shared by the mirror retention tests.

use nodedb_types::config::tuning::NetworkTuning;
use nodedb_types::{DatabaseId, TenantId};

use super::mirror_bootstrap_fixture::{
    LinkPki, SOURCE_NAME, await_following, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, serve_source, use_database,
};
use crate::common::cluster_harness::TestCluster;

pub(super) const COPY: &str = "retention_copy";
/// The tenant of the harness superuser.
pub(super) const TENANT: TenantId = TenantId::new(1);

pub(super) fn now_ms() -> i64 {
    nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64())
}

pub(super) async fn source_with(database: &str, ddl: &[&str]) -> TestCluster {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    seed_source(source, database, ddl).await
}

pub(super) async fn tuned_source_with(database: &str, ddl: &[&str]) -> TestCluster {
    let source = TestCluster::spawn_three_with_network_tuning(short_hold())
        .await
        .expect("source cluster");
    seed_source(source, database, ddl).await
}

async fn seed_source(source: TestCluster, database: &str, ddl: &[&str]) -> TestCluster {
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create database");
    use_database(&source, database).await;
    for statement in ddl {
        source
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    source
}

pub(super) async fn following_mirror(
    source: &TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    link_mirror(source, mirror, database).await
}

pub(super) async fn tuned_following_mirror(
    source: &TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let mirror = TestCluster::spawn_three_with_network_tuning(short_hold())
        .await
        .expect("mirror cluster");
    link_mirror(source, mirror, database).await
}

async fn link_mirror(
    source: &TestCluster,
    mirror: TestCluster,
    database: &str,
) -> (TestCluster, DatabaseId, LinkPki) {
    let pki = link_pki();
    let seeds = serve_source(source, &pki, &[database]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{database}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let id = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;
    (mirror, id, pki)
}

/// A cleanup horizon short enough for a test: one second of snapshot age, ten of
/// statement deadline, and the fixed clock skew bound.
fn short_hold() -> NetworkTuning {
    NetworkTuning {
        default_deadline_secs: 10,
        copy_deadline_secs: 1,
        txn_snapshot_max_age_secs: 1,
        ..NetworkTuning::default()
    }
}

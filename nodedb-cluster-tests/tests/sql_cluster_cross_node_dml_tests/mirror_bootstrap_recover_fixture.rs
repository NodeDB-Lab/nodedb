// SPDX-License-Identifier: BUSL-1.1

//! Helpers shared by the mirror link-recovery tests.

use nodedb_types::{DatabaseId, MirrorStatus};

use super::mirror_bootstrap_fixture::use_database;
use crate::common::cluster_harness::{TestCluster, TestClusterNode};

pub(super) const COPY: &str = "recover_copy";

pub(super) async fn seeded_source(database: &str) -> TestCluster {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
        .await
        .expect("create database");
    use_database(&source, database).await;
    source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('a', 1)")
        .await
        .expect("insert");
    source
}

pub(super) fn status_on(node: &TestClusterNode, database: DatabaseId) -> Option<MirrorStatus> {
    node.shared
        .credentials
        .catalog()
        .get_database(database)
        .expect("catalog")
        .and_then(|descriptor| descriptor.mirror_origin)
        .map(|origin| origin.status)
}

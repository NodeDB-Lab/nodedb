// SPDX-License-Identifier: BUSL-1.1

//! A write to a mirror on a three-node cluster is refused on every node. A node
//! that does not lead the collection's group forwards the entry, and the
//! leader's gate answers with the typed refusal. No replica applies it.

use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::security::catalog::database_types::DatabaseStatus;
use nodedb_types::{DatabaseId, Lsn, MirrorMode, MirrorOrigin, MirrorStatus};

use crate::common::cluster_harness::{TestCluster, wait_for};

const DATABASE: &str = "mirror_forward";

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn every_node_answers_a_mirror_write_with_the_read_only_refusal() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {DATABASE}"))
            .await
            .expect("use database");
    }
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");

    let shared = &cluster.nodes[0].shared;
    let catalog = shared.credentials.catalog();
    let database_id = catalog
        .get_database_id_by_name(DATABASE)
        .expect("catalog lookup")
        .expect("database exists");
    let mut descriptor = catalog
        .get_database(database_id)
        .expect("catalog read")
        .expect("descriptor exists");
    descriptor.status = DatabaseStatus::Mirroring;
    descriptor.mirror_origin = Some(MirrorOrigin {
        source_cluster: "source".to_string(),
        source_database: DatabaseId::new(0),
        mode: MirrorMode::Async,
        last_applied: Lsn::new(1),
        status: MirrorStatus::Following,
    });
    propose_catalog_entry_async(shared, &CatalogEntry::PutDatabase(Box::new(descriptor)))
        .await
        .expect("mark the database a mirror");
    wait_for(
        "every node applied the mirror descriptor",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|node| {
                node.shared
                    .mirror
                    .fence
                    .read_only_name(database_id)
                    .is_some()
            })
        },
    )
    .await;

    for (index, node) in cluster.nodes.iter().enumerate() {
        let refusal = node
            .exec(&format!(
                "INSERT INTO docs (id, v) VALUES ('row{index}', 1)"
            ))
            .await
            .expect_err("a mirror refuses the write");
        assert!(
            refusal.contains("read-only mirror"),
            "node {index} answered: {refusal}"
        );
    }

    cluster.shutdown().await;
}

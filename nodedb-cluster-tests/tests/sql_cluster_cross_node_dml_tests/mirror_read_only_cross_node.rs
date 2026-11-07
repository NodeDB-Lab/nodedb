// SPDX-License-Identifier: BUSL-1.1

//! A write to a mirror on a three-node cluster is refused on every node. A node
//! that does not lead the collection's group forwards the entry, and the
//! leader's gate answers with the typed refusal. No replica applies it.

use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::security::catalog::database_types::DatabaseStatus;
use nodedb_types::{
    DatabaseId, MirrorCutState, MirrorGroupPosition, MirrorMode, MirrorOrigin, MirrorStatus,
};

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
        source_database_name: "source_db".to_string(),
        mode: MirrorMode::Async,
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

    // The cut state reaches every node through the metadata log.
    let cut_state = MirrorCutState {
        ready_cut: 7,
        applied_cut: 7,
        applied_cut_hlc_ns: 1,
        cap_ordinal: 1,
        positions: vec![MirrorGroupPosition {
            group_id: 1,
            log_index: 40,
        }],
        epochs: Vec::new(),
        fence_epoch: 0,
        source_timeline: None,
    };
    propose_catalog_entry_async(
        shared,
        &CatalogEntry::PutMirrorCutState {
            database_id: database_id.as_u64(),
            state: Box::new(cut_state.clone()),
        },
    )
    .await
    .expect("propose the cut state");
    wait_for(
        "every node applied the cut state",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|node| {
                node.shared
                    .credentials
                    .catalog()
                    .get_mirror_cut_state(database_id)
                    .expect("read the cut state")
                    .as_ref()
                    == Some(&cut_state)
            })
        },
    )
    .await;

    // PROMOTE keeps it on every node: a replica whose data group lags the
    // promotion still decides its held entries from the row. A later DROP
    // removes it.
    cluster
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {DATABASE} PROMOTE"))
        .await
        .expect("promote the mirror");
    wait_for(
        "every node applied the promotion",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|node| {
                let catalog = node.shared.credentials.catalog();
                let promoted = catalog
                    .get_database(database_id)
                    .expect("read the descriptor")
                    .and_then(|descriptor| descriptor.mirror_origin)
                    .is_some_and(|origin| {
                        matches!(origin.status, nodedb_types::MirrorStatus::Promoted)
                    });
                promoted
                    && catalog
                        .get_mirror_cut_state(database_id)
                        .expect("read the cut state")
                        .as_ref()
                        == Some(&cut_state)
            })
        },
    )
    .await;

    cluster.shutdown().await;
}

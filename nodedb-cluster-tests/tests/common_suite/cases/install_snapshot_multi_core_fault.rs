// SPDX-License-Identifier: BUSL-1.1

//! A snapshot install that one core fails reports a typed, retryable error,
//! and a re-install of the same bytes converges to every row on its owning
//! core.
//!
//! The fail point `snapshot_install::core1`, armed for node N, makes core 1
//! of node N report its share as failed after it installed it: the other core holds the
//! snapshot, this one does not count as settled. The re-install clears every
//! core before it installs, so no row lands twice or off its core.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::time::Duration;

use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb::control::cluster::snapshot_install::SnapshotInstallError;
use nodedb::types::TenantId;
use nodedb_cluster::SnapshotBuilder;
use nodedb_test_support::fail_point::{FailAction, FailGuard};

use crate::common::cluster_harness::shared_steps::key_collection;
use crate::common::cluster_harness::{TestCluster, wait_for};

const CORES: usize = 2;
const COLLECTIONS: usize = 8;
const ROWS: usize = 5;

fn collection(i: usize) -> String {
    format!("snap_fault_{i}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn failed_core_install_is_retryable_and_reinstall_converges() {
    let cluster = TestCluster::spawn_three_with_cores(CORES)
        .await
        .expect("3-node 2-core cluster");

    for i in 0..COLLECTIONS {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {} (id TEXT PRIMARY KEY, payload TEXT) \
                 WITH (engine='document_strict')",
                collection(i)
            ))
            .await
            .expect("CREATE COLLECTION");
    }
    wait_for(
        "all nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= COLLECTIONS)
        },
    )
    .await;
    for i in 0..COLLECTIONS {
        for r in 0..ROWS {
            cluster.nodes[0]
                .client
                .simple_query(&format!(
                    "INSERT INTO {} (id, payload) VALUES ('r{r}', 'v{r}')",
                    collection(i)
                ))
                .await
                .unwrap_or_else(|e| panic!("insert {} r{r}: {e}", collection(i)));
        }
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let source = &cluster.nodes[0];
    let target = &cluster.nodes[1];
    let gid = source
        .group_id_for_collection(&collection(0))
        .expect("collection maps to a data group");
    let in_group: Vec<String> = (0..COLLECTIONS)
        .map(collection)
        .filter(|c| source.group_id_for_collection(c) == Some(gid))
        .collect();

    let bytes = DataPlaneSnapshotBuilder::new(source.shared.clone())
        .build_group_snapshot(gid, 0, 0)
        .await
        .expect("build the group snapshot")
        .bytes;
    assert!(!bytes.is_empty(), "group {gid} snapshot carries data");

    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    {
        let _fault = FailGuard::for_node(
            target.node_id,
            "snapshot_install::core1",
            FailAction::Fail("injected core install fault".to_string()),
        );
        let err = applier
            .install(gid, &bytes)
            .await
            .expect_err("a failed core must fail the install");
        assert!(
            matches!(err, SnapshotInstallError::CoreInstall { core_id: 1, .. }),
            "unexpected error: {err}"
        );
        assert!(err.is_retryable(), "a core install error is retryable");
    }

    applier
        .install(gid, &bytes)
        .await
        .expect("the re-install converges");

    let homes: HashMap<String, usize> = in_group
        .iter()
        .map(|c| (c.clone(), target.home_core_of(c)))
        .collect();
    let tenant = target
        .shared
        .credentials
        .catalog()
        .load_all_collections_across_databases()
        .expect("catalog read")
        .into_iter()
        .find(|c| c.name == in_group[0])
        .expect("collection in catalog")
        .tenant_id;

    let mut per_collection: HashMap<String, usize> = HashMap::new();
    for core in 0..CORES {
        for key in target
            .document_keys_on_core(core, TenantId::new(tenant))
            .await
        {
            let Some(coll) = key_collection(&key) else {
                continue;
            };
            if let Some(home) = homes.get(coll) {
                assert_eq!(*home, core, "{coll} row on core {core}, home {home}");
                *per_collection.entry(coll.to_string()).or_default() += 1;
            }
        }
    }
    for coll in &in_group {
        assert_eq!(
            per_collection.get(coll).copied().unwrap_or(0),
            ROWS,
            "{coll} rows after the re-install"
        );
    }

    cluster.shutdown().await;
}

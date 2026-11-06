// SPDX-License-Identifier: BUSL-1.1
//! A KV key whose TTL passed is reaped once, through its data group's log,
//! on every replica alike.
//!
//! No core reaps a key on its own. Each core hands its passed TTLs to a due
//! set, and the data group's leader proposes one reap per key through write
//! admission. Every replica applies the reap at the same log position. The
//! test writes a key with a one-second TTL on a three-node cluster, waits
//! past its expiry, and checks that every node answers the key absent and
//! holds no due entry left: each replica applied the leader's reap.

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb::engine::kv::DueExpiry;
use nodedb::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

const COLL: &str = "ttl_reap_converges";

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn holds_key(client: &tokio_postgres::Client) -> bool {
    let rows = client
        .simple_query(&format!("SELECT key FROM {COLL} WHERE key = 'eph'"))
        .await
        .unwrap_or_else(|e| panic!("read the key: {}", pg_detail(&e)));
    rows.into_iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
}

/// The due entries every core of `node` still holds for `COLL`.
async fn due_on(node: &common::cluster_harness::TestClusterNode) -> Vec<DueExpiry> {
    let response = nodedb::control::server::broadcast::broadcast_to_all_cores(
        &node.shared,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        PhysicalPlan::Meta(MetaOp::KvDueExpiries {
            limit: 1024,
            skip_databases: Vec::new(),
        }),
        TraceId::generate(),
    )
    .await
    .expect("every core answers its due expiries");
    let due: Vec<DueExpiry> =
        zerompk::from_msgpack(response.payload.as_bytes()).expect("decode due expiries");
    due.into_iter()
        .filter(|entry| entry.collection == COLL)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_kv_key_is_reaped_alike_on_every_replica() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (key TEXT PRIMARY KEY) WITH (engine='kv')"
        ))
        .await
        .expect("create the kv collection");
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {COLL} (key, value, ttl) VALUES ('eph', 'temp', 1)"
        ))
        .await
        .unwrap_or_else(|e| panic!("write the expiring key: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut converged = true;
        for node in &cluster.nodes {
            if holds_key(&node.client).await || !due_on(node).await.is_empty() {
                converged = false;
            }
        }
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the expired key was not reaped on every replica within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Every replica applied the same log, the leader's reap included.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;
    for node in &cluster.nodes {
        assert!(due_on(node).await.is_empty(), "a replica kept a due entry");
    }
}

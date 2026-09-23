// SPDX-License-Identifier: BUSL-1.1
//! A proposal whose bytes commit twice applies once on every replica.
//!
//! A proposer re-proposes the same entry bytes after `RetryableLeaderChange`.
//! When the first copy also committed, the data group's log holds two copies
//! with one `idempotency_key`. The apply loop recognises the second copy by
//! that key and skips it.
//!
//! The test commits the same `KV_INCR` entry twice through the group leader's
//! raw proposer, which is exactly the log a double commit leaves behind. A
//! delta applied twice moves the counter twice, so every replica must read
//! the counter moved once.

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use nodedb::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

const COLL: &str = "dup_proposal_ctr";
const TENANT: u64 = 1;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn counter_on(client: &tokio_postgres::Client) -> Option<String> {
    let rows = client
        .simple_query(&format!("SELECT n FROM {COLL} WHERE key = 'ctr'"))
        .await
        .unwrap_or_else(|e| panic!("read counter: {}", pg_detail(&e)));
    rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proposal_committed_twice_moves_the_counter_once() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .expect("create the counter collection");
    cluster.nodes[0]
        .client
        .simple_query(&format!("INSERT INTO {COLL} (key, n) VALUES ('ctr', 5)"))
        .await
        .unwrap_or_else(|e| panic!("seed the counter: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, COLL);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut committed = 0;
    let mut entry_bytes: Option<Vec<u8>> = None;
    while committed < 2 {
        assert!(
            Instant::now() < deadline,
            "could not commit both copies of the proposal through the group leader"
        );
        let leader_id = {
            let routing = cluster.nodes[0]
                .shared
                .cluster_routing
                .as_ref()
                .expect("cluster_routing")
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let group = routing
                .group_for_vshard(vshard.as_u32())
                .expect("the counter's vShard maps to a data group");
            routing
                .group_info(group)
                .map(|info| info.leader)
                .unwrap_or(0)
        };
        let Some(leader) = cluster.nodes.iter().find(|n| n.node_id == leader_id) else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        // One entry, built once: both copies carry the same bytes and so the
        // same idempotency key, like a re-proposal after a leader change.
        let bytes = match &entry_bytes {
            Some(bytes) => bytes.clone(),
            None => {
                let surrogate = leader
                    .shared
                    .surrogate_assigner
                    .assign(DatabaseId::DEFAULT, TenantId::new(TENANT), COLL, b"ctr")
                    .expect("the seeded key has a surrogate");
                let plan = PhysicalPlan::Kv(KvOp::Incr {
                    collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                    key: b"ctr".to_vec(),
                    delta: 3,
                    ttl_ms: 0,
                    surrogate,
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                });
                let write =
                    ReplicableWrite::decide_for_replication(&plan).expect("KV_INCR replicates");
                let entry =
                    to_replicated_entry(TenantId::new(TENANT), DatabaseId::DEFAULT, vshard, &write)
                        .expect("encode the proposal")
                        .expect("KV_INCR encodes to a replicated entry");
                let bytes = entry.to_bytes();
                entry_bytes = Some(bytes.clone());
                bytes
            }
        };
        let proposer = leader
            .shared
            .raft_proposer
            .get()
            .expect("the leader has a raft proposer");
        match proposer(vshard.as_u32(), bytes) {
            Ok(_) => committed += 1,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for node in &cluster.nodes {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut last = None;
        while Instant::now() < deadline {
            last = counter_on(&node.client).await;
            if last.as_deref() != Some("5") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            last.as_deref(),
            Some("8"),
            "node {} must hold the counter moved once by the proposal committed twice \
             (5 + 3); 11 means the second copy applied again",
            node.node_id
        );
    }

    for node in cluster.nodes {
        node.shutdown().await;
    }
}

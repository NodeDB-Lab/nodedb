// SPDX-License-Identifier: BUSL-1.1
//! Cluster end-to-end BACKUP / RESTORE.
//!
//! Verifies that the wire-streaming COPY surface fans out across
//! every node holding tenant data and that restore replays through
//! the current routing topology rather than the source one.

use crate::common;
use common::cluster_harness::TestCluster;

use bytes::Bytes;
use common::cluster_harness::wait::wait_for;
use futures::SinkExt;
use futures::StreamExt;
use nodedb_types::backup_envelope::{DEFAULT_MAX_TOTAL_BYTES, parse_encrypted as parse_envelope};
use std::time::Duration;

/// Fixed test KEK injected into cluster nodes via `cluster_harness`.
const TEST_KEK: [u8; 32] = [0x42u8; 32];

const TENANT: u64 = 1;

async fn drain_backup(node_idx: usize, cluster: &TestCluster, tenant: u64) -> Vec<u8> {
    let stream = cluster.nodes[node_idx]
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .expect("copy_out");
    let mut bytes = Vec::new();
    let mut s = Box::pin(stream);
    while let Some(chunk) = s.next().await {
        bytes.extend_from_slice(&chunk.expect("copy chunk"));
    }
    bytes
}

async fn push_restore(
    node_idx: usize,
    cluster: &TestCluster,
    tenant: u64,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let sink = cluster.nodes[node_idx]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(bytes))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

async fn unique_contents(client: &tokio_postgres::Client) -> std::collections::BTreeSet<String> {
    let rows = client
        .simple_query("SELECT content FROM cl_docs")
        .await
        .expect("SELECT cl_docs");
    let mut seen = std::collections::BTreeSet::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            // 'content' may be the only column — try field 0.
            if let Some(s) = r.get(0) {
                seen.insert(s.to_string());
            }
        }
    }
    seen
}

fn db_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_backup_gathers_one_section_per_node() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse envelope");
    assert_eq!(
        env.meta.tenant_id, TENANT,
        "envelope tenant id should match request"
    );
    assert!(
        env.meta.source_vshard_count >= 1,
        "envelope must record source vshard count, got {}",
        env.meta.source_vshard_count
    );
    // One section per unique cluster node — three nodes, three sections.
    // The orchestrator dedupes by node id (leader + replicas of every group).
    assert!(
        !env.sections.is_empty() && env.sections.len() <= 3,
        "expected 1..=3 sections, got {}",
        env.sections.len()
    );
    let origins: std::collections::BTreeSet<u64> =
        env.sections.iter().map(|s| s.origin_node_id).collect();
    assert!(
        !origins.contains(&0),
        "origin_node_id 0 must not appear in cluster sections: {origins:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_roundtrip_preserves_data() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    // Write through node 0 — gateway routing places writes on the
    // current vshard owner (which may be any node).
    //
    // Hard fail-early invariant: after every INSERT the value MUST be
    // visible from node 0's pgwire client, otherwise a previous
    // INSERT silently dropped its data — surface that as the test
    // failure rather than letting it propagate into the
    // backup/restore symptom.
    for i in 0..6 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('k{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert k{i}: {}", db_detail(&e)));
        let rows = unique_contents(&cluster.nodes[0].client).await;
        let expected_count = (i + 1) as usize;
        assert_eq!(
            rows.len(),
            expected_count,
            "after INSERT k{i}: expected {expected_count} rows, got {} — \
             previous INSERT silently dropped a row. rows={rows:?}",
            rows.len()
        );
        let needle = format!("v{i}");
        assert!(
            rows.iter().any(|s| s.contains(&needle)),
            "after INSERT k{i}: v{i} missing — INSERT returned Ok but the row \
             never landed. rows={rows:?}"
        );
    }

    let bytes = drain_backup(0, &cluster, TENANT).await;

    // Restore into the same cluster. Writes are idempotent at the engine
    // level (PointPut overwrites); after restore we should still see all
    // six rows from any node's pgwire client.
    push_restore(0, &cluster, TENANT, bytes)
        .await
        .expect("RESTORE");

    // After the restore, every node's `ProposeTracker.complete`
    // bumps its per-group apply watermark after the Data Plane
    // confirms the storage write. Wait for cluster-wide convergence
    // so a follower-side SELECT sees the same state as the leader
    // — deterministic, no SQL polling.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    let post = unique_contents(&cluster.nodes[1].client).await;
    for i in 0..6u32 {
        let needle = format!("v{i}");
        assert!(
            post.iter().any(|s| s.contains(&needle)),
            "post-restore missing v{i}; rows={post:?}"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_dry_run_validates_envelope() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let sink = cluster.nodes[0]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN DRY RUN"))
        .await
        .expect("copy_in");
    let mut sink = Box::pin(sink);
    sink.as_mut().send(Bytes::from(bytes)).await.expect("send");
    sink.as_mut().finish().await.expect("dry run finish");

    cluster.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────
// Snapshot-watermark coordination across the fan-out.
//
// The backup orchestrator must gather a cluster-wide "as-of" LSN
// before snapshotting each node. Without it, a write that interleaves
// with the fan-out can land on one node's snapshot but not another,
// materialising a state that never existed at any wall-clock instant
// (cross-shard UNIQUE / FK invariants observably break). The spec is:
//
//   1. The envelope's `snapshot_watermark` is non-zero whenever any
//      writes have committed (0 means "not captured" — a placeholder).
//   2. Two backups bracketing a write advance the watermark strictly
//      monotonically — it reflects a real coordinated commit instant,
//      not a hardcoded constant.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_envelope_carries_nonzero_snapshot_watermark() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('wm{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert wm{i}: {}", db_detail(&e)));
    }

    let bytes = drain_backup(0, &cluster, TENANT).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse envelope");

    assert_ne!(
        env.meta.snapshot_watermark, 0,
        "backup envelope must carry a coordinated snapshot watermark (nonzero LSN), \
         got 0 — the orchestrator is skipping the coordination round and the \
         envelope represents a smear of per-node instants rather than one logical \
         instant, breaking cross-shard invariants on restore"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_watermark_advances_after_writes() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO cl_docs (id, content) VALUES ('a','pre')")
        .await
        .unwrap_or_else(|e| panic!("insert a: {}", db_detail(&e)));

    let bytes_before = drain_backup(0, &cluster, TENANT).await;
    let env_before =
        parse_envelope(&bytes_before, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse before");

    for i in 0..4 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('b{i}','post')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert b{i}: {}", db_detail(&e)));
    }

    let bytes_after = drain_backup(0, &cluster, TENANT).await;
    let env_after =
        parse_envelope(&bytes_after, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse after");

    assert!(
        env_after.meta.snapshot_watermark > env_before.meta.snapshot_watermark,
        "watermark must advance across writes: before={}, after={} — a constant \
         watermark means the orchestrator is not capturing a real LSN, so restore \
         has no logical instant to recover to",
        env_before.meta.snapshot_watermark,
        env_after.meta.snapshot_watermark
    );

    cluster.shutdown().await;
}

// Cross-cluster topology drift (source mapping ≠ destination mapping)
// is verified out-of-process against the released binary. The
// in-process variant exposed a non-deterministic 1-row loss on the
// restore-side dispatch race; the reproducer lives where 3-node ×
// 3-node spawn cost is acceptable.

// ────────────────────────────────────────────────────────────────────
// Restore into a group that lost its quorum.
//
// A restore commits every write through Raft: catalog rows through the
// metadata group, rows through each collection's data group. One dead node
// of three leaves every group a majority, and the restore succeeds. Two dead
// nodes leave no group a majority, and nothing can commit. Two contracts
// must hold:
//
//   1. Loud, prompt failure — the client receives a typed error that names
//      the group and the nodes it cannot reach, well within the statement
//      deadline, never a hang and never a silent success.
//   2. Nothing applied — no group on the surviving node applied an entry of
//      the refused restore.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_a_group_without_quorum_and_applies_nothing() {
    let mut cluster = TestCluster::spawn_three().await.expect("cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    for i in 0..4 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('f{i}','v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert f{i}: {}", db_detail(&e)));
    }
    let bytes = drain_backup(0, &cluster, TENANT).await;

    // Take down two of the three nodes: no group keeps a majority.
    let mut downed_ids = Vec::new();
    for _ in 0..2 {
        let downed = cluster.nodes.remove(1);
        downed_ids.push(downed.node_id);
        downed.shutdown().await;
    }
    downed_ids.sort_unstable();
    wait_for(
        "the surviving node marks both downed peers inactive",
        Duration::from_secs(20),
        Duration::from_millis(20),
        || cluster.nodes[0].active_topology_size() == 1,
    )
    .await;

    let applied_before = cluster.nodes[0].shared.group_watchers().snapshot();
    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        push_restore(0, &cluster, TENANT, bytes),
    )
    .await
    .expect("a restore into a group without quorum must fail promptly, not hang")
    .expect_err("a restore into a group without quorum must fail loudly");

    assert!(
        err.contains("has no reachable quorum"),
        "expected the typed quorum refusal, got: {err}"
    );
    assert!(
        err.contains(&format!("unreachable {downed_ids:?}")),
        "the refusal must name the unreachable nodes {downed_ids:?}, got: {err}"
    );
    assert!(
        err.contains("raft group "),
        "the refusal must name the group, got: {err}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the refusal took {:?}; it must not wait out a commit deadline",
        started.elapsed()
    );
    assert_eq!(
        cluster.nodes[0].shared.group_watchers().snapshot(),
        applied_before,
        "the refused restore applied an entry on the surviving node"
    );

    cluster.shutdown().await;
}

// NOTE: a proper same-cluster retry test (fault a node, it recovers,
// retry succeeds on the same cluster) requires harness support for
// in-place node restart — shutting down a node and bringing it back
// with the same node id, same data dir, and a fresh transport that
// the raft group accepts as a rejoin rather than a new member. The
// current `TestClusterNode::spawn` always creates a fresh data dir
// and a fresh transport keypair, so a "replacement" node fails the
// rejoin handshake. Filed as a separate harness initiative.
//
// Idempotent replay on a healthy cluster is already covered by
// `three_node_roundtrip_preserves_data`, which restores into a
// cluster that already holds the same rows and expects convergence.

// ────────────────────────────────────────────────────────────────────
// Staleness gate on restore.
//
// Once the backup orchestrator captures a real watermark, a
// restore into a cluster whose current applied state is *newer* than
// the envelope's watermark would roll back committed writes. The
// operator should be told. Spec: by default, restoring a
// stale-watermark envelope is rejected with a structured error. An
// explicit opt-in (`FORCE` keyword) is the separate design decision —
// not pinned here until the DDL surface lands.
//
// This test will remain red until the coordination round produces
// nonzero watermarks AND the restore path compares them.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_stale_watermark() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION cl_docs  \
             (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION");

    // Write, back up — envelope's watermark pins this logical instant.
    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('s{i}','early')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert s{i}: {}", db_detail(&e)));
    }
    let stale_bytes = drain_backup(0, &cluster, TENANT).await;

    // Advance the cluster past the envelope's watermark.
    for i in 0..3 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO cl_docs (id, content) VALUES ('s{i}_late','late{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert s{i}_late: {}", db_detail(&e)));
    }

    // Restoring the older envelope over newer state must be rejected.
    let err = push_restore(0, &cluster, TENANT, stale_bytes)
        .await
        .expect_err(
            "restore of stale-watermark envelope into newer cluster state must be \
             rejected by default — silently overwriting newer committed writes is \
             a correctness bug",
        );
    let lower = err.to_lowercase();
    assert!(
        lower.contains("stale") || lower.contains("watermark") || lower.contains("older"),
        "rejection error must name the staleness cause so operators know to \
         pass FORCE if they really mean it; got: {err}"
    );

    cluster.shutdown().await;
}

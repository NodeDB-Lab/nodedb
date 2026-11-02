// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster failover tests for the idempotent sync layer.
//!
//! Two invariants must survive a leader failover:
//!
//!   1. **Producer fencing (metadata Raft path).** A Lite client's
//!      `(producer_id, epoch)` is registered + fenced through the metadata
//!      Raft group, so every node holds the same registration. After the
//!      metadata leader fails over, the new leader fences a stale device
//!      exactly as the old one would have.
//!
//!   2. **Sync-write dedup (data Raft path).** A columnar sync insert is
//!      routed through Raft, so the idempotency gate's high-water mark
//!      advances on every replica when the entry applies. After the data
//!      leader fails over, the new leader still deduplicates a re-sent
//!      `(producer, seq)` — the HWM it advanced as a follower survives.
//!
//! Before Stage 5, sync writes bypassed Raft: followers never applied them,
//! so a post-failover leader started with an empty gate HWM and would
//! re-apply (double-write) a re-sent delta. These tests are the regression
//! guard for that fix — each test name contains `/cluster/` so nextest
//! serialises it.

use crate::common;
use common::cluster_harness::{TestCluster, wait::wait_for};

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use nodedb::control::server::sync::columnar_handler::{
    ColumnarDispatcher, SharedStateColumnarDispatcher,
};
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, VShardId};
use nodedb_types::TenantId;
use nodedb_types::sync::wire::{AckStatus, SyncAckResult};
use nodedb_types::value::Value;

// ── test 1: producer registration + fence epoch survive metadata failover ────

/// cluster/sync_producer_fence_survives_failover
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_sync_producer_fence_survives_failover() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    let leader_id = cluster.nodes[0].metadata_group_leader();
    assert_ne!(leader_id, 0, "no metadata-group leader elected");
    let leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .unwrap_or(0);

    const LITE_ID: &str = "device-failover-7x9";
    const TENANT: u64 = 0;
    const USER: u64 = 7;
    const CREATED_MS: i64 = 1_700_000_000_000;

    // Mirror the handshake exactly: allocate + write locally on the leader,
    // then replicate the registration through the metadata Raft group.
    let producer_id = {
        let reg = cluster.nodes[leader_idx]
            .shared
            .producer_registry
            .as_ref()
            .expect("producer_registry present on leader");
        let registration = reg
            .register(LITE_ID, TENANT, USER, 1, CREATED_MS)
            .expect("local register");
        nodedb::control::metadata_proposer::propose_sync_producer_register(
            &cluster.nodes[leader_idx].shared,
            LITE_ID,
            registration.producer_id,
            TENANT,
            USER,
            registration.current_epoch,
            CREATED_MS,
        )
        .await
        .expect("propose producer register");
        registration.producer_id
    };

    // Advance the fence epoch and replicate it, as the handshake does on an
    // epoch bump.
    {
        let reg = cluster.nodes[leader_idx]
            .shared
            .producer_registry
            .as_ref()
            .expect("producer_registry present on leader");
        reg.fence(LITE_ID, 5).expect("local fence");
        nodedb::control::metadata_proposer::propose_sync_producer_fence(
            &cluster.nodes[leader_idx].shared,
            LITE_ID,
            5,
        )
        .await
        .expect("propose producer fence");
    }

    // Every node converges on the replicated registration (producer_id, epoch=5).
    wait_for(
        "all nodes have the replicated registration at epoch=5",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| {
                n.shared
                    .producer_registry
                    .as_ref()
                    .and_then(|r| r.get(LITE_ID).ok().flatten())
                    .map(|reg| reg.producer_id == producer_id && reg.current_epoch == 5)
                    .unwrap_or(false)
            })
        },
    )
    .await;

    // Fail over the metadata leader.
    let leader_node = cluster.nodes.remove(leader_idx);
    leader_node.shutdown().await;
    assert_eq!(cluster.nodes.len(), 2);

    wait_for(
        "two survivors elect a new metadata-group leader",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || {
            let leaders: Vec<u64> = cluster
                .nodes
                .iter()
                .map(|n| n.metadata_group_leader())
                .collect();
            let first = leaders[0];
            first != 0 && first != leader_id && leaders.iter().all(|&l| l == first)
        },
    )
    .await;

    // The fence epoch survives on both survivors: a device presenting an
    // epoch below 5 would still be rejected, exactly as before the failover.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let reg = node
            .shared
            .producer_registry
            .as_ref()
            .expect("producer_registry present on survivor")
            .get(LITE_ID)
            .expect("registry get")
            .expect("registration present after failover");
        assert_eq!(
            reg.producer_id, producer_id,
            "survivor {idx} producer_id must survive failover"
        );
        assert_eq!(
            reg.current_epoch, 5,
            "survivor {idx} fence epoch must survive failover"
        );
    }

    let mut nodes = cluster.nodes;
    while let Some(node) = nodes.pop() {
        node.shutdown().await;
    }
}

// ── test 2: columnar sync-write dedup survives data-group failover ───────────

/// The superuser the cluster's pgwire sessions authenticate as, from
/// `shared`'s own credential store: the identity whose DDL created the
/// collection, in its tenant.
fn cluster_superuser(shared: &SharedState) -> AuthenticatedIdentity {
    shared
        .credentials
        .to_identity("nodedb", AuthMethod::Trust)
        .expect("the cluster's superuser")
}

/// Constant context for a columnar sync send: which node, route, and producer.
/// The varying `seq`/`val` are passed per call.
struct ColumnarSendCtx<'a> {
    shared: &'a Arc<SharedState>,
    tenant: TenantId,
    vshard: VShardId,
    collection: &'a str,
    producer: u64,
}

/// Dispatch one columnar sync row with provenance `(producer, epoch=1, seq)`
/// through the real sync dispatcher (which routes through Raft).
/// Returns the gate verdict, or an `Err` string on transient dispatch failure
/// (e.g. the data group is mid-election).
async fn dispatch_columnar_seq(
    ctx: &ColumnarSendCtx<'_>,
    seq: u64,
    val: &str,
) -> Result<AckStatus, String> {
    let identity = cluster_superuser(ctx.shared);
    let dispatcher =
        SharedStateColumnarDispatcher::new(ctx.shared.as_ref(), &identity, DatabaseId::DEFAULT);
    let rows = vec![vec![
        Value::Integer(seq as i64),
        Value::String(val.to_string()),
    ]];
    let provenance = nodedb_types::sync::wire::SyncProvenance {
        producer_id: ctx.producer,
        epoch: 1,
        stream_id: nodedb_types::sync::wire::stream_id_for(
            nodedb_types::sync::wire::EngineKind::Columnar,
            ctx.collection,
        ),
        seq,
    };
    let payload = dispatcher
        .dispatch_insert(
            ctx.tenant,
            ctx.vshard,
            ctx.collection.to_string(),
            rows,
            Vec::new(),
            provenance,
        )
        .await
        .map_err(|e| e.to_string())?;
    let ack: SyncAckResult = zerompk::from_msgpack(&payload).map_err(|e| e.to_string())?;
    ack.status()
        .ok_or_else(|| format!("columnar ingest was terminally refused: {:?}", ack.outcome))
}

/// Retry `dispatch_columnar_seq` on transient dispatch errors (the data
/// vshard may still be electing a leader) and return the gate verdict.
async fn dispatch_with_retry(
    ctx: &ColumnarSendCtx<'_>,
    seq: u64,
    val: &str,
    label: &str,
) -> AckStatus {
    let mut last_error = String::new();
    for _ in 0..120 {
        match dispatch_columnar_seq(ctx, seq, val).await {
            Ok(status) => return status,
            Err(error) => {
                last_error = error;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!("dispatch never succeeded: {label}; last error: {last_error}");
}

/// cluster/sync_columnar_dedup_survives_failover
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_sync_columnar_dedup_survives_failover() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    const COLL: &str = "csync_failover";
    const PRODUCER: u64 = 7777;
    // The sync writes as the superuser, into the tenant its DDL created the
    // collection in.
    let tenant = cluster_superuser(&cluster.nodes[0].shared).tenant_id;
    let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, COLL).vshard();

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (id BIGINT, val TEXT) WITH (engine='columnar')"
        ))
        .await
        .expect("CREATE columnar collection");

    // First send of seq=1: the gate admits it (Applied). A transient
    // error-after-commit could make the first observed verdict Duplicate, so
    // accept either — the point is that seq=1 is now recorded on the leader.
    let ctx_pre = ColumnarSendCtx {
        shared: &cluster.nodes[0].shared,
        tenant,
        vshard,
        collection: COLL,
        producer: PRODUCER,
    };
    let first = dispatch_with_retry(&ctx_pre, 1, "a", "first seq=1").await;
    assert!(
        matches!(first, AckStatus::Applied | AckStatus::Duplicate),
        "first seq=1 must be admitted, got {first:?}"
    );

    // A definite re-send of seq=1 is deduplicated by the gate on the leader.
    let resend = dispatch_with_retry(&ctx_pre, 1, "a", "re-send seq=1 (pre-failover)").await;
    assert_eq!(
        resend,
        AckStatus::Duplicate,
        "re-sent seq=1 must be deduped on the leader before failover"
    );

    // ── Fail over the data-group leader ──────────────────────────────────────
    let data_leader_id = cluster.nodes[0].data_group_leader();
    assert_ne!(data_leader_id, 0, "no data-group leader elected");
    let data_leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == data_leader_id)
        .unwrap_or(0);
    let data_leader = cluster.nodes.remove(data_leader_idx);
    data_leader.shutdown().await;
    assert_eq!(cluster.nodes.len(), 2);

    wait_for(
        "two survivors elect a new data-group leader",
        Duration::from_secs(20),
        Duration::from_millis(100),
        || {
            let leaders: Vec<u64> = cluster
                .nodes
                .iter()
                .map(|n| n.data_group_leader())
                .collect();
            let first = leaders[0];
            first != 0 && first != data_leader_id && leaders.iter().all(|&l| l == first)
        },
    )
    .await;

    // THE KEYSTONE: a re-send of seq=1 against the NEW leader must still be
    // deduped. This only holds if the survivor advanced its gate HWM when it
    // applied the replicated write as a follower — i.e. the write was routed
    // through Raft (Stage 5R-b). A survivor whose gate starts
    // empty would re-apply this delta instead of deduping it.
    let ctx_post = ColumnarSendCtx {
        shared: &cluster.nodes[0].shared,
        tenant,
        vshard,
        collection: COLL,
        producer: PRODUCER,
    };
    let after_failover =
        dispatch_with_retry(&ctx_post, 1, "a", "re-send seq=1 (post-failover)").await;
    assert_eq!(
        after_failover,
        AckStatus::Duplicate,
        "re-sent seq=1 must still be deduped after data-group failover — \
         the gate HWM must survive on the new leader"
    );

    // The gate still advances normally post-failover: a fresh seq=2 applies.
    let next = dispatch_with_retry(&ctx_post, 2, "b", "seq=2 (post-failover)").await;
    assert_eq!(
        next,
        AckStatus::Applied,
        "a fresh seq=2 must be admitted by the new leader's gate"
    );

    let mut nodes = cluster.nodes;
    while let Some(node) = nodes.pop() {
        node.shutdown().await;
    }
}

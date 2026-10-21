// SPDX-License-Identifier: BUSL-1.1

//! A webhook sink delivers every event once, from one node, across a
//! failover.
//!
//! Every node runs a delivery task for the stream, but only the node that
//! holds the leader lease of the stream's owning group delivers, and it
//! commits the group's offsets through the metadata log. So:
//!
//! - every event reaches the endpoint exactly once while the cluster is
//!   stable;
//! - after the delivering node dies, the next lease holder resumes from the
//!   committed offsets: every later event arrives once, and no earlier one
//!   arrives again;
//! - every delivery carries the owner's fencing token, and the next owner's
//!   token is higher.

use crate::common;
use common::cluster_harness::{TestCluster, wait_for};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use nodedb::event::cdc::consume::{ConsumeParams, consume_local};
use nodedb::event::cdc::sink_owner::owning_group;
use nodedb_types::DatabaseId;

const COLLECTION: &str = "webhook_owner_rows";
const STREAM: &str = "webhook_owner_feed";
const TENANT: u64 = 1;
const BEFORE: usize = 5;
const AFTER: usize = 4;
const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// Deliveries per `X-Idempotency-Key`, which names one event.
pub(super) type Deliveries = Arc<Mutex<HashMap<String, usize>>>;

/// The `X-Fencing-Token` of every delivery, in arrival order.
pub(super) type FencingTokens = Arc<Mutex<Vec<u64>>>;

/// Serve HTTP/1.1 POSTs on `listener`, counting each event's deliveries and
/// recording each delivery's fencing token.
pub(super) async fn serve(listener: TcpListener, deliveries: Deliveries, tokens: FencingTokens) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let deliveries = Arc::clone(&deliveries);
        let tokens = Arc::clone(&tokens);
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            loop {
                let mut key = None;
                let mut token = None;
                let mut length = 0usize;
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                    let header = line.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let lower = header.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("x-idempotency-key:") {
                        key = header.split_once(':').map(|(_, v)| v.trim().to_owned());
                    }
                    if let Some(value) = lower.strip_prefix("x-fencing-token:") {
                        token = value.trim().parse::<u64>().ok();
                    }
                }
                let mut body = vec![0u8; length];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                if let Some(token) = token {
                    tokens.lock().unwrap_or_else(|p| p.into_inner()).push(token);
                }
                if let Some(key) = key {
                    *deliveries
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .entry(key)
                        .or_default() += 1;
                }
                if write
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
    }
}

pub(super) fn distinct(deliveries: &Deliveries) -> usize {
    deliveries.lock().unwrap_or_else(|p| p.into_inner()).len()
}

pub(super) fn duplicates(deliveries: &Deliveries) -> Vec<(String, usize)> {
    deliveries
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(key, count)| (key.clone(), *count))
        .collect()
}

async fn insert(cluster: &TestCluster, node: usize, row: usize) {
    let sql = format!("INSERT INTO {COLLECTION} {{ id: 'row-{row}', n: {row} }}");
    let deadline = Instant::now() + CONVERGE;
    loop {
        match cluster.nodes[node].client.simple_query(&sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(row, %error, "insert not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("insert row-{row}: {error}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_webhook_delivers_every_event_once_across_a_failover() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    // The egress policy refuses an IP-literal host, so the URL names the
    // loopback listener by DNS name.
    let url = format!(
        "http://localhost:{}/hook",
        listener.local_addr().expect("addr").port()
    );
    let deliveries: Deliveries = Arc::default();
    let tokens: FencingTokens = Arc::default();
    let server = tokio::spawn(serve(
        listener,
        Arc::clone(&deliveries),
        Arc::clone(&tokens),
    ));

    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE CHANGE STREAM {STREAM} ON {COLLECTION} WITH (URL = '{url}')"
        ))
        .await
        .expect("create change stream");
    wait_for("every node registers the stream", CONVERGE, STEP, || {
        cluster
            .nodes
            .iter()
            .all(|node| node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM))
    })
    .await;

    for row in 0..BEFORE {
        insert(&cluster, 0, row).await;
    }
    wait_for("the endpoint receives every event", CONVERGE, STEP, || {
        distinct(&deliveries) == BEFORE
    })
    .await;

    // The delivering node commits after each batch. Once every node's
    // committed offsets cover every buffered event, no survivor redelivers.
    let group_name = format!("_webhook:{STREAM}");
    wait_for(
        "every node holds offsets past every event",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                let params = ConsumeParams {
                    database_id: DatabaseId::DEFAULT,
                    tenant_id: TENANT,
                    stream_name: STREAM,
                    group_name: &group_name,
                    partition: None,
                    limit: 1,
                };
                matches!(
                    consume_local(&node.shared, &params),
                    Ok(result) if result.events.is_empty()
                )
            })
        },
    )
    .await;
    // Let a stray second delivery surface before counting.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        duplicates(&deliveries).is_empty(),
        "an event was delivered twice while the cluster was stable: {:?}",
        duplicates(&deliveries)
    );

    let first_owner_tokens = tokens.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert_eq!(
        first_owner_tokens.len(),
        BEFORE,
        "every delivery carries a fencing token"
    );

    // Kill the node that delivers: the leader of the stream's owning group.
    let group = owning_group(&cluster.nodes[0].shared, DatabaseId::DEFAULT, STREAM)
        .expect("the stream's name maps to a data group");
    let leader = cluster.nodes[0]
        .all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0);
    let owner = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap_or_else(|| panic!("no live node leads the owning group {group}"));
    let dead = cluster.nodes.remove(owner);
    let dead_id = dead.node_id;
    dead.shutdown().await;
    wait_for("the survivors elect a new owner", CONVERGE, STEP, || {
        cluster.nodes.iter().all(|node| {
            node.all_group_leaders()
                .into_iter()
                .any(|(id, leader)| id == group && leader != 0 && leader != dead_id)
        })
    })
    .await;

    for row in BEFORE..BEFORE + AFTER {
        insert(&cluster, 0, row).await;
    }
    wait_for(
        "the new owner delivers the later events",
        CONVERGE,
        STEP,
        || distinct(&deliveries) == BEFORE + AFTER,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        duplicates(&deliveries).is_empty(),
        "the failover delivered an event twice: {:?}",
        duplicates(&deliveries)
    );
    let all_tokens = tokens.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let first_owner_max = first_owner_tokens.iter().copied().max().unwrap_or(0);
    assert!(
        all_tokens[first_owner_tokens.len()..]
            .iter()
            .all(|token| *token > first_owner_max),
        "the new owner's fencing token rises above the old owner's: {all_tokens:?}"
    );

    server.abort();
    cluster.shutdown().await;
}

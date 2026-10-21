// SPDX-License-Identifier: BUSL-1.1

//! A webhook sink delivers every event once when the stream's rows live on
//! a node that does not run the sink.
//!
//! With replication factor 1 on 3 nodes, each data group has one member. The
//! test picks a collection whose group lives on a different node than the
//! stream's owning group. The sink owner holds none of the stream's events
//! in its own buffer, so it reads them from the collection's group member
//! over the remote consume path, and commits the replicated offsets. Every
//! event must reach the endpoint exactly once.

use crate::common;
use common::cluster_harness::{TestCluster, wait_for};

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;

use nodedb::event::cdc::sink_owner::owning_group;
use nodedb_types::{CollectionKey, DatabaseId};

use super::webhook_sink_single_owner::{Deliveries, FencingTokens, distinct, duplicates, serve};

const STREAM: &str = "webhook_rf1_feed";
const TENANT: u64 = 1;
const ROWS: usize = 6;
const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// The sole member of data group `group_id`.
fn sole_member(cluster: &TestCluster, group_id: u64) -> u64 {
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("a cluster node has a routing table");
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let members = routing
        .group_info(group_id)
        .map(|info| info.members.clone())
        .unwrap_or_default();
    assert_eq!(
        members.len(),
        1,
        "replication factor 1 gives group {group_id} one member: {members:?}"
    );
    members[0]
}

/// The data group a collection's rows route to.
fn collection_group(cluster: &TestCluster, collection: &str) -> u64 {
    let vshard = nodedb_cluster::routing::vshard_for_collection(CollectionKey::from_bare(
        DatabaseId::DEFAULT,
        collection,
    ));
    let routing = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("a cluster node has a routing table");
    routing
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(vshard)
        .expect("every vShard maps to a group")
}

/// A collection name whose group lives on another node than `sink_node`.
fn remote_collection(cluster: &TestCluster, sink_node: u64) -> String {
    (0..256)
        .map(|i| format!("webhook_rf1_rows_{i}"))
        .find(|name| sole_member(cluster, collection_group(cluster, name)) != sink_node)
        .expect("some collection name routes to another node's group")
}

async fn insert(cluster: &TestCluster, collection: &str, row: usize) {
    let sql = format!("INSERT INTO {collection} {{ id: 'row-{row}', n: {row} }}");
    let deadline = Instant::now() + CONVERGE;
    loop {
        match cluster.nodes[0].client.simple_query(&sql).await {
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
async fn a_webhook_reads_events_from_the_node_that_holds_them() {
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

    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("spawn 3-node cluster with replication factor 1");
    let stream_group = owning_group(&cluster.nodes[0].shared, DatabaseId::DEFAULT, STREAM)
        .expect("the stream's name maps to a data group");
    let sink_node = sole_member(&cluster, stream_group);
    let collection = remote_collection(&cluster, sink_node);

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE CHANGE STREAM {STREAM} ON {collection} WITH (URL = '{url}')"
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

    for row in 0..ROWS {
        insert(&cluster, &collection, row).await;
    }
    wait_for("the endpoint receives every event", CONVERGE, STEP, || {
        distinct(&deliveries) == ROWS
    })
    .await;
    // Let a stray second delivery surface before counting.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        distinct(&deliveries),
        ROWS,
        "the endpoint receives exactly the stream's events"
    );
    assert!(
        duplicates(&deliveries).is_empty(),
        "an event was delivered twice: {:?}",
        duplicates(&deliveries)
    );

    server.abort();
    cluster.shutdown().await;
}

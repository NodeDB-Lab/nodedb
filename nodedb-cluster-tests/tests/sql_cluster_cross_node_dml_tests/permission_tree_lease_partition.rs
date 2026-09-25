// SPDX-License-Identifier: BUSL-1.1

//! A partitioned node loses its authorization lease, and a revoke
//! acknowledged meanwhile never plans on it.
//!
//! Node B is severed from the other two nodes. It cannot renew its lease, so
//! the revoke written on another node is acknowledged once B's lease lapses.
//! From then on B refuses every permission-checked statement with a
//! retryable error. After the partition heals, B renews only once its own
//! state covers the revoke, so its first answer shows the revoke.

use std::time::{Duration, Instant};

use crate::common::cluster_harness::TestCluster;

const SELECT_DOCS: &str = "SELECT id FROM ptl_docs ORDER BY id";
const AUTHORIZATION_BEHIND: &str = "55P03";

#[derive(Debug, PartialEq)]
enum Read {
    Rows(Vec<String>),
    Refused,
}

async fn probe_read(client: &tokio_postgres::Client) -> Read {
    match client.simple_query(SELECT_DOCS).await {
        Ok(messages) => Read::Rows(
            messages
                .into_iter()
                .filter_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        Some(row.get(0).unwrap_or("").to_string())
                    }
                    _ => None,
                })
                .collect(),
        ),
        Err(error) => {
            assert_eq!(
                error.code().map(|code| code.code().to_string()).as_deref(),
                Some(AUTHORIZATION_BEHIND),
                "a probe read failed with an error other than a retryable refusal: {error:?}"
            );
            Read::Refused
        }
    }
}

async fn connect_probe(
    pg_addr: std::net::SocketAddr,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={} port={} user=ptl_probe dbname=default",
        pg_addr.ip(),
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as ptl_probe to {pg_addr}: {e}"));
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}

/// Sever node `b` from every other node, both ways, or heal it.
fn partition(cluster: &TestCluster, b: usize, severed: bool) {
    let b_id = cluster.nodes[b].node_id;
    let b_transport = cluster.nodes[b]
        .shared
        .cluster_transport
        .as_ref()
        .expect("cluster transport");
    for (index, node) in cluster.nodes.iter().enumerate() {
        if index == b {
            continue;
        }
        let transport = node
            .shared
            .cluster_transport
            .as_ref()
            .expect("cluster transport");
        if severed {
            transport.sever(b_id);
            b_transport.sever(node.node_id);
        } else {
            transport.heal(b_id);
            b_transport.heal(node.node_id);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_partitioned_node_refuses_until_it_covers_the_revoke() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for sql in [
        "CREATE COLLECTION ptl_docs (id TEXT PRIMARY KEY, title TEXT) \
         WITH (engine='document_strict')",
        "CREATE COLLECTION ptl_grants",
        "CREATE ROLE ptl_role",
        // `readwrite` is the built-in role that grants Read on every
        // collection. An unknown name such as `read_write` is a custom role
        // with no permissions, and every read would be denied.
        "CREATE USER ptl_probe WITH PASSWORD 'ptl-probe-password' ROLE readwrite",
        "GRANT ROLE ptl_role TO ptl_probe",
    ] {
        cluster
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // B leads neither the metadata group nor the group homing the grants,
    // so both keep a quorum while B is cut off.
    let grants_group = cluster.nodes[0]
        .group_id_for_collection("ptl_grants")
        .expect("grants group");
    let metadata_leader = cluster.nodes[0].metadata_group_leader();
    let grants_leader = cluster.nodes[0]
        .all_group_leaders()
        .into_iter()
        .find(|(group, _)| *group == grants_group)
        .map(|(_, leader)| leader)
        .unwrap_or(0);
    let b = cluster
        .nodes
        .iter()
        .position(|node| node.node_id != metadata_leader && node.node_id != grants_leader)
        .expect("a node that leads neither group");
    let writer = &cluster.nodes[(b + 1) % cluster.nodes.len()];

    writer
        .exec("INSERT INTO ptl_docs (id, title) VALUES ('d1', 'Doc One')")
        .await
        .expect("insert d1");

    // The probe's base Read is in effect on every node, B included, before
    // the tree narrows it. A denial here is a setup error, not a tree result.
    for (index, node) in cluster.nodes.iter().enumerate() {
        let (probe, handle) = connect_probe(node.pg_addr).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let read = probe_read(&probe).await;
            if read == Read::Rows(vec!["d1".to_string()]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {index}: base Read before the tree: last read {read:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(probe);
        handle.abort();
    }

    cluster
        .exec_ddl_on_any_leader(
            "ALTER COLLECTION ptl_docs SET PERMISSION_TREE = '{\
                \"resource_column\":\"id\",\
                \"graph_index\":\"ptl_docs_tree\",\
                \"permission_table\":\"ptl_grants\"\
             }'",
        )
        .await
        .expect("set permission tree");
    writer
        .exec(
            "INSERT INTO ptl_grants (resource_id, grantee, level, inherited) \
             VALUES ('d1', 'ptl_role', 'viewer', false)",
        )
        .await
        .expect("grant d1");

    let (probe, probe_handle) = connect_probe(cluster.nodes[b].pg_addr).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match probe_read(&probe).await {
            Read::Rows(rows) if rows == vec!["d1".to_string()] => break,
            Read::Rows(rows) => panic!("node B served {rows:?} after the grant was acknowledged"),
            Read::Refused => {}
        }
        assert!(Instant::now() < deadline, "node B never served the grant");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    partition(&cluster, b, true);
    writer
        .exec("DELETE FROM ptl_grants WHERE resource_id = 'd1' AND grantee = 'ptl_role'")
        .await
        .expect("the revoke is acknowledged once B's lease lapses");

    // B's lease has lapsed: it refuses rather than plan against the grant.
    for _ in 0..5 {
        assert_eq!(
            probe_read(&probe).await,
            Read::Refused,
            "the partitioned node planned without a lease"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    partition(&cluster, b, false);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match probe_read(&probe).await {
            Read::Rows(rows) => {
                assert!(rows.is_empty(), "node B served the revoked grant: {rows:?}");
                break;
            }
            Read::Refused => {}
        }
        assert!(
            Instant::now() < deadline,
            "node B never renewed after the partition healed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(probe);
    probe_handle.abort();
    cluster.shutdown().await;
}

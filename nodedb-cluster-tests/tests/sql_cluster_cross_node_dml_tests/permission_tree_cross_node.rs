// SPDX-License-Identifier: BUSL-1.1

//! A permission-tree revoke acknowledged on one node binds the next
//! statement on every other node.
//!
//! A grant is a row in the tree's permission table. Node B learns of it when
//! its own replica applies the write and its Event Plane updates its
//! permission cache. Node A acknowledges the write only after every node
//! holding an authorization lease reported that coverage, or its lease
//! expired. So B's first statement after the acknowledgement either sees the
//! change or is refused with a retryable error. It never plans against the
//! grant before the revoke.
//!
//! The test repeats grant and revoke several rounds and reads on every other
//! node right after each acknowledgement, with no wait in between.

use std::time::{Duration, Instant};

use crate::common::cluster_harness::TestCluster;

const PROBE_USER: &str = "ptx_probe";
const SELECT_DOCS: &str = "SELECT id FROM ptx_docs ORDER BY id";
const ROUNDS: usize = 5;

/// SQLSTATE of a statement refused because this node's authorization state
/// is behind. The client retries it.
const AUTHORIZATION_BEHIND: &str = "55P03";

/// The outcome of one probe read.
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
            let code = error.code().map(|code| code.code().to_string());
            assert_eq!(
                code.as_deref(),
                Some(AUTHORIZATION_BEHIND),
                "a probe read failed with an error other than a retryable refusal: {error:?}"
            );
            Read::Refused
        }
    }
}

/// Wait until `probe` reads exactly `expected`. A retryable refusal and a
/// replica still catching up are retried. A denial fails at once, in
/// [`probe_read`].
async fn await_rows(probe: &tokio_postgres::Client, expected: &[&str], what: &str) {
    let expected: Vec<String> = expected.iter().map(|row| row.to_string()).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let read = probe_read(probe).await;
        if read == Read::Rows(expected.clone()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: last read {read:?}, expected {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn connect_probe(
    pg_addr: std::net::SocketAddr,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={} port={} user={PROBE_USER} dbname=default",
        pg_addr.ip(),
        pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {PROBE_USER} to {pg_addr}: {e}"));
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn revoke_on_one_node_binds_the_next_statement_on_every_other_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for sql in [
        "CREATE COLLECTION ptx_docs (id TEXT PRIMARY KEY, title TEXT) \
         WITH (engine='document_strict')",
        "CREATE COLLECTION ptx_grants",
        "CREATE ROLE ptx_role",
        // `readwrite` is the built-in role that grants Read on every
        // collection. An unknown name such as `read_write` is a custom role
        // with no permissions, and every read would be denied.
        "CREATE USER ptx_probe WITH PASSWORD 'ptx-probe-password' ROLE readwrite",
        "GRANT ROLE ptx_role TO ptx_probe",
    ] {
        cluster
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let writer = &cluster.nodes[0];
    for sql in [
        "INSERT INTO ptx_docs (id, title) VALUES ('d1', 'Doc One')",
        "INSERT INTO ptx_docs (id, title) VALUES ('d2', 'Doc Two')",
    ] {
        writer
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    // The probe's base Read is in effect on every node, the writer included,
    // before the tree narrows it. A denial here is a setup error, not a
    // tree result.
    for (index, node) in cluster.nodes.iter().enumerate() {
        let (probe, handle) = connect_probe(node.pg_addr).await;
        await_rows(
            &probe,
            &["d1", "d2"],
            &format!("node {index}: base Read before the tree"),
        )
        .await;
        drop(probe);
        handle.abort();
    }

    cluster
        .exec_ddl_on_any_leader(
            "ALTER COLLECTION ptx_docs SET PERMISSION_TREE = '{\
                \"resource_column\":\"id\",\
                \"graph_index\":\"ptx_docs_tree\",\
                \"permission_table\":\"ptx_grants\"\
             }'",
        )
        .await
        .expect("set permission tree");

    let mut probes = Vec::new();
    for node in &cluster.nodes[1..] {
        probes.push(connect_probe(node.pg_addr).await);
    }

    // Reads that planned rather than being refused. A run of refusals only
    // proves nothing was served stale, so the test also requires answers.
    let mut answered = 0usize;
    for round in 0..ROUNDS {
        writer
            .exec(
                "INSERT INTO ptx_grants (resource_id, grantee, level, inherited) \
                 VALUES ('d1', 'ptx_role', 'viewer', false)",
            )
            .await
            .unwrap_or_else(|e| panic!("round {round}: grant: {e}"));
        for (index, (probe, _)) in probes.iter().enumerate() {
            let read = probe_read(probe).await;
            answered += usize::from(read != Read::Refused);
            assert!(
                read == Read::Rows(vec!["d1".to_string()]) || read == Read::Refused,
                "round {round}: node {} planned against the state before the grant: {read:?}",
                index + 1
            );
        }

        writer
            .exec("DELETE FROM ptx_grants WHERE resource_id = 'd1' AND grantee = 'ptx_role'")
            .await
            .unwrap_or_else(|e| panic!("round {round}: revoke: {e}"));
        for (index, (probe, _)) in probes.iter().enumerate() {
            let read = probe_read(probe).await;
            answered += usize::from(read != Read::Refused);
            assert!(
                read == Read::Rows(Vec::new()) || read == Read::Refused,
                "round {round}: node {} served d1 after the revoke was acknowledged: {read:?}",
                index + 1
            );
        }
    }

    assert!(answered > 0, "every probe read was refused");

    for (probe, handle) in probes {
        drop(probe);
        handle.abort();
    }
    cluster.shutdown().await;
}

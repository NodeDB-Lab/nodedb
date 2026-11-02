// SPDX-License-Identifier: BUSL-1.1

//! RESTORE and TRUNCATE of an edge collection order by their Calvin
//! sequence.
//!
//! A RESTORE re-issues each edge version at its original system time, as a
//! Calvin transaction applied at its own ordinal. A TRUNCATE hides the
//! versions applied below its ordinal. So a TRUNCATE sequenced before the
//! RESTORE leaves the restored edge, even though the edge's history predates
//! the TRUNCATE, and a TRUNCATE sequenced after the RESTORE hides it. The
//! TRUNCATE's tombstones of the collection's rows stay in their history, and
//! the RESTORE sets each row current again above them. A restart reads the
//! same.

use bytes::Bytes;
use futures::SinkExt;

use super::backup_support::drain_backup;
use crate::harness::TestServer;

const TENANT: u64 = 1;
const COLLECTION: &str = "ret_people";

/// Restore `envelope` into `tenant` with `FORCE`: the server holds writes
/// newer than the backup, which a plain RESTORE refuses as stale.
async fn force_restore(
    client: &tokio_postgres::Client,
    tenant: u64,
    envelope: Vec<u8>,
) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN FORCE"))
        .await
        .map_err(|e| format!("copy_in: {e:?}"))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| format!("send: {e:?}"))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Whether `GRAPH NEIGHBORS` of `node` in `direction` names `other`.
async fn linked(server: &TestServer, node: &str, other: &str, direction: &str) -> bool {
    server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{COLLECTION}' OF '{node}' LABEL 'knows' DIRECTION {direction}"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node}: {e}"))
        .join("")
        .contains(other)
}

/// Whether the edge alice -> bob reads back from both endpoints.
async fn edge_live(server: &TestServer) -> bool {
    let out = linked(server, "alice", "bob", "out").await;
    let inbound = linked(server, "bob", "alice", "in").await;
    assert_eq!(
        out, inbound,
        "both endpoint homes of alice -> bob agree on whether it is live"
    );
    out
}

/// Whether both rows read back as current.
async fn rows_live(server: &TestServer) -> bool {
    let rows = server
        .query_text(&format!("SELECT id FROM {COLLECTION}"))
        .await
        .unwrap_or_else(|e| panic!("select: {e}"))
        .join(" ");
    rows.contains("alice") && rows.contains("bob")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_hides_a_restored_edge_only_when_sequenced_after_the_restore() {
    let server = TestServer::start().await;
    for sql in [
        format!("CREATE COLLECTION {COLLECTION} WITH (engine='document_schemaless')"),
        format!("INSERT INTO {COLLECTION} {{ id: 'alice' }}"),
        format!("INSERT INTO {COLLECTION} {{ id: 'bob' }}"),
        format!("GRAPH INSERT EDGE IN '{COLLECTION}' FROM 'alice' TO 'bob' TYPE 'knows'"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(edge_live(&server).await, "the edge before the backup");
    let backup = drain_backup(&server.client, TENANT)
        .await
        .expect("take the backup");

    // A TRUNCATE sequenced before the RESTORE: the restored edge keeps its
    // history from before the TRUNCATE, and stays live.
    server
        .exec(&format!("TRUNCATE {COLLECTION}"))
        .await
        .expect("TRUNCATE commits");
    assert!(!edge_live(&server).await, "the TRUNCATE hides the edge");
    force_restore(&server.client, TENANT, backup.clone())
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert!(
        edge_live(&server).await,
        "a TRUNCATE sequenced before the RESTORE leaves the restored edge"
    );
    assert!(
        rows_live(&server).await,
        "the RESTORE sets the truncated rows current again"
    );

    // A TRUNCATE sequenced after the RESTORE hides the restored edge.
    server
        .exec(&format!("TRUNCATE {COLLECTION}"))
        .await
        .expect("second TRUNCATE commits");
    assert!(
        !edge_live(&server).await,
        "a TRUNCATE sequenced after the RESTORE hides the restored edge"
    );

    // A retry of the same RESTORE brings it back, and a restart reads the
    // same.
    force_restore(&server.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("second restore: {e}"));
    assert!(edge_live(&server).await, "the retried RESTORE");
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    assert!(edge_live(&server).await, "after a restart");
    assert!(rows_live(&server).await, "the rows after a restart");
}

// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's staleness guard reads the write marks of every data group, not
//! the restoring node's memory.
//!
//! With a replication factor of 1, each data group lives on one node. A write
//! after the backup applies only on that node. A restore issued on another
//! node, which never applied the write, still refuses: the guard asks the
//! group's replica for its newest write.

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};

use crate::common;
use common::cluster_harness::TestCluster;
use common::cluster_harness::wait::wait_for;

const TENANT: u64 = 1;
const COLLECTION: &str = "nonreplica_docs";

fn db_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn drain_backup(client: &tokio_postgres::Client) -> Vec<u8> {
    let stream = client
        .copy_out(&format!("COPY (BACKUP TENANT {TENANT}) TO STDOUT"))
        .await
        .unwrap_or_else(|e| panic!("copy_out: {}", db_detail(&e)));
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap_or_else(|e| panic!("chunk: {}", db_detail(&e))));
    }
    bytes
}

async fn push_restore(client: &tokio_postgres::Client, envelope: Vec<u8>) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_on_a_node_that_never_applied_the_write_refuses() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
        ))
        .await
        .expect("CREATE COLLECTION");
    let group_id = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("the collection's data group");
    // Every joiner enters every group as a learner. Placement convergence
    // then removes the nodes outside the group's placement, one per tick,
    // after a leadership transfer when the leader itself must leave. A
    // removed node keeps its mounted replica, so membership decides.
    wait_for(
        "exactly one node replicates the collection's group",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .filter(|node| node.replicates_data_group(group_id))
                .count()
                == 1
        },
    )
    .await;
    let restorer = cluster
        .nodes
        .iter()
        .find(|node| !node.replicates_data_group(group_id))
        .expect("a node that does not replicate the group");

    let backup = drain_backup(&restorer.client).await;
    restorer
        .client
        .simple_query(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('after', 'x')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert: {}", db_detail(&e)));
    assert!(
        restorer.shared.tenant_write_mark(TENANT).is_none(),
        "the restoring node applied no write of the tenant, so its memory holds no mark"
    );

    let error = push_restore(&restorer.client, backup)
        .await
        .expect_err("a write after the backup must refuse the restore on every node");
    assert!(
        error.contains("restore refused"),
        "expected the staleness refusal, got: {error}"
    );
    assert!(
        error.contains(COLLECTION),
        "the refusal must name the collection of the newer write, got: {error}"
    );

    cluster.shutdown().await;
}

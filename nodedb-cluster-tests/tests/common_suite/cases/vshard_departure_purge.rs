// SPDX-License-Identifier: BUSL-1.1

//! A node that leaves a vShard's data group purges its copy of the vShard's
//! data once the new owners serve it.
//!
//! Before the move the leaving node's own cores hold every engine's rows of
//! the departed vShards. After placement drops it from the group, every
//! engine's rows, CRDT included, still read back through the cluster, and
//! the leaving node's cores hold none of them. An edge with one endpoint on a vShard the node
//! keeps stays there.

use std::time::Duration;

use super::vshard_departure_fixture::DepartureFixture;
use crate::common::cluster_harness::wait_for_async;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_departed_vshard_is_purged_from_its_old_owner() {
    let mut fx = DepartureFixture::seed().await;

    let before = fx.stale_on(fx.leaving_node()).await;
    for kind in [
        "the edge both of whose endpoints left",
        "the label of a node that left",
        "array cells of a departed vShard",
        "the CRDT row of a departed vShard",
    ] {
        assert!(
            before.iter().any(|line| line == kind),
            "the leaving node holds {kind} before the move: {before:?}"
        );
    }
    for collection in fx.names.collections() {
        let line = format!("rows of {collection}");
        assert!(
            before.contains(&line),
            "the leaving node holds the rows of {collection} before the move: {before:?}"
        );
    }

    fx.depart().await;

    let probe = &fx;
    wait_for_async(
        "the leaving node purges every departed row",
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async move { probe.stale_on(probe.leaving_node()).await.is_empty() },
    )
    .await;
    assert!(
        fx.holds_kept_edge(fx.leaving_node()).await,
        "an edge with an endpoint on a vShard the node keeps stays"
    );

    let rows = fx
        .leaving_node()
        .shared
        .credentials
        .catalog()
        .load_vshard_residency()
        .expect("residency rows");
    for vshard in &fx.departed {
        assert!(
            rows.iter()
                .all(|(v, row)| v != vshard || (row.departed && row.purged)),
            "vShard {vshard} is departed and purged, or settled: {rows:?}"
        );
    }

    for node in &fx.cluster.nodes {
        fx.assert_served(node, &format!("through node {}", node.node_id))
            .await;
    }

    fx.cluster.shutdown().await;
}

// SPDX-License-Identifier: BUSL-1.1

//! A departure purge that a restart interrupts runs again at the next boot.
//!
//! The fail point `vshard_departure::purge` fails every purge on the leaving
//! node. The node records the departure, writes its barrier, and keeps the
//! stale rows. It then stops with the purge owed, and the fail point clears.
//! At the next boot the owed purge runs, and the node's cores hold none of
//! the departed rows, while every engine's rows still read back through the
//! cluster.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::vshard_departure_fixture::DepartureFixture;
use crate::common::cluster_harness::{wait_for, wait_for_async};

const PURGE_FAULT: &str = "vshard_departure::purge";

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_interrupted_departure_purge_completes_after_a_restart() {
    let mut fx = DepartureFixture::seed().await;
    let fault = FailGuard::for_node(
        fx.leaving,
        PURGE_FAULT,
        FailAction::Fail("injected departure purge fault".into()),
    );

    fx.depart().await;

    // The departure is recorded and owed; the purge itself never ran.
    let owed = |fx: &DepartureFixture| {
        let rows = fx
            .leaving_node()
            .shared
            .credentials
            .catalog()
            .load_vshard_residency()
            .expect("residency rows");
        fx.departed.iter().all(|vshard| {
            rows.iter()
                .any(|(v, row)| v == vshard && row.departed && !row.purged && row.barrier_lsn > 0)
        })
    };
    wait_for(
        "the leaving node records the departure and its barrier",
        Duration::from_secs(60),
        Duration::from_millis(250),
        || owed(&fx),
    )
    .await;
    let stale = fx.stale_on(fx.leaving_node()).await;
    assert!(
        !stale.is_empty(),
        "an interrupted purge leaves the stale rows in place"
    );

    let index = fx
        .cluster
        .nodes
        .iter()
        .position(|node| node.node_id == fx.leaving)
        .expect("leaving node index");
    let stopped = fx
        .cluster
        .stop_member(index)
        .await
        .expect("stop the leaving node");
    drop(fault);
    fx.cluster
        .restart_member(stopped)
        .await
        .expect("restart the leaving node");

    let probe = &fx;
    wait_for_async(
        "the restarted node runs the owed purge",
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async move { probe.stale_on(probe.leaving_node()).await.is_empty() },
    )
    .await;
    assert!(
        fx.holds_kept_edge(fx.leaving_node()).await,
        "an edge with an endpoint on a vShard the node keeps stays"
    );
    for node in &fx.cluster.nodes {
        fx.assert_served(
            node,
            &format!("after the restart, through node {}", node.node_id),
        )
        .await;
    }

    fx.cluster.shutdown().await;
}

// SPDX-License-Identifier: BUSL-1.1

//! A new vShard 0 leader whose catalog lags the schedule mark skips the tick
//! instead of rerunning the due minute.

use nodedb::control::backup::schedule::envelope_name;
use nodedb::control::cluster::metadata_applier::BACKUP_MARK_FAIL_POINT;
use nodedb_test_support::fail_point::{FailAction, FailGuard};

use super::fixture::{DATABASE, Fixture};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_whose_catalog_lags_never_reruns_the_due_minute() {
    let mut fx = Fixture::new().await;
    let due = fx.due;
    let leader = fx.leader();

    fx.tick_all((due - 1) * 60 + 5).await;
    fx.wait_marks("every node holds the armed mark", due - 1)
        .await;

    // Hold back every other node's apply of the next mark.
    let guards: Vec<FailGuard> = fx
        .cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != leader)
        .map(|node| {
            FailGuard::for_node(
                node.node_id,
                BACKUP_MARK_FAIL_POINT,
                FailAction::Fail("held back by the test".to_string()),
            )
        })
        .collect();

    // The leader runs `due`. Only its own catalog holds the new mark.
    fx.tick_node(leader, due * 60 + 5).await;
    let leader_node = fx
        .cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader)
        .expect("leader node");
    assert_eq!(
        fx.runs(leader_node),
        (1, 0),
        "the leader runs the due minute"
    );
    assert_eq!(fx.envelopes(), [envelope_name(DATABASE, due * 60_000)]);

    fx.kill_leader().await;
    assert!(
        fx.local_marks().iter().all(|mark| *mark == Some(due - 1)),
        "the survivors' catalogs lag the mark: {:?}",
        fx.local_marks()
    );

    // The new leader's catalog shows `due` as due, but its read of the mark
    // cannot be confirmed while its catalog lags, so the tick is skipped.
    fx.tick_all((due + 1) * 60 + 5).await;
    fx.tick_all((due + 1) * 60 + 35).await;
    for node in &fx.cluster.nodes {
        assert_eq!(
            fx.runs(node),
            (0, 0),
            "node {} runs nothing on a lagging catalog",
            node.node_id
        );
    }

    // Released, the survivors apply the mark, and nothing is due.
    drop(guards);
    fx.wait_marks("every survivor applies the mark of the due minute", due)
        .await;
    fx.tick_all((due + 2) * 60 + 5).await;
    for node in &fx.cluster.nodes {
        assert_eq!(
            fx.runs(node),
            (0, 0),
            "node {} never reruns the due minute",
            node.node_id
        );
    }
    assert_eq!(fx.envelopes(), [envelope_name(DATABASE, due * 60_000)]);

    fx.shutdown().await;
}

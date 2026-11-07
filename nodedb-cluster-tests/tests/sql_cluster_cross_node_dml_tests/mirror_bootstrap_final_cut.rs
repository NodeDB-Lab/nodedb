// SPDX-License-Identifier: BUSL-1.1

//! A promoted mirror issues values above every value its source's last cut held.
//!
//! The test runs two clusters joined the way an operator joins them.

use std::time::Duration;

use nodedb::control::mirror::source::cut_loop::CUT_BARRIER_FAIL_POINT;
use nodedb_types::MirrorStatus;
use nodedb_types::fail_point::{FailAction, FailGuard};

use super::mirror_bootstrap_fixture::{
    SOURCE_NAME, await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings,
    read_mirror, rows, serve_source,
};
use super::mirror_bootstrap_recover_fixture::{COPY, seeded_source, status_on};
use crate::common::cluster_harness::{TestCluster, wait_for};

/// A cut's mark reads its metadata index ahead of its barriers. A value issued
/// between the two sits in the cut, with its reservation past the mark. The source
/// then dies, so no later cut covers it, and the promoted mirror still issues above
/// the value.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_mirror_bootstrap_final_cut_covers_the_values_it_holds() {
    const DATABASE: &str = "recover_final_value";
    let source = seeded_source(DATABASE).await;
    source
        .exec_ddl_on_any_leader("CREATE SEQUENCE ids")
        .await
        .expect("create sequence");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");
    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[DATABASE]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {COPY} FROM {SOURCE_NAME}.{DATABASE}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_following(&mirror, COPY).await;
    read_mirror(&mirror, COPY).await;

    let gates = tempfile::tempdir().expect("gate directory");
    let release = gates.path().join("release");
    let parked = gates.path().join("release.parked");
    let first = FailGuard::install(
        CUT_BARRIER_FAIL_POINT,
        FailAction::WaitForFile(release.clone()),
    );
    wait_for(
        "a source cut parks between its mark and its barriers",
        Duration::from_secs(30),
        Duration::from_millis(20),
        || parked.exists(),
    )
    .await;
    let value: i64 = rows(&source.nodes[1], "SELECT nextval('ids')")
        .await
        .first()
        .and_then(|row| row.trim().parse().ok())
        .expect("nextval answers one number");
    let id = format!("v{value}");
    source.nodes[1]
        .exec(&format!("INSERT INTO docs (id, v) VALUES ('{id}', 1)"))
        .await
        .expect("insert");

    // Every later cut parks for good, so this one is the mirror's last.
    drop(first);
    let _later_cuts = FailGuard::install(
        CUT_BARRIER_FAIL_POINT,
        FailAction::WaitForFile(gates.path().join("never")),
    );
    std::fs::write(&release, b"go").expect("release the cut");
    await_mirror_reads(
        &mirror,
        "SELECT id FROM docs",
        &["a".to_string(), id.clone()],
    )
    .await;
    source.shutdown().await;

    mirror
        .exec_ddl_on_any_leader(&format!("ALTER DATABASE {COPY} PROMOTE"))
        .await
        .expect("PROMOTE");
    wait_for(
        "every node shows the mirror promoted",
        Duration::from_secs(120),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                matches!(status_on(node, database), Some(MirrorStatus::Promoted))
                    && !node.shared.mirror.fence.is_read_only(database)
            })
        },
    )
    .await;
    for node in &mirror.nodes {
        let next: i64 = rows(node, "SELECT nextval('ids')")
            .await
            .first()
            .and_then(|row| row.trim().parse().ok())
            .expect("nextval answers one number");
        assert!(next > value, "{next} repeats the source's range of {value}");
    }

    mirror.shutdown().await;
}

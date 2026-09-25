// SPDX-License-Identifier: BUSL-1.1

//! Compiled only with `--features failpoints`.
//!
//! A permission-tree grant or revoke is a plain write to the permission
//! table. The Event Plane's permission step applies it to the permission
//! cache. The write is acknowledged only once the cache holds it, so every
//! statement planned after the acknowledgement plans against it.
//!
//! The fail gate `permission_tree::before_apply` parks the permission step
//! while its file is absent. The test sets the tree up with the gate open,
//! removes the file, and issues a revoke: the acknowledgement must wait.
//! Writing the file releases the step, and the revoke then binds the other
//! session's very next statement.

#[cfg(feature = "failpoints")]
use std::time::Duration;

#[cfg(feature = "failpoints")]
use super::permission_tree_support::{
    SELECT_DOCS, connect_probe, create_tree, grant_d1, select_ids,
};
#[cfg(feature = "failpoints")]
use crate::harness::TestServer;

/// How long the revoke must stay unacknowledged while the step is parked.
#[cfg(feature = "failpoints")]
const PARKED_FOR: Duration = Duration::from_millis(1500);

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoke_is_acknowledged_only_after_the_permission_step_applies_it() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-permission-step");
    // The gate starts open: the file exists.
    std::fs::write(&release, b"").expect("open the gate");
    let server = TestServer::start_with_failpoints(&format!(
        "permission_tree::before_apply=wait_file({})",
        release.display()
    ))
    .await;

    create_tree(&server).await;
    grant_d1(&server).await;
    let (first, first_handle) = connect_probe(&server).await;
    let (second, second_handle) = connect_probe(&server).await;
    // The grant was acknowledged, so both sessions see it at once.
    assert_eq!(select_ids(&first, SELECT_DOCS).await, vec!["d1"]);
    assert_eq!(select_ids(&second, SELECT_DOCS).await, vec!["d1"]);

    // Park the permission step, then revoke on a superuser connection of its
    // own, so the test can watch the acknowledgement wait.
    let (revoker, revoker_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .unwrap_or_else(|e| panic!("connect the revoker: {e}"));
    std::fs::remove_file(&release).expect("park the permission step");
    let revoke = {
        let client = revoker;
        tokio::spawn(async move {
            client
                .simple_query(
                    "DELETE FROM pt_grants WHERE resource_id = 'd1' AND grantee = 'pt_role'",
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    };
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !revoke.is_finished(),
        "the revoke was acknowledged while the permission step had not applied it"
    );

    // Release the step: the revoke is acknowledged, and binds the other
    // session's next statement, cached or not.
    std::fs::write(&release, b"").expect("release the permission step");
    revoke
        .await
        .expect("revoke task")
        .unwrap_or_else(|e| panic!("revoke: {e}"));
    assert_eq!(
        select_ids(&second, SELECT_DOCS).await,
        Vec::<String>::new(),
        "a session planned against the permission state before the acknowledged revoke"
    );
    assert_eq!(
        select_ids(&first, SELECT_DOCS).await,
        Vec::<String>::new(),
        "a cached plan kept the revoked grant"
    );

    drop(first);
    drop(second);
    first_handle.abort();
    second_handle.abort();
    revoker_handle.abort();
}

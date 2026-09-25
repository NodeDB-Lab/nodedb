// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree grants survive a restart.
//!
//! The permission cache is in-memory. Its grants and hierarchy edges live in
//! collections, and the boot sequence loads them before the gateway serves.
//! A non-superuser's first statement after the restart plans against the
//! grants in force before it: a granted row stays visible, and a revoked row
//! stays hidden.

use super::permission_tree_support::{
    SELECT_DOCS, connect_probe, create_tree, grant_d1, revoke_d1, select_ids,
};
use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grant_stays_visible_after_restart() {
    let server = TestServer::start().await;
    create_tree(&server).await;
    grant_d1(&server).await;
    {
        let (probe, handle) = connect_probe(&server).await;
        assert_eq!(select_ids(&probe, SELECT_DOCS).await, vec!["d1"]);
        drop(probe);
        handle.abort();
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let (probe, handle) = connect_probe(&server).await;
    assert_eq!(
        select_ids(&probe, SELECT_DOCS).await,
        vec!["d1"],
        "the grant on d1 was lost across the restart"
    );
    drop(probe);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoke_stays_in_force_after_restart() {
    let server = TestServer::start().await;
    create_tree(&server).await;
    grant_d1(&server).await;
    revoke_d1(&server).await;
    {
        let (probe, handle) = connect_probe(&server).await;
        assert_eq!(select_ids(&probe, SELECT_DOCS).await, Vec::<String>::new());
        drop(probe);
        handle.abort();
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let (probe, handle) = connect_probe(&server).await;
    assert_eq!(
        select_ids(&probe, SELECT_DOCS).await,
        Vec::<String>::new(),
        "the revoked grant on d1 came back after the restart"
    );
    drop(probe);
    handle.abort();
}

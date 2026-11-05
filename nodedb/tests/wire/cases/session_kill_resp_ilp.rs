// SPDX-License-Identifier: BUSL-1.1

//! A dropped user's RESP and ILP sessions end at once, with no client traffic.
//!
//! Both protocols hold a session-lifetime identity, so a quiet connection has no
//! request boundary to re-check it. The kill signal is the only thing that can
//! end it.

use std::time::Duration;

use tokio::io::AsyncReadExt;

use nodedb_test_support::ilp_client;

use crate::harness::TestServer;

const CLOSE_BOUND: Duration = Duration::from_secs(10);

#[tokio::test]
async fn dropping_a_user_ends_its_quiet_resp_session() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kill_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .expect("create collection");
    let mut resp = server.resp_session("resp_victim", "kill_kv").await;

    server
        .exec("DROP USER resp_victim")
        .await
        .expect("drop user");

    let reply = tokio::time::timeout(CLOSE_BOUND, resp.read_reply())
        .await
        .expect("the kill reaches a quiet RESP session");
    assert!(reply.is_error(), "the session reports its end: {reply:?}");
}

#[tokio::test]
async fn a_resp_session_sees_a_role_change_on_its_next_command() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION soft_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')")
        .await
        .expect("create collection");
    let mut resp = server.resp_session("resp_soft", "soft_kv").await;
    let allowed = resp.cmd(&["SET", "k", "v"]).await;
    assert!(!allowed.is_error(), "readwrite can write: {allowed:?}");

    server
        .exec("REVOKE ROLE readwrite FROM resp_soft")
        .await
        .expect("revoke role");

    let refused = resp.cmd(&["SET", "k", "w"]).await;
    assert!(
        refused.is_error(),
        "the revoked role stops the next write: {refused:?}"
    );
}

#[tokio::test]
async fn dropping_a_user_ends_its_quiet_ilp_session() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER ilp_victim PASSWORD 'ilp-victim-pw'")
        .await
        .expect("create user");
    server
        .exec("GRANT ROLE readwrite TO ilp_victim")
        .await
        .expect("grant role");
    let addr = format!("127.0.0.1:{}", server.ilp_port)
        .parse()
        .expect("loopback ILP address must parse");
    let mut ilp = ilp_client::connect_trust(addr, "ilp_victim").await;

    server
        .exec("DROP USER ilp_victim")
        .await
        .expect("drop user");

    let mut rest = Vec::new();
    tokio::time::timeout(CLOSE_BOUND, ilp.read_to_end(&mut rest))
        .await
        .expect("the kill closes a quiet ILP session")
        .expect("the close is clean");
}

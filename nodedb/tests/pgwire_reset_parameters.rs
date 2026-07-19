// SPDX-License-Identifier: BUSL-1.1

//! PostgreSQL-compatible RESET behavior for mutable session parameters.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn reset_unknown_parameter_is_rejected() {
    let server = TestServer::start().await;
    assert!(
        server
            .exec("RESET totally_made_up_parameter")
            .await
            .is_err(),
        "RESET must reject parameters outside the mutable allowlist"
    );
}

#[tokio::test]
async fn reset_restores_each_session_default() {
    let server = TestServer::start().await;
    let cases = [
        ("nodedb.consistency", "eventual", "strong"),
        ("default_read_consistency", "eventual", "strong"),
        ("cross_shard_txn", "best_effort_non_atomic", "strict"),
        ("default_transaction_read_only", "on", "off"),
    ];

    for (parameter, changed, expected) in cases {
        server
            .exec(&format!("SET {parameter} = '{changed}'"))
            .await
            .unwrap();
        server.exec(&format!("RESET {parameter}")).await.unwrap();
        assert_eq!(
            server
                .query_text(&format!("SHOW {parameter}"))
                .await
                .unwrap(),
            vec![expected],
            "RESET must restore the default for {parameter}"
        );
    }
}

#[tokio::test]
async fn reset_all_restores_session_defaults() {
    let server = TestServer::start().await;
    server
        .exec("SET nodedb.consistency = eventual")
        .await
        .unwrap();
    server
        .exec("SET default_read_consistency = eventual")
        .await
        .unwrap();

    server.exec("RESET ALL").await.unwrap();

    assert_eq!(
        server.query_text("SHOW nodedb.consistency").await.unwrap(),
        vec!["strong"]
    );
    assert_eq!(
        server
            .query_text("SHOW default_read_consistency")
            .await
            .unwrap(),
        vec!["strong"]
    );
}

#[tokio::test]
async fn reset_all_cannot_change_tenant_inside_transaction() {
    let server = TestServer::start().await;
    server.exec("CREATE TENANT reset_guard ID 2").await.unwrap();
    server.exec("SET nodedb.tenant_id = 2").await.unwrap();
    server.exec("BEGIN").await.unwrap();

    let error = server
        .exec("RESET ALL")
        .await
        .expect_err("RESET ALL must not change tenant inside a transaction");
    assert!(
        error.contains("25001"),
        "expected SQLSTATE 25001, got {error}"
    );

    server.exec("ROLLBACK").await.unwrap();
    let tenant = server.query_rows("SHOW TENANT").await.unwrap();
    assert_eq!(
        tenant[0][0], "2",
        "failed RESET ALL must preserve tenant override"
    );
}

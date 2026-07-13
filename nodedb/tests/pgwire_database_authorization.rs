// SPDX-License-Identifier: BUSL-1.1

//! Extended-protocol database authorization and catalog-scope regressions.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn prepared_parse_uses_authorized_session_database_catalog() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE prepared_scope")
        .await
        .expect("create database");
    server
        .exec("CREATE USER prepared_reader WITH PASSWORD 'x' ROLE readonly")
        .await
        .expect("create reader");
    server
        .exec("GRANT ALL ON DATABASE prepared_scope TO prepared_reader")
        .await
        .expect("grant database access");
    server
        .exec("USE DATABASE prepared_scope")
        .await
        .expect("select database");
    server
        .exec(
            "CREATE COLLECTION prepared_rows \
             (id TEXT PRIMARY KEY, content TEXT NOT NULL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create collection in selected database");

    let (client, _connection) = server
        .connect_as_database("prepared_reader", "x", "prepared_scope")
        .await
        .expect("connect to granted database");
    let statement = client
        .prepare("SELECT content FROM prepared_rows")
        .await
        .expect("Parse/Describe must use the selected database catalog");

    assert_eq!(statement.columns().len(), 1);
    assert_eq!(statement.columns()[0].name(), "content");
}

fn assert_insufficient_privilege(
    result: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>,
) {
    let error = result.expect_err("query must be denied");
    let database_error = error.as_db_error().expect("server must return SQLSTATE");
    assert_eq!(database_error.code().code(), "42501");
}

#[tokio::test]
async fn prepared_parse_denies_database_before_describe_metadata() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE prepared_denied")
        .await
        .expect("create database");
    server
        .exec("CREATE USER denied_reader WITH PASSWORD 'x' ROLE readonly")
        .await
        .expect("create reader");
    server
        .exec("USE DATABASE prepared_denied")
        .await
        .expect("select database");
    server
        .exec(
            "CREATE COLLECTION secret_schema \
             (id TEXT PRIMARY KEY, secret TEXT NOT NULL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create secret collection");

    let (client, _connection) = server
        .connect_as_database("denied_reader", "x", "prepared_denied")
        .await
        .expect("startup may complete before the first statement gate");
    let error = client
        .prepare("SELECT secret FROM secret_schema")
        .await
        .expect_err("Parse must reject the unauthorized database before Describe");
    let database_error = error.as_db_error().expect("server must return SQLSTATE");

    assert_eq!(database_error.code().code(), "42501");
    assert!(database_error.message().contains("database"));
}

#[tokio::test]
async fn prepared_parse_denies_collection_before_describe_metadata() {
    let server = TestServer::start().await;
    server
        .exec("CREATE ROLE metadata_denied_role")
        .await
        .expect("create custom role");
    server
        .exec(
            "CREATE USER metadata_denied WITH PASSWORD 'x' \
             ROLE metadata_denied_role",
        )
        .await
        .expect("create ungranted user");
    server
        .exec(
            "CREATE COLLECTION metadata_secret \
             (id TEXT PRIMARY KEY, secret TEXT NOT NULL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create secret collection");

    let (client, _connection) = server
        .connect_as("metadata_denied", "x")
        .await
        .expect("connect ungranted user");
    let error = client
        .prepare("SELECT secret FROM metadata_secret")
        .await
        .expect_err("Parse must deny collection access before returning schema");

    assert_eq!(
        error.as_db_error().expect("server SQLSTATE").code().code(),
        "42501"
    );
}

#[tokio::test]
async fn pgwire_interceptors_authorize_before_plan_or_dispatch() {
    let server = TestServer::start().await;
    server
        .exec("CREATE ROLE interceptor_denied_role")
        .await
        .expect("create custom role");
    server
        .exec(
            "CREATE USER interceptor_denied WITH PASSWORD 'x' \
             ROLE interceptor_denied_role",
        )
        .await
        .expect("create ungranted user");
    server
        .exec("CREATE COLLECTION interceptor_secret")
        .await
        .expect("create secret collection");

    let (client, _connection) = server
        .connect_as("interceptor_denied", "x")
        .await
        .expect("connect ungranted user");

    assert_insufficient_privilege(
        client
            .simple_query("LIVE SELECT * FROM interceptor_secret")
            .await,
    );
    assert_insufficient_privilege(
        client
            .simple_query("EXPLAIN SELECT * FROM interceptor_secret")
            .await,
    );
    assert_insufficient_privilege(
        client
            .simple_query(
                "SELECT FACET_COUNTS(collection => 'interceptor_secret', fields => ['kind'])",
            )
            .await,
    );

    client
        .simple_query("BEGIN")
        .await
        .expect("begin cursor transaction");
    assert_insufficient_privilege(
        client
            .simple_query("DECLARE denied_cursor CURSOR FOR SELECT * FROM interceptor_secret")
            .await,
    );
}

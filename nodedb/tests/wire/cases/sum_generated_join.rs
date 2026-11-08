// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard materialized sum whose join column is generated.
//!
//! The Control Plane names the target row from the join value in the write
//! body. A generated join column is absent from that body. So
//! `ALTER .. MATERIALIZED_SUM` refuses a generated source column as the join
//! column with SQLSTATE `0A000`. A stored join column on the same source binds.

use crate::harness::TestServer;

/// The sum's target collection.
const ACCOUNTS: &str = "gj_acct";
/// The sum's source collection.
const POSTINGS: &str = "gj_post";
/// Data Plane cores the server runs.
const CORES: usize = 2;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// `ALTER .. MATERIALIZED_SUM` that binds `join_column` of the source.
fn bind_sum(join_column: &str) -> String {
    format!(
        "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance FLOAT \
         MATERIALIZED_SUM SOURCE {POSTINGS} \
         ON {POSTINGS}.{join_column} = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
    )
}

/// A generated join column is refused. A stored join column on the same
/// source binds after the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_generated_join_column_is_refused_and_a_stored_one_binds() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ),
    )
    .await;

    match server.exec(&bind_sum("account_id")).await {
        Err(message) => assert!(
            message.contains("SQLSTATE 0A000") && message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("a sum bound on the generated join column '{POSTINGS}.account_id'"),
    }

    exec(&server, &bind_sum("acct")).await;
}

/// A source created after the bind is checked at create time. A generated
/// column that the bind names as join column is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_created_after_the_bind_cannot_generate_the_join_column() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(&server, &bind_sum("account_id")).await;

    let created = server
        .exec(&format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ))
        .await;
    match created {
        Err(message) => assert!(
            message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("'{POSTINGS}' created with a generated join column that a sum binds"),
    }
}

/// A source dropped and created again is checked at create time. A generated
/// column that the surviving bind names as join column is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_recreated_after_a_drop_cannot_generate_the_join_column() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT) WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(&server, &bind_sum("account_id")).await;
    exec(&server, &format!("DROP COLLECTION {POSTINGS}")).await;

    let created = server
        .exec(&format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ))
        .await;
    match created {
        Err(message) => assert!(
            message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("'{POSTINGS}' recreated with a generated join column that a sum binds"),
    }
}

/// A generated column added to a bound source is checked at `ADD COLUMN` time.
/// A generated column named like the join column that a bind names is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_generated_column_added_to_the_source_cannot_take_the_join_name() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(&server, &bind_sum("account_id")).await;

    let added = server
        .exec(&format!(
            "ALTER COLLECTION {POSTINGS} ADD COLUMN account_id TEXT \
             GENERATED ALWAYS AS ('a-' || acct)"
        ))
        .await;
    match added {
        Err(message) => assert!(
            message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("'{POSTINGS}.account_id' added as generated while a sum binds it"),
    }
}

/// A generated column renamed onto the join name of a bound source is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_generated_column_renamed_to_the_join_name_is_refused() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             g TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(&server, &bind_sum("account_id")).await;

    let renamed = server
        .exec(&format!(
            "ALTER COLLECTION {POSTINGS} RENAME COLUMN g TO account_id"
        ))
        .await;
    match renamed {
        Err(message) => assert!(
            message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("'{POSTINGS}.g' renamed to the join column that a sum binds"),
    }
}

/// A bound stored join column is dropped. The name is free, but a generated
/// column cannot take it by `ADD COLUMN` or by `RENAME COLUMN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_join_column_cannot_come_back_generated() {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT, g TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(&server, &bind_sum("account_id")).await;
    exec(
        &server,
        &format!("ALTER COLLECTION {POSTINGS} DROP COLUMN account_id"),
    )
    .await;

    for sql in [
        format!(
            "ALTER COLLECTION {POSTINGS} ADD COLUMN account_id TEXT \
             GENERATED ALWAYS AS ('a-' || acct)"
        ),
        format!("ALTER COLLECTION {POSTINGS} RENAME COLUMN g TO account_id"),
    ] {
        match server.exec(&sql).await {
            Err(message) => assert!(
                message.contains("generated column"),
                "expected the generated join column refusal for {sql}, got: {message}"
            ),
            Ok(()) => panic!("accepted after the join column was dropped: {sql}"),
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! `[auth] ddl_authorization = "owner"` gates every statement that changes
//! an existing object, not only the ones the matrix-fix series named.
//!
//! `CREATE TYPEGUARD`/`ALTER TYPEGUARD`/`DROP TYPEGUARD` and
//! `ALTER VECTOR INDEX ... SEAL/COMPACT/SET` are the same "change an
//! existing object" shape as `ADD/DROP CONSTRAINT` and `ALTER COLLECTION`,
//! which already carry the owner-or-admin check.

use crate::harness::TestServer;

const PASSWORD: &str = "ddl-perm-owner-secret-42";

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Run `sql` as `user`, returning the server's error message on failure.
async fn exec_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    drop(client);
    handle.abort();
    result
}

fn assert_owner_denied(result: &Result<(), String>, what: &str) {
    match result {
        Ok(()) => panic!("{what} must be refused for a non-owner, but it succeeded"),
        Err(message) => assert!(
            message.contains("permission denied") && message.contains("owner"),
            "{what} must be refused with the owner-or-admin message, got: {message}"
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_collections_owner_or_admin_changes_its_type_guards() {
    let server = TestServer::start_with_ddl_authorization("owner").await;
    create_user(&server, "dpt_owner").await;
    create_user(&server, "dpt_other").await;

    exec_as(&server, "dpt_owner", "CREATE COLLECTION dpt_tg")
        .await
        .expect("owner creates the collection");

    let denied = exec_as(
        &server,
        "dpt_other",
        "CREATE TYPEGUARD ON dpt_tg (name STRING)",
    )
    .await;
    assert_owner_denied(&denied, "CREATE TYPEGUARD by a non-owner");

    exec_as(
        &server,
        "dpt_owner",
        "CREATE TYPEGUARD ON dpt_tg (name STRING)",
    )
    .await
    .expect("the owner may create a type guard on their own collection");

    let denied = exec_as(
        &server,
        "dpt_other",
        "ALTER TYPEGUARD ON dpt_tg ADD age INT",
    )
    .await;
    assert_owner_denied(&denied, "ALTER TYPEGUARD ADD by a non-owner");

    let denied = exec_as(&server, "dpt_other", "ALTER TYPEGUARD ON dpt_tg DROP name").await;
    assert_owner_denied(&denied, "ALTER TYPEGUARD DROP by a non-owner");

    let denied = exec_as(&server, "dpt_other", "DROP TYPEGUARD ON dpt_tg").await;
    assert_owner_denied(&denied, "DROP TYPEGUARD by a non-owner");

    exec_as(&server, "dpt_owner", "DROP TYPEGUARD ON dpt_tg")
        .await
        .expect("the owner may drop a type guard on their own collection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_collections_owner_or_admin_maintains_its_vector_index() {
    let server = TestServer::start_with_ddl_authorization("owner").await;
    create_user(&server, "dpv_owner").await;
    create_user(&server, "dpv_other").await;

    exec_as(
        &server,
        "dpv_owner",
        "CREATE COLLECTION dpv_vec (embedding VECTOR(4))",
    )
    .await
    .expect("owner creates the collection");
    exec_as(
        &server,
        "dpv_owner",
        "CREATE VECTOR INDEX dpv_idx ON dpv_vec (embedding) METRIC cosine DIM 4",
    )
    .await
    .expect("owner creates the vector index");

    let denied = exec_as(
        &server,
        "dpv_other",
        "ALTER VECTOR INDEX ON dpv_vec.embedding SEAL",
    )
    .await;
    assert_owner_denied(&denied, "ALTER VECTOR INDEX SEAL by a non-owner");

    let denied = exec_as(
        &server,
        "dpv_other",
        "ALTER VECTOR INDEX ON dpv_vec.embedding COMPACT",
    )
    .await;
    assert_owner_denied(&denied, "ALTER VECTOR INDEX COMPACT by a non-owner");

    let denied = exec_as(
        &server,
        "dpv_other",
        "ALTER VECTOR INDEX ON dpv_vec.embedding SET (ef_construction = 128)",
    )
    .await;
    assert_owner_denied(&denied, "ALTER VECTOR INDEX SET by a non-owner");

    exec_as(
        &server,
        "dpv_owner",
        "ALTER VECTOR INDEX ON dpv_vec.embedding SEAL",
    )
    .await
    .expect("the owner may seal their own vector index");
}

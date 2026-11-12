// SPDX-License-Identifier: BUSL-1.1

//! The `database` startup parameter under password authentication.
//!
//! An unknown database refuses the connection with a `FATAL` 3D000. No
//! statement on that connection runs, and no later statement reads the
//! refusal as its own reply.

use crate::harness::TestServer;

const USER: &str = "startup_db_user";
const PASSWORD: &str = "startup-db-pw";

async fn create_user(server: &TestServer) {
    server
        .client
        .simple_query(&format!(
            "CREATE USER {USER} WITH PASSWORD '{PASSWORD}' ROLE readonly"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE USER {USER}: {e}"));
}

async fn connect(
    server: &TestServer,
    database: &str,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={USER} password={PASSWORD} dbname={database}",
        server.pg_port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, handle))
}

fn assert_fatal_3d000(error: &tokio_postgres::Error, context: &str) {
    let db_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{context}: expected a server error, got {error}"));
    assert_eq!(db_error.code().code(), "3D000", "{context}: {db_error}");
    assert_eq!(db_error.severity(), "FATAL", "{context}: {db_error}");
    assert!(
        db_error.message().contains("does not exist"),
        "{context}: {db_error}"
    );
}

#[tokio::test]
async fn an_unknown_startup_database_refuses_the_connection() {
    let server = TestServer::start().await;
    create_user(&server).await;

    let (client, handle) = match connect(&server, "no_such_database").await {
        Err(error) => {
            assert_fatal_3d000(&error, "connect");
            return;
        }
        Ok(pair) => pair,
    };
    let first = client
        .simple_query("SELECT 1")
        .await
        .expect_err("the first statement on a refused connection must fail");
    assert_fatal_3d000(&first, "first statement");
    let second = client.simple_query("SELECT 2").await;
    assert!(
        second.is_err(),
        "a refused connection must be closed, but a later statement ran"
    );
    handle.abort();
}

#[tokio::test]
async fn a_known_startup_database_answers_each_statement_with_its_own_reply() {
    let server = TestServer::start().await;
    create_user(&server).await;

    let (client, handle) = connect(&server, "default")
        .await
        .unwrap_or_else(|e| panic!("connect to default: {e}"));
    for n in 1..=3 {
        let rows = client
            .query_one(&format!("SELECT {n} AS v"), &[])
            .await
            .unwrap_or_else(|e| panic!("SELECT {n}: {e}"));
        // `SELECT n` is an `integer`, as in PostgreSQL.
        assert_eq!(rows.get::<_, i32>(0), n, "statement {n} read another reply");
    }
    handle.abort();
}

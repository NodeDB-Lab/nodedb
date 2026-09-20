// SPDX-License-Identifier: BUSL-1.1

//! Row-level `FOR WRITE` policy over vector-primary predicate `DELETE` /
//! `UPDATE`.
//!
//! A `primary='vector'` collection stores the row's non-vector columns in
//! the payload sidecar, so a predicate write has no image until the Data
//! Plane resolves its targets. The policy is decided there, against each
//! targeted row's stored sidecar (a delete) or merged post-image (an
//! update), before the first row changes.
//!
//! What these tests pin:
//!
//! - A predicate write whose every target the caller owns applies, reports
//!   the count of rows it changed, and a search afterwards reflects it.
//! - A predicate write that reaches a row the policy excludes is refused as
//!   a whole; the excluded row and every other target stay as they were.
//! - An update whose post-image hands the row to another owner is refused.

use crate::harness::TestServer;

const PASSWORD: &str = "vector-primary-write-rls-secret-42";

/// The least privilege that can run the statements under test, so a denial
/// is the policy's doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

/// Restrict writes on `collection` to rows the authenticated principal owns.
async fn write_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy {policy}: {e}"));
}

async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} \
                 (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING, region STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner', 'region'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

async fn insert(server: &TestServer, name: &str, id: &str, vec: &str, owner: &str, region: &str) {
    server
        .exec(&format!(
            "INSERT INTO {name} (id, vec, owner, region) \
             VALUES ('{id}', ARRAY[{vec}], '{owner}', '{region}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {name}: {e}"));
}

/// Run `sql` as `user`, returning the command tag's row count on success and
/// the server's error message on failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<u64, String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    let count = result.map(|messages| {
        messages
            .into_iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(n),
                _ => None,
            })
            .next()
            .unwrap_or(0)
    });
    drop(client);
    handle.abort();
    count
}

/// Assert a statement was refused BY THE POLICY rather than by some unrelated
/// failure that would make the test pass for the wrong reason.
fn assert_rls_denied(result: Result<u64, String>, what: &str) {
    match result {
        Ok(count) => panic!("{what} must be refused, but it succeeded with {count} rows"),
        Err(message) => assert!(
            message.contains("RLS"),
            "{what} must be refused by the RLS policy, got: {message}"
        ),
    }
}

/// Ids in nearest-first order for `query`, read as the superuser, who holds
/// no restricting policy — so this is the true stored row set.
async fn nearest(server: &TestServer, name: &str, query: &str) -> Vec<String> {
    server
        .query_rows(&format!(
            "SELECT id FROM {name} ORDER BY vector_distance(vec, ARRAY[{query}]) LIMIT 10"
        ))
        .await
        .unwrap_or_else(|e| panic!("search {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

/// `(owner, region)` of the point read `WHERE id = '<id>'`.
async fn point(server: &TestServer, name: &str, id: &str) -> Vec<(String, String)> {
    server
        .query_rows(&format!(
            "SELECT owner, region FROM {name} WHERE id = '{id}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("point read {id} from {name}: {e}"))
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect()
}

/// A predicate delete over rows the caller owns removes exactly those rows,
/// reports their count, and a search no longer finds them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_delete_over_owned_rows_applies() {
    let server = TestServer::start().await;
    let user = "vp_rls_pdel_user";
    create(&server, "vp_rls_pdel").await;
    insert(&server, "vp_rls_pdel", "r1", "1.0, 0.0, 0.0", user, "eu").await;
    insert(&server, "vp_rls_pdel", "r2", "0.0, 1.0, 0.0", user, "us").await;
    insert(&server, "vp_rls_pdel", "r3", "0.0, 0.0, 1.0", "alice", "eu").await;
    create_user(&server, user).await;
    write_policy(&server, "vp_rls_pdel_owner", "vp_rls_pdel").await;

    let deleted = run_as(
        &server,
        user,
        &format!("DELETE FROM vp_rls_pdel WHERE owner = '{user}' AND region = 'eu'"),
    )
    .await
    .expect("deleting owned rows must apply");
    assert_eq!(deleted, 1, "exactly the one owned eu row is removed");

    assert_eq!(
        nearest(&server, "vp_rls_pdel", "1.0, 0.0, 0.0").await,
        vec!["r2".to_string(), "r3".to_string()],
        "the removed row must leave the search results; the others stay"
    );
    assert_eq!(point(&server, "vp_rls_pdel", "r1").await, Vec::new());
}

/// A predicate update whose post-images the caller still owns rewrites
/// exactly the matched rows and reports their count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_update_over_owned_rows_applies() {
    let server = TestServer::start().await;
    let user = "vp_rls_pupd_user";
    create(&server, "vp_rls_pupd").await;
    insert(&server, "vp_rls_pupd", "r1", "1.0, 0.0, 0.0", user, "eu").await;
    insert(&server, "vp_rls_pupd", "r2", "0.0, 1.0, 0.0", user, "us").await;
    insert(&server, "vp_rls_pupd", "r3", "0.0, 0.0, 1.0", "alice", "eu").await;
    create_user(&server, user).await;
    write_policy(&server, "vp_rls_pupd_owner", "vp_rls_pupd").await;

    let updated = run_as(
        &server,
        user,
        &format!("UPDATE vp_rls_pupd SET region = 'apac' WHERE owner = '{user}'"),
    )
    .await
    .expect("updating owned rows must apply");
    assert_eq!(updated, 2, "both owned rows are rewritten");

    assert_eq!(
        point(&server, "vp_rls_pupd", "r1").await,
        vec![(user.to_string(), "apac".to_string())]
    );
    assert_eq!(
        point(&server, "vp_rls_pupd", "r2").await,
        vec![(user.to_string(), "apac".to_string())]
    );
    assert_eq!(
        point(&server, "vp_rls_pupd", "r3").await,
        vec![("alice".to_string(), "eu".to_string())],
        "a row the predicate did not name is untouched"
    );
    assert_eq!(
        nearest(&server, "vp_rls_pupd", "1.0, 0.0, 0.0").await,
        vec!["r1".to_string(), "r2".to_string(), "r3".to_string()],
        "a payload-only update keeps every row searchable"
    );
}

/// A predicate delete that reaches a row the policy excludes is refused as a
/// whole: the excluded row survives and so does every owned target beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_delete_reaching_an_excluded_row_is_refused() {
    let server = TestServer::start().await;
    let user = "vp_rls_pdel_deny_user";
    create(&server, "vp_rls_pdel_deny").await;
    insert(
        &server,
        "vp_rls_pdel_deny",
        "r1",
        "1.0, 0.0, 0.0",
        user,
        "eu",
    )
    .await;
    insert(
        &server,
        "vp_rls_pdel_deny",
        "r3",
        "0.0, 0.0, 1.0",
        "alice",
        "eu",
    )
    .await;
    create_user(&server, user).await;
    write_policy(&server, "vp_rls_pdel_deny_owner", "vp_rls_pdel_deny").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "DELETE FROM vp_rls_pdel_deny WHERE region = 'eu'",
        )
        .await,
        "a predicate delete reaching a row owned by someone else",
    );
    assert_eq!(
        point(&server, "vp_rls_pdel_deny", "r3").await,
        vec![("alice".to_string(), "eu".to_string())],
        "the excluded row must survive the refused delete"
    );
    assert_eq!(
        point(&server, "vp_rls_pdel_deny", "r1").await,
        vec![(user.to_string(), "eu".to_string())],
        "a refused statement leaves the owned target in place too"
    );
    assert_eq!(
        nearest(&server, "vp_rls_pdel_deny", "1.0, 0.0, 0.0").await,
        vec!["r1".to_string(), "r3".to_string()]
    );
}

/// The policy decides an update's POST-image, so handing an owned row to
/// another owner is refused and the row keeps its stored image.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_handing_the_row_to_another_owner_is_refused() {
    let server = TestServer::start().await;
    let user = "vp_rls_pupd_deny_user";
    create(&server, "vp_rls_pupd_deny").await;
    insert(
        &server,
        "vp_rls_pupd_deny",
        "r1",
        "1.0, 0.0, 0.0",
        user,
        "eu",
    )
    .await;
    create_user(&server, user).await;
    write_policy(&server, "vp_rls_pupd_deny_owner", "vp_rls_pupd_deny").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!("UPDATE vp_rls_pupd_deny SET owner = 'alice' WHERE owner = '{user}'"),
        )
        .await,
        "an update whose post-image belongs to another owner",
    );
    assert_eq!(
        point(&server, "vp_rls_pupd_deny", "r1").await,
        vec![(user.to_string(), "eu".to_string())],
        "the refused update must not touch the stored row"
    );
}

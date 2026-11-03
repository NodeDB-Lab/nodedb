// SPDX-License-Identifier: BUSL-1.1

//! Row-level security and grants over multi-edge `GRAPH INSERT EDGE` and
//! `GRAPH DELETE EDGE` statements.
//!
//! A batch is decided per edge, exactly as the single-edge form is: an
//! insert on each edge's `PROPERTIES` image, a delete on each edge's stored
//! properties. One refused edge refuses the whole statement, so a batch with
//! one allowed and one denied edge writes neither. A caller without the
//! collection's write grant is refused before any edge is resolved.

use std::collections::BTreeSet;

use crate::harness::TestServer;

const PASSWORD: &str = "graph-edge-batch-authz-secret-42";

/// A user holding the built-in `readwrite` role, the least privilege that
/// runs the statements under test, so a denial is the policy's doing.
async fn create_writer(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// A user holding only `role`, a custom role that confers nothing without
/// an explicit grant.
async fn create_stranger(server: &TestServer, user: &str, role: &str) {
    server
        .exec(&format!("CREATE ROLE IF NOT EXISTS {role}"))
        .await
        .unwrap_or_else(|e| panic!("create role {role}: {e}"));
    server
        .exec(&format!(
            "CREATE USER {user} PASSWORD '{PASSWORD}' ROLE {role}"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
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

/// Run `sql` as `user`. `Err` carries the server's message.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
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

/// The out-neighbors of `node`, read as the superuser.
async fn neighbors(server: &TestServer, collection: &str, node: &str) -> BTreeSet<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION out"
        ))
        .await
        .unwrap_or_else(|e| panic!("read neighbors of {node}: {e}"));
    let mut out = BTreeSet::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(node) = entry.get("node").and_then(|value| value.as_str()) {
                out.insert(node.to_string());
            }
        }
    }
    out
}

/// One edge clause run with an `owner` property.
fn owned_edge(src: &str, dst: &str, owner: &str) -> String {
    format!("FROM '{src}' TO '{dst}' TYPE 'knows' PROPERTIES '{{\"owner\":\"{owner}\"}}'")
}

fn assert_rls_denied(result: Result<(), String>, what: &str) {
    match result {
        Ok(()) => panic!("{what} must be refused, but it succeeded"),
        Err(message) => assert!(
            message.contains("RLS"),
            "{what} must be refused by the RLS policy, got: {message}"
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_batch_with_one_violating_edge_writes_none() {
    let server = TestServer::start().await;
    let user = "geb_ins_user";
    server.exec("CREATE COLLECTION geb_ins").await.unwrap();
    create_writer(&server, user).await;
    write_policy(&server, "geb_ins_owner", "geb_ins").await;

    let mixed = format!(
        "GRAPH INSERT EDGE IN 'geb_ins' {} {}",
        owned_edge("a", "b", user),
        owned_edge("c", "d", "alice")
    );
    assert_rls_denied(
        run_as(&server, user, &mixed).await,
        "a batch whose second edge hands its properties to another owner",
    );
    assert!(
        neighbors(&server, "geb_ins", "a").await.is_empty(),
        "the allowed edge of a refused batch must not land"
    );
    assert!(
        neighbors(&server, "geb_ins", "c").await.is_empty(),
        "the denied edge must not land"
    );

    let bare = format!(
        "GRAPH INSERT EDGE IN 'geb_ins' {} FROM 'e' TO 'f' TYPE 'knows'",
        owned_edge("a", "b", user)
    );
    assert_rls_denied(
        run_as(&server, user, &bare).await,
        "a batch whose second edge carries no property object",
    );

    let conforming = format!(
        "GRAPH INSERT EDGE IN 'geb_ins' {} {}",
        owned_edge("a", "b", user),
        owned_edge("c", "d", user)
    );
    run_as(&server, user, &conforming)
        .await
        .expect("a batch whose every edge satisfies the policy must apply");
    assert_eq!(
        neighbors(&server, "geb_ins", "a").await,
        BTreeSet::from(["b".to_string()])
    );
    assert_eq!(
        neighbors(&server, "geb_ins", "c").await,
        BTreeSet::from(["d".to_string()])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_batch_with_one_excluded_edge_deletes_none() {
    let server = TestServer::start().await;
    let user = "geb_del_user";
    server.exec("CREATE COLLECTION geb_del").await.unwrap();
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN 'geb_del' {} {} {}",
            owned_edge("a", "b", user),
            owned_edge("c", "d", "alice"),
            owned_edge("e", "f", user)
        ))
        .await
        .expect("seed edges owned by the caller and by someone else");
    create_writer(&server, user).await;
    write_policy(&server, "geb_del_owner", "geb_del").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "GRAPH DELETE EDGE IN 'geb_del' FROM 'a' TO 'b' TYPE 'knows' \
             FROM 'c' TO 'd' TYPE 'knows'",
        )
        .await,
        "a delete batch naming an edge outside the write policy",
    );
    assert_eq!(
        neighbors(&server, "geb_del", "a").await,
        BTreeSet::from(["b".to_string()]),
        "the owned edge of a refused batch must survive"
    );
    assert_eq!(
        neighbors(&server, "geb_del", "c").await,
        BTreeSet::from(["d".to_string()]),
        "the excluded edge must survive"
    );

    run_as(
        &server,
        user,
        "GRAPH DELETE EDGE IN 'geb_del' FROM 'a' TO 'b' TYPE 'knows' \
         FROM 'e' TO 'f' TYPE 'knows'",
    )
    .await
    .expect("a delete batch of owned edges must apply");
    assert!(neighbors(&server, "geb_del", "a").await.is_empty());
    assert!(neighbors(&server, "geb_del", "e").await.is_empty());
    assert_eq!(
        neighbors(&server, "geb_del", "c").await,
        BTreeSet::from(["d".to_string()]),
        "an edge the batch did not name is untouched"
    );
}

/// A caller without the collection's write grant cannot write edges, one or
/// many, while a caller holding it can.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_writes_need_the_write_grant() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION geb_grant").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'geb_grant' FROM 'a' TO 'b' TYPE 'knows'")
        .await
        .expect("seed an edge");
    create_stranger(&server, "geb_stranger", "geb_nobody").await;
    create_writer(&server, "geb_writer").await;

    for sql in [
        "GRAPH INSERT EDGE IN 'geb_grant' FROM 'x' TO 'y' TYPE 'knows'",
        "GRAPH INSERT EDGE IN 'geb_grant' FROM 'x' TO 'y' TYPE 'knows' \
         FROM 'y' TO 'z' TYPE 'knows'",
        "GRAPH DELETE EDGE IN 'geb_grant' FROM 'a' TO 'b' TYPE 'knows'",
        "GRAPH DELETE EDGE IN 'geb_grant' FROM 'a' TO 'b' TYPE 'knows' \
         FROM 'x' TO 'y' TYPE 'knows'",
    ] {
        let error = run_as(&server, "geb_stranger", sql)
            .await
            .expect_err("a caller without the write grant is refused");
        assert!(
            error.to_lowercase().contains("permission denied"),
            "{sql}: {error}"
        );
    }
    assert!(neighbors(&server, "geb_grant", "x").await.is_empty());
    assert_eq!(
        neighbors(&server, "geb_grant", "a").await,
        BTreeSet::from(["b".to_string()])
    );

    run_as(
        &server,
        "geb_writer",
        "GRAPH INSERT EDGE IN 'geb_grant' FROM 'x' TO 'y' TYPE 'knows' \
         FROM 'y' TO 'z' TYPE 'knows'",
    )
    .await
    .expect("a caller holding the write grant inserts a batch");
    run_as(
        &server,
        "geb_writer",
        "GRAPH DELETE EDGE IN 'geb_grant' FROM 'a' TO 'b' TYPE 'knows' \
         FROM 'x' TO 'y' TYPE 'knows'",
    )
    .await
    .expect("a caller holding the write grant deletes a batch");
    assert!(neighbors(&server, "geb_grant", "a").await.is_empty());
    assert_eq!(
        neighbors(&server, "geb_grant", "y").await,
        BTreeSet::from(["z".to_string()])
    );
}

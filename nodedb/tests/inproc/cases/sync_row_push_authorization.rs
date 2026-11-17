// SPDX-License-Identifier: BUSL-1.1

//! Read authorization for the rows Origin pushes to sync sessions.
//!
//! A `RowPush` carries a row post-image to a device, which persists it. It is
//! a read on the session's behalf, so it obeys the rules the shape snapshot
//! obeys, at the moment the push leaves:
//!
//! 1. A session receives rows only of collections its authorized shapes name.
//! 2. A session without a read grant cannot subscribe and receives nothing.
//! 3. A row a read policy hides is never pushed.
//! 4. A grant revoked after subscribe stops the pushes.
//! 5. A row leaving a shape sends a delete. A row never in it sends nothing.
//! 6. A purge notice reaches only sessions that hold the collection.

use nodedb_types::sync::wire::RowOp;

use super::sync_push_support::{Harness, collect, collect_quiet, eq_predicate, subscribe};

/// Create `collection` with no rows.
async fn create(h: &Harness, collection: &str) {
    h.exec(&format!("CREATE COLLECTION {collection}")).await;
}

async fn insert(h: &Harness, collection: &str, id: &str, owner: &str, status: &str) {
    h.exec(&format!(
        "INSERT INTO {collection} (id, owner, status) VALUES ('{id}', '{owner}', '{status}')"
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_receives_rows_only_of_subscribed_collections() {
    let h = Harness::start().await;
    create(&h, "push_sub_a").await;
    create(&h, "push_sub_b").await;

    let mut ws = h.session("push_sub_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "sub-a", "push_sub_a", Vec::new())
            .await
            .is_some(),
        "an authorized subscription must be answered with a snapshot"
    );

    insert(&h, "push_sub_b", "b-1", "alice", "open").await;
    insert(&h, "push_sub_a", "a-1", "alice", "open").await;

    let feed = collect(&mut ws, |f| f.has_row("push_sub_a", "a-1", RowOp::Upsert)).await;
    assert!(
        feed.has_row("push_sub_a", "a-1", RowOp::Upsert),
        "the subscribed collection's write must be pushed"
    );
    assert!(
        !feed.pushed_collection("push_sub_b"),
        "a write to a collection the session never subscribed to was pushed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_without_read_grant_cannot_subscribe_or_receive() {
    let h = Harness::start().await;
    create(&h, "push_ungranted").await;

    let mut intruder = h.session("push_intruder", &[]).await;
    assert!(
        subscribe(&mut intruder, "ungranted", "push_ungranted", Vec::new())
            .await
            .is_none(),
        "a principal without a read grant was answered with a snapshot"
    );
    let mut reader = h.session("push_granted_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut reader, "granted", "push_ungranted", Vec::new())
            .await
            .is_some()
    );

    insert(&h, "push_ungranted", "g-1", "alice", "open").await;

    // The authorized session proves the write went out.
    let control = collect(&mut reader, |f| f.pushed_document("g-1")).await;
    assert!(control.has_row("push_ungranted", "g-1", RowOp::Upsert));
    let feed = collect_quiet(&mut intruder).await;
    assert!(
        feed.rows.is_empty(),
        "a principal without a read grant received row pushes: {:?}",
        feed.rows
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_policy_stops_the_push_of_a_hidden_row() {
    let h = Harness::start().await;
    create(&h, "push_rls").await;
    h.exec(
        "CREATE RLS POLICY push_owner_only ON push_rls FOR READ \
         USING (owner = $auth.username)",
    )
    .await;

    let mut ws = h.session("push_rls_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "rls", "push_rls", Vec::new())
            .await
            .is_some()
    );

    insert(&h, "push_rls", "hidden-1", "alice", "open").await;
    insert(&h, "push_rls", "visible-1", "push_rls_reader", "open").await;

    let feed = collect(&mut ws, |f| f.pushed_document("visible-1")).await;
    assert!(
        feed.has_row("push_rls", "visible-1", RowOp::Upsert),
        "a row the read policy admits must be pushed"
    );
    assert!(
        !feed.pushed_document("hidden-1"),
        "a row the read policy hides was pushed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_grant_stops_the_pushes() {
    let h = Harness::start().await;
    create(&h, "push_revoke").await;
    h.exec("CREATE ROLE push_grantee").await;
    h.exec("GRANT READ ON push_revoke TO push_grantee").await;

    let mut grantee = h.session("push_revoke_user", &["push_grantee"]).await;
    assert!(
        subscribe(&mut grantee, "revoke", "push_revoke", Vec::new())
            .await
            .is_some(),
        "a principal granted read must be answered with a snapshot"
    );
    let mut control = h.session("push_revoke_control", &["readwrite"]).await;
    assert!(
        subscribe(&mut control, "control", "push_revoke", Vec::new())
            .await
            .is_some()
    );

    insert(&h, "push_revoke", "before-1", "alice", "open").await;
    let feed = collect(&mut grantee, |f| f.pushed_document("before-1")).await;
    assert!(
        feed.has_row("push_revoke", "before-1", RowOp::Upsert),
        "the granted principal must receive pushes before the revoke"
    );

    h.exec("REVOKE READ ON push_revoke FROM push_grantee").await;
    insert(&h, "push_revoke", "after-1", "alice", "open").await;

    let control_feed = collect(&mut control, |f| f.pushed_document("after-1")).await;
    assert!(control_feed.has_row("push_revoke", "after-1", RowOp::Upsert));
    let feed = collect_quiet(&mut grantee).await;
    assert!(
        !feed.pushed_document("after-1"),
        "a write after the revoke was pushed to the revoked principal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_leaving_the_shape_sends_a_delete_and_a_never_visible_row_sends_nothing() {
    let h = Harness::start().await;
    create(&h, "push_shape").await;

    let mut ws = h.session("push_shape_reader", &["readwrite"]).await;
    assert!(
        subscribe(
            &mut ws,
            "open-only",
            "push_shape",
            eq_predicate("status", "open")
        )
        .await
        .is_some()
    );

    insert(&h, "push_shape", "moving-1", "alice", "open").await;
    insert(&h, "push_shape", "never-1", "alice", "closed").await;
    h.exec("UPDATE push_shape SET status = 'archived' WHERE id = 'never-1'")
        .await;
    h.exec("UPDATE push_shape SET status = 'closed' WHERE id = 'moving-1'")
        .await;

    let feed = collect(&mut ws, |f| {
        f.has_row("push_shape", "moving-1", RowOp::Delete)
    })
    .await;
    assert!(
        feed.has_row("push_shape", "moving-1", RowOp::Upsert),
        "a row inside the shape must be pushed"
    );
    assert!(
        feed.has_row("push_shape", "moving-1", RowOp::Delete),
        "a row leaving the shape must send a delete"
    );
    assert!(
        !feed.pushed_document("never-1"),
        "a row that was never inside the shape was pushed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_reaches_only_sessions_that_hold_the_collection() {
    let h = Harness::start().await;
    create(&h, "push_purge_a").await;
    create(&h, "push_purge_b").await;

    let mut holder = h.session("push_purge_holder", &["readwrite"]).await;
    assert!(
        subscribe(&mut holder, "purge-a", "push_purge_a", Vec::new())
            .await
            .is_some()
    );
    let mut other = h.session("push_purge_other", &["readwrite"]).await;
    assert!(
        subscribe(&mut other, "purge-b", "push_purge_b", Vec::new())
            .await
            .is_some()
    );
    let mut bystander = h.session("push_purge_bystander", &["readwrite"]).await;

    h.exec("DROP COLLECTION push_purge_a PURGE").await;

    let feed = collect(&mut holder, |f| f.purged("push_purge_a")).await;
    assert!(
        feed.purged("push_purge_a"),
        "the session holding the collection must hear its purge"
    );
    for (name, ws) in [("other", &mut other), ("bystander", &mut bystander)] {
        let feed = collect_quiet(ws).await;
        assert!(
            !feed.purged("push_purge_a"),
            "the {name} session heard the purge of a collection it does not hold"
        );
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! The push gate as the principal changes under an open sync session.
//!
//! Every push is decided at the drain that sends it, against the principal
//! as it stands then:
//!
//! 1. A client that sends nothing still receives its pushes.
//! 2. A re-handshake as another principal drops every push the first one
//!    subscribed to.
//! 3. A blacklisted principal receives nothing more, and its session ends.
//! 4. A REVOKE under sustained write load stops the pushes: the drain is
//!    bounded, so the session sees the revoke while the load runs.
//! 5. Function, trigger and procedure bodies reach every principal of the
//!    definition's tenant and database.
//! 6. A flood of resync requests draws from the snapshot bucket, and the
//!    excess gets a rate-limited reject.
//! 7. A presence update on a shape channel spelled another way joins the
//!    canonical channel, and a REVOKE removes the session from it.

use std::time::Duration;

use nodedb_types::sync::wire::{
    PresenceUpdateMsg, ResyncReason, ResyncRequestMsg, RowOp, SyncMessageType,
};

use super::sync_push_support::{Harness, collect, collect_quiet, send, subscribe};

async fn create(h: &Harness, collection: &str) {
    h.exec(&format!("CREATE COLLECTION {collection}")).await;
}

async fn insert(h: &Harness, collection: &str, id: &str) {
    h.exec(&format!(
        "INSERT INTO {collection} (id, owner, status) VALUES ('{id}', 'alice', 'open')"
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_client_receives_pushes_without_sending_frames() {
    let h = Harness::start().await;
    create(&h, "gate_idle").await;
    let mut ws = h.session("gate_idle_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "idle", "gate_idle", Vec::new())
            .await
            .is_some()
    );

    insert(&h, "gate_idle", "idle-1").await;

    // `collect` sends nothing: the push goes out on the queue wake alone.
    let feed = collect(&mut ws, |f| f.pushed_document("idle-1")).await;
    assert!(
        feed.has_row("gate_idle", "idle-1", RowOp::Upsert),
        "a client that sends no frame received no push"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rehandshake_as_another_principal_drops_the_earlier_subscriptions() {
    let h = Harness::start().await;
    create(&h, "gate_rehandshake").await;
    let mut ws = h.session("gate_first_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "first", "gate_rehandshake", Vec::new())
            .await
            .is_some()
    );
    let mut control = h.session("gate_rehandshake_control", &["readwrite"]).await;
    assert!(
        subscribe(&mut control, "control", "gate_rehandshake", Vec::new())
            .await
            .is_some()
    );

    // The same socket authenticates again as a principal with no grant.
    h.handshake(&mut ws, "gate_second_intruder", &[]).await;
    insert(&h, "gate_rehandshake", "after-rehandshake").await;

    let seen = collect(&mut control, |f| f.pushed_document("after-rehandshake")).await;
    assert!(seen.has_row("gate_rehandshake", "after-rehandshake", RowOp::Upsert));
    let feed = collect_quiet(&mut ws).await;
    assert!(
        feed.rows.is_empty(),
        "the second principal received rows the first one subscribed to: {:?}",
        feed.rows
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blacklisted_principal_receives_nothing_more() {
    let h = Harness::start().await;
    create(&h, "gate_blacklist").await;
    let mut ws = h.session("gate_blacklisted", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "bl", "gate_blacklist", Vec::new())
            .await
            .is_some()
    );
    let mut control = h.session("gate_blacklist_control", &["readwrite"]).await;
    assert!(
        subscribe(&mut control, "control", "gate_blacklist", Vec::new())
            .await
            .is_some()
    );

    // A JWT identity with no numeric user id is named by its subject.
    h.exec("BLACKLIST AUTH USER 'gate_blacklisted' REASON 'push gate test'")
        .await;
    insert(&h, "gate_blacklist", "after-blacklist").await;

    let seen = collect(&mut control, |f| f.pushed_document("after-blacklist")).await;
    assert!(seen.has_row("gate_blacklist", "after-blacklist", RowOp::Upsert));
    let feed = collect_quiet(&mut ws).await;
    assert!(
        !feed.pushed_document("after-blacklist"),
        "a blacklisted principal received a push"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoke_under_sustained_load_stops_the_pushes() {
    const ROWS: usize = 400;
    const REVOKE_AT: usize = 200;

    let h = Harness::start().await;
    create(&h, "gate_load").await;
    h.exec("CREATE ROLE gate_load_reader").await;
    h.exec("GRANT READ ON gate_load TO gate_load_reader").await;
    let mut ws = h.session("gate_load_user", &["gate_load_reader"]).await;
    assert!(
        subscribe(&mut ws, "load", "gate_load", Vec::new())
            .await
            .is_some()
    );

    let writer = async {
        for i in 0..ROWS {
            if i == REVOKE_AT {
                h.exec("REVOKE READ ON gate_load FROM gate_load_reader")
                    .await;
            }
            insert(&h, "gate_load", &format!("load-{i:04}")).await;
        }
    };
    let reader = collect(&mut ws, |_| false);
    let ((), feed) = tokio::join!(writer, reader);

    let index_of = |id: &str| -> Option<usize> { id.strip_prefix("load-")?.parse().ok() };
    let late: Vec<&str> = feed
        .rows
        .iter()
        .map(|r| r.document_id.as_str())
        .filter(|id| index_of(id).is_some_and(|i| i >= REVOKE_AT))
        .collect();
    assert!(
        late.is_empty(),
        "rows written after the revoke were pushed: {late:?}"
    );
    assert!(
        feed.rows.iter().any(|r| index_of(&r.document_id).is_some()),
        "the principal received no push before the revoke"
    );

    // The bounded drain leaves the session answering its inbound frames.
    tokio::time::timeout(
        Duration::from_secs(5),
        h.handshake(&mut ws, "gate_load_user", &["readwrite"]),
    )
    .await
    .expect("the session answers a handshake after the load");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn definition_bodies_reach_every_principal_of_the_scope() {
    let h = Harness::start().await;
    let mut admin = h.session("gate_def_admin", &["tenant_admin"]).await;
    // No read grant at all: function bodies are not secret.
    let mut plain = h.session("gate_def_plain", &[]).await;

    h.exec("CREATE FUNCTION gate_double(x INT) RETURNS INT AS SELECT x * 2")
        .await;
    h.exec("DROP FUNCTION gate_double").await;

    for (name, ws) in [("tenant admin", &mut admin), ("plain", &mut plain)] {
        let feed = collect(ws, |f| f.has_definition("gate_double")).await;
        assert!(
            feed.has_definition("gate_double"),
            "the {name} session received no definition sync frame"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resync_request_flood_is_rate_limited() {
    const REQUESTS: usize = 128;

    let h = Harness::start().await;
    create(&h, "gate_resync_flood").await;
    let mut ws = h.session("gate_resync_reader", &["readwrite"]).await;
    assert!(
        subscribe(&mut ws, "flood", "gate_resync_flood", Vec::new())
            .await
            .is_some()
    );

    let request = ResyncRequestMsg {
        reason: ResyncReason::SequenceGap {
            expected: 1,
            received: 2,
        },
        from_mutation_id: 0,
        collection: "gate_resync_flood".into(),
        shape_id: "flood".into(),
    };
    for _ in 0..REQUESTS {
        send(&mut ws, SyncMessageType::ResyncRequest, &request).await;
    }

    let feed = collect(&mut ws, |f| f.rate_limited() > 0).await;
    assert!(
        feed.rate_limited() > 0,
        "{REQUESTS} resync requests drew no rate-limited reject"
    );
    assert!(
        feed.rejects.iter().all(|r| r.shape_id == "flood"),
        "a rate-limited reject names the shape it refused"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_canonical_presence_channel_is_left_after_revoke() {
    let h = Harness::start().await;
    create(&h, "gate_presence").await;
    h.exec("CREATE ROLE gate_presence_reader").await;
    h.exec("GRANT READ ON gate_presence TO gate_presence_reader")
        .await;

    // The observer joins the canonical channel through its subscription.
    let mut observer = h.session("gate_presence_observer", &["readwrite"]).await;
    assert!(
        subscribe(&mut observer, "watch", "gate_presence", Vec::new())
            .await
            .is_some()
    );

    // The peer names the channel in another case.
    let mut peer = h
        .session("gate_presence_peer", &["gate_presence_reader"])
        .await;
    let update = |channel: &str| PresenceUpdateMsg {
        channel: channel.into(),
        state: vec![1],
    };
    send(
        &mut peer,
        SyncMessageType::PresenceUpdate,
        &update("shape:GATE_PRESENCE"),
    )
    .await;
    let feed = collect(&mut observer, |f| f.broadcast_on("shape:gate_presence")).await;
    assert!(
        feed.broadcast_on("shape:gate_presence"),
        "an update on another spelling did not reach the canonical channel"
    );

    h.exec("REVOKE READ ON gate_presence FROM gate_presence_reader")
        .await;
    // Any frame wakes the peer's session, and its wake leaves the channel.
    send(
        &mut peer,
        SyncMessageType::PresenceUpdate,
        &update("doc:wake"),
    )
    .await;
    let feed = collect(&mut observer, |f| f.left_channel("shape:gate_presence")).await;
    assert!(
        feed.left_channel("shape:gate_presence"),
        "the peer stayed in the shape channel after the revoke"
    );
}

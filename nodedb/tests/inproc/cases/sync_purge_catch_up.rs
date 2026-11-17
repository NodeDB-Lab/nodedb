// SPDX-License-Identifier: BUSL-1.1

//! Offline purge catch-up at the sync handshake.
//!
//! 1. A client that held a collection while it was purged gets its
//!    `CollectionPurged` ahead of the `HandshakeAck` when it reconnects, and
//!    the ack's watermark covers the purge.
//! 2. A client that names the advanced watermark gets nothing again.
//! 3. A client that never held the collection gets nothing.
//! 4. A client of another tenant that names the same collection hears only
//!    its own tenant's purge of that name.
//! 5. A client whose watermark is below the purge log's retention floor
//!    gets a `DatabaseResync`.
//! 6. A CREATE over a soft-dropped name purges the earlier incarnation
//!    through the replicated entry, so it replays like any purge.

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::security::catalog::collection_purge_log::{
    CollectionPurge, StoredPurgeRecord,
};
use nodedb_types::sync::wire::{
    CollectionPurgedMsg, DatabaseResyncMsg, DatabaseResyncReason, SyncFrame, SyncMessageType,
};

use super::sync_push_support::{Harness, OTHER_TENANT, TENANT};

const HELD: &str = "purge_catch_up_held";

fn purges(frames: &[SyncFrame]) -> Vec<CollectionPurgedMsg> {
    frames
        .iter()
        .filter(|f| f.msg_type == SyncMessageType::CollectionPurged)
        .map(|f| f.decode_body().expect("purge notice decodes"))
        .collect()
}

fn resyncs(frames: &[SyncFrame]) -> Vec<DatabaseResyncMsg> {
    frames
        .iter()
        .filter(|f| f.msg_type == SyncMessageType::DatabaseResync)
        .map(|f| f.decode_body().expect("resync directive decodes"))
        .collect()
}

/// Connect as a reader of tenant 1 holding `held` at `watermark`.
async fn reconnect(h: &Harness, held: &[&str], watermark: u64) -> (Vec<SyncFrame>, u64) {
    let mut ws = h.connect().await;
    let (ahead, ack) = h
        .handshake_holding(
            &mut ws,
            h.token("purge_catch_up_reader", &["readwrite"]),
            held,
            watermark,
        )
        .await;
    assert!(ack.success, "handshake rejected: {:?}", ack.error);
    (ahead, ack.server_watermark)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_missed_offline_is_replayed_at_reconnect() {
    let h = Harness::start().await;
    h.exec(&format!("CREATE COLLECTION {HELD}")).await;

    let (ahead, before) = reconnect(&h, &[HELD], 0).await;
    assert!(ahead.is_empty(), "nothing was purged yet: {ahead:?}");
    assert!(before > 0, "the ack names the server's watermark");

    // The client is offline while the collection is purged.
    h.exec(&format!("DROP COLLECTION {HELD} PURGE")).await;

    let (ahead, after) = reconnect(&h, &[HELD], before).await;
    let replayed = purges(&ahead);
    assert_eq!(replayed.len(), 1, "one purge notice: {ahead:?}");
    assert_eq!(replayed[0].name, HELD);
    assert_eq!(replayed[0].tenant_id, TENANT);
    assert!(replayed[0].purge_index > before);
    assert!(
        after >= replayed[0].purge_index,
        "the watermark covers the purge"
    );

    let (ahead, _) = reconnect(&h, &[HELD], after).await;
    assert!(
        ahead.is_empty(),
        "a client at the advanced watermark heard the purge again: {ahead:?}"
    );

    let (ahead, _) = reconnect(&h, &["purge_catch_up_other"], before).await;
    assert!(
        ahead.is_empty(),
        "a client that never held the collection heard its purge: {ahead:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_tenant_hears_only_its_own_purge_of_the_same_name() {
    let h = Harness::start().await;
    h.exec(&format!(
        "CREATE TENANT purge_catch_up_other ID {OTHER_TENANT}"
    ))
    .await;
    h.exec(&format!("CREATE COLLECTION {HELD}")).await;
    let (_, before) = reconnect(&h, &[HELD], 0).await;
    h.exec(&format!("DROP COLLECTION {HELD} PURGE")).await;

    // A purge of the other tenant's own collection of the same name, on
    // record past the client's watermark. The client hears it, and only
    // it: the session authenticated into the other tenant, and this
    // tenant's purge stays out of its replay.
    h.server
        .shared
        .credentials
        .catalog()
        .record_collection_purge(&CollectionPurge {
            database_id: nodedb_types::DatabaseId::DEFAULT.as_u64(),
            tenant_id: OTHER_TENANT,
            purge_index: before + 1,
            name: HELD.into(),
            record: StoredPurgeRecord {
                incarnation: nodedb_types::Hlc::new(1, 0),
                purged_at_ms: 1,
            },
        })
        .expect("record the other tenant's purge");

    let mut ws = h.connect().await;
    let (ahead, ack) = h
        .handshake_holding(
            &mut ws,
            h.token_in_other_tenant("purge_catch_up_foreign", &["readwrite"]),
            &[HELD],
            before,
        )
        .await;
    assert!(
        ack.success,
        "the other tenant's handshake was refused: {:?}",
        ack.error
    );
    let heard = purges(&ahead);
    assert_eq!(heard.len(), 1, "one purge of the other tenant: {ahead:?}");
    assert_eq!(heard[0].tenant_id, OTHER_TENANT);
    assert_eq!(heard[0].purge_index, before + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_watermark_below_the_retention_floor_resyncs_the_database() {
    let h = Harness::start().await;
    h.exec(&format!("CREATE COLLECTION {HELD}")).await;
    let (_, before) = reconnect(&h, &[HELD], 0).await;
    h.exec(&format!("DROP COLLECTION {HELD} PURGE")).await;

    // The replicated prune a sweep proposes once the purge is past retention.
    let first = h
        .server
        .shared
        .credentials
        .catalog()
        .latest_collection_purge(nodedb_types::DatabaseId::DEFAULT.as_u64(), TENANT, HELD)
        .expect("purge log reads")
        .expect("the purge is on record");
    propose_catalog_entry_async(
        &h.server.shared,
        &CatalogEntry::PruneCollectionPurgeLog {
            cutoff_ms: first.record.purged_at_ms + 1,
        },
    )
    .await
    .expect("propose the prune");

    let (ahead, _) = reconnect(&h, &[HELD], before).await;
    let directives = resyncs(&ahead);
    assert_eq!(directives.len(), 1, "one resync directive: {ahead:?}");
    assert_eq!(directives[0].tenant_id, TENANT);
    assert_eq!(
        directives[0].reason,
        DatabaseResyncReason::BelowRetentionFloor {
            client_watermark: before,
            retention_floor: first.purge_index,
        }
    );
    assert!(purges(&ahead).is_empty(), "a resync replaces the replay");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreate_over_a_soft_dropped_name_is_replayed_as_a_purge() {
    let h = Harness::start().await;
    h.exec(&format!("CREATE COLLECTION {HELD}")).await;
    let (_, before) = reconnect(&h, &[HELD], 0).await;

    // The client is offline while the name is dropped and created again.
    h.exec(&format!("DROP COLLECTION {HELD}")).await;
    h.exec(&format!("CREATE COLLECTION {HELD}")).await;

    let (ahead, after) = reconnect(&h, &[HELD], before).await;
    let replayed = purges(&ahead);
    assert_eq!(
        replayed.len(),
        1,
        "the earlier incarnation's purge: {ahead:?}"
    );
    assert_eq!(replayed[0].name, HELD);
    assert!(replayed[0].purge_index > before);
    assert!(after >= replayed[0].purge_index);
}

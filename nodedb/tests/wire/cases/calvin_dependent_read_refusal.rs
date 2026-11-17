// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard `TRANSFER_ITEM` whose passive read fails ends at once with
//! the read's error.
//!
//! `TRANSFER_ITEM` between key-value collections on two vShards is a
//! read-dependent Calvin txn: the source vShard reads the item under the
//! txn's locks, and the destination's barrier waits for that read. The
//! `calvin::passive_read::<source>` failpoint fails the read with a typed
//! data exception. The failed read sends its refusal to the destination's
//! barrier, which votes abort at once. The verdict reports the read's error,
//! SQLSTATE 22000, well before the barrier's 5 s timeout, and the move
//! changes nothing.

#![cfg(feature = "failpoints")]

use std::time::{Duration, Instant};

use nodedb_types::id::DatabaseId;

use crate::harness::TestServer;

const SOURCE: &str = "drr_src";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A destination name on another vShard than `SOURCE`.
fn dest() -> String {
    (0..)
        .map(|n| format!("drr_dst_{n}"))
        .find(|name| vshard_of(name) != vshard_of(SOURCE))
        .expect("a name on another vShard")
}

/// Less than the barrier's 5 s timeout, with room for a loaded machine.
const SETTLES_WITHIN: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_passive_read_ends_the_move_at_once_with_its_error() {
    let spec = format!("calvin::passive_read::{SOURCE}=fail(the stored item does not read)");
    let server = TestServer::start_multicores_with_failpoints(2, &spec).await;
    let dest = dest();
    for collection in [SOURCE, dest.as_str()] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
                 WITH (engine='kv')"
            ))
            .await
            .expect("create the collection");
    }
    server
        .exec(&format!(
            "INSERT INTO {SOURCE} (key, owner, note) VALUES ('alice:sword', 'alice', 'forged')"
        ))
        .await
        .expect("store the item");

    let started = Instant::now();
    server
        .expect_error(
            &format!("SELECT TRANSFER_ITEM('{SOURCE}', '{dest}', 'sword', 'alice', 'bob')"),
            "SQLSTATE 22000",
        )
        .await;
    let took = started.elapsed();
    assert!(
        took < SETTLES_WITHIN,
        "the move waited {took:?}: the barrier waited for its timeout"
    );

    assert_eq!(
        server
            .query_text(&format!(
                "SELECT owner FROM {SOURCE} WHERE key = 'alice:sword'"
            ))
            .await
            .expect("read the source"),
        ["alice"],
        "the item stays at the source"
    );
    assert!(
        server
            .query_text(&format!("SELECT owner FROM {dest} WHERE key = 'bob:sword'"))
            .await
            .expect("read the destination")
            .is_empty(),
        "the destination holds nothing"
    );
}

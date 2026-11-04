// SPDX-License-Identifier: BUSL-1.1

//! The storage sampler reads a database's stored bytes from every core.
//!
//! The test server is the sole member of a one-node cluster, so its share is
//! the total and reaches the database storage gauge. Writes raise the gauge. A
//! purged drop lowers it once the Data Plane reclaims the collection.

use std::time::{Duration, Instant};

use nodedb::control::metrics::storage::sample_storage;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::DatabaseId;

const COLLECTION: &str = "storage_sampler_probe";

/// Run one storage sample. Returns the default database's storage gauge.
///
/// The background sampler can publish between this sample and the read. The
/// store is quiet then, so both samples read the same bytes.
async fn sampled(srv: &TestServer) -> u64 {
    let share = sample_storage(&srv.shared).await.expect("storage sample");
    assert!(
        !share.led_vshards.is_empty(),
        "a lone node leads its vShards"
    );
    assert!(
        srv.shared.storage_shares.latest().is_some(),
        "the sample publishes a share"
    );
    srv.shared
        .database_metrics
        .storage_bytes(DatabaseId::DEFAULT)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_raise_and_a_purged_drop_lowers_the_storage_gauge() {
    let srv = TestServer::start().await;
    srv.shared
        .database_metrics
        .ensure_database(DatabaseId::DEFAULT);
    let baseline = sampled(&srv).await;

    srv.exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");
    for i in 0..50u32 {
        srv.exec(&format!(
            "INSERT INTO {COLLECTION} (id, payload) VALUES ('r{i}', 'payload-{i}-{}')",
            "x".repeat(64)
        ))
        .await
        .expect("insert");
    }
    let written = sampled(&srv).await;
    assert!(
        written > baseline,
        "50 rows raise the storage gauge: baseline {baseline}, after writes {written}"
    );

    srv.exec(&format!("DROP COLLECTION {COLLECTION} PURGE"))
        .await
        .expect("DROP COLLECTION PURGE");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut purged = sampled(&srv).await;
    while purged >= written && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        purged = sampled(&srv).await;
    }
    assert!(
        purged < written,
        "a purged drop lowers the storage gauge: after writes {written}, after purge {purged}"
    );
}

// SPDX-License-Identifier: BUSL-1.1

//! Collection-scoped quiesce drain regression test.
//!
//! Enforces the contract that `execute_unregister_collection`'s
//! pre-reclaim drain depends on:
//!
//! 1. An in-flight scan blocks `wait_until_drained` until the scan
//!    completes.
//! 2. Once `begin_drain` is called, subsequent `try_start_scan`
//!    attempts are refused with `ScanStartError::Draining`.
//! 3. Completing the last scan unblocks the drain future before any
//!    unlink-equivalent action (the reclaim handler) runs.
//! 4. The drain is collection-scoped — other collections / tenants
//!    are unaffected.
//! 5. After `forget`, the state is reset and new scans are accepted
//!    again (relevant when a `DROP ... CASCADE` partially fails and
//!    the purge is retried).

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::quiesce::{CollectionQuiesce, ScanStartError};
use tokio::time::timeout;

const DB: u64 = 0;

#[tokio::test]
async fn drain_blocks_until_in_flight_scan_completes() {
    let q = CollectionQuiesce::new();
    let guard = q.try_start_scan(DB, 1, "users").unwrap();
    q.begin_drain(DB, 1, "users");

    // Draining flag is observable.
    assert!(q.is_draining(DB, 1, "users"));

    // Subsequent scans refused.
    let err = q.try_start_scan(DB, 1, "users").unwrap_err();
    assert_eq!(err, ScanStartError::Draining);

    let q_clone = Arc::clone(&q);
    let drain = tokio::spawn(async move { q_clone.wait_until_drained(DB, 1, "users").await });

    // With the one scan still open, drain must not resolve within a
    // generous bound — we use 100ms as a proxy for "clearly not ready".
    assert!(
        timeout(Duration::from_millis(100), async {
            loop {
                if drain.is_finished() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err(),
        "drain resolved while an active scan guard was still held"
    );

    // Releasing the last scan unblocks the drain.
    drop(guard);
    timeout(Duration::from_secs(1), drain)
        .await
        .expect("drain did not resolve after last scan release")
        .expect("drain task panicked");
}

#[tokio::test]
async fn drain_is_scoped_to_one_collection() {
    let q = CollectionQuiesce::new();
    // Hold a scan on a sibling collection that must not be drained.
    let _sibling_guard = q.try_start_scan(DB, 1, "orders").unwrap();

    q.begin_drain(DB, 1, "users");
    // Immediately drainable because no scans were open on "users".
    timeout(Duration::from_secs(1), q.wait_until_drained(DB, 1, "users"))
        .await
        .expect("scoped drain should resolve immediately");

    // Sibling must still accept new scans.
    assert!(q.try_start_scan(DB, 1, "orders").is_ok());
    // Different tenant on the same name must be unaffected.
    assert!(q.try_start_scan(DB, 2, "users").is_ok());
    // Same tenant/name in a different database must be unaffected.
    assert!(q.try_start_scan(1, 1, "users").is_ok());
}

#[tokio::test]
async fn forget_resets_state_for_retry_semantics() {
    let q = CollectionQuiesce::new();
    q.begin_drain(DB, 1, "users");
    q.wait_until_drained(DB, 1, "users").await;
    q.forget(DB, 1, "users");

    assert!(!q.is_draining(DB, 1, "users"));
    assert!(q.try_start_scan(DB, 1, "users").is_ok());
}

#[tokio::test]
async fn concurrent_releases_all_unblock_the_drain() {
    let q = CollectionQuiesce::new();
    // Open five scans concurrently.
    let guards: Vec<_> = (0..5)
        .map(|_| q.try_start_scan(DB, 1, "users").unwrap())
        .collect();
    q.begin_drain(DB, 1, "users");

    let q_clone = Arc::clone(&q);
    let drain = tokio::spawn(async move { q_clone.wait_until_drained(DB, 1, "users").await });

    // Release four; drain still blocked on one.
    let mut iter = guards.into_iter();
    for _ in 0..4 {
        drop(iter.next().unwrap());
    }
    tokio::task::yield_now().await;
    assert!(
        !drain.is_finished(),
        "drain resolved with one scan still open"
    );

    drop(iter.next().unwrap());
    timeout(Duration::from_secs(1), drain)
        .await
        .expect("drain did not resolve after all scans released")
        .unwrap();
}

#[tokio::test]
async fn begin_drain_is_idempotent() {
    let q = CollectionQuiesce::new();
    q.begin_drain(DB, 1, "users");
    q.begin_drain(DB, 1, "users");
    q.begin_drain(DB, 1, "users");
    assert!(q.is_draining(DB, 1, "users"));
    assert_eq!(
        q.try_start_scan(DB, 1, "users").unwrap_err(),
        ScanStartError::Draining
    );
}

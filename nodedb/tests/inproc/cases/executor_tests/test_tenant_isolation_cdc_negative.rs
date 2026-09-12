// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: CDC — negative (subscription filtering) cases.
//!
//! Verifies that a subscriber scoped to Tenant B does NOT receive events
//! published by Tenant A, even when both write to the same collection.
//! The `recv_filtered` path must reject cross-tenant events.

use std::time::Duration;

use super::helpers::{TENANT_A, TENANT_B};
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation, ChangeStream, ReplayStart};
use nodedb::types::{Lsn, TenantId};
use nodedb_types::RowIdentity;

/// A subscription scoped to Tenant B must never deliver Tenant A's events,
/// even when both tenants write to the same collection.
#[tokio::test]
async fn cdc_tenant_b_subscription_rejects_tenant_a_events() {
    let stream = ChangeStream::new(1024);

    // Subscribe to "orders" filtered to Tenant B only.
    let mut sub_b = stream.subscribe(Some("orders".into()), Some(TenantId::new(TENANT_B)));

    // Publish several events for Tenant A.
    for i in 0..5u64 {
        stream.publish(ChangeEvent {
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(format!("a_order_{i}")),
            operation: ChangeOperation::Insert,
            timestamp_ms: (i + 1) * 1000,
            tenant_id: TenantId::new(TENANT_A),
            lsn: Lsn::new(i + 1),
            after: None,
        });
    }

    // Publish one event for Tenant B.
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("b_order_1"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 10_000,
        tenant_id: TenantId::new(TENANT_B),
        lsn: Lsn::new(100),
        after: None,
    });

    // recv_filtered must skip the 5 Tenant A events and deliver the 1 Tenant B event.
    let received = tokio::time::timeout(Duration::from_millis(500), sub_b.recv_filtered())
        .await
        .expect("timed out waiting for Tenant B's event")
        .expect("channel error");

    assert_eq!(
        received.tenant_id,
        TenantId::new(TENANT_B),
        "First event delivered to Tenant B subscription must belong to Tenant B"
    );
    assert_eq!(
        received.document_id.as_str(),
        "b_order_1",
        "Received wrong document_id: expected b_order_1, got {}",
        received.document_id
    );

    // No more events should be available immediately (the 5 A events were filtered).
    let second = tokio::time::timeout(Duration::from_millis(50), sub_b.recv_filtered()).await;
    assert!(
        second.is_err(),
        "No further events expected for Tenant B subscription; Tenant A's events must have been filtered"
    );
}

/// A subscription with no tenant filter receives events from all tenants (broadcast).
/// Verify this is intentional: the filter is opt-in.
#[tokio::test]
async fn cdc_unfiltered_subscription_receives_all_tenants() {
    let stream = ChangeStream::new(1024);

    // Subscribe with no filters.
    let mut sub_all = stream.subscribe(None, None);

    stream.publish(ChangeEvent {
        collection: "events".into(),
        document_id: RowIdentity::from_user_key("e_a"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(1),
        after: None,
    });
    stream.publish(ChangeEvent {
        collection: "events".into(),
        document_id: RowIdentity::from_user_key("e_b"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 2000,
        tenant_id: TenantId::new(TENANT_B),
        lsn: Lsn::new(2),
        after: None,
    });

    let ev1 = tokio::time::timeout(Duration::from_millis(200), sub_all.recv_filtered())
        .await
        .expect("timed out on first event")
        .expect("channel error");
    let ev2 = tokio::time::timeout(Duration::from_millis(200), sub_all.recv_filtered())
        .await
        .expect("timed out on second event")
        .expect("channel error");

    let tenant_ids: Vec<_> = [&ev1, &ev2].iter().map(|e| e.tenant_id).collect();
    assert!(
        tenant_ids.contains(&TenantId::new(TENANT_A)),
        "Unfiltered subscription must receive Tenant A events"
    );
    assert!(
        tenant_ids.contains(&TenantId::new(TENANT_B)),
        "Unfiltered subscription must receive Tenant B events"
    );
}

/// query_changes filters by tenant before applying the caller's result limit.
#[test]
fn cdc_query_changes_scopes_the_requested_tenant_before_limit() {
    let stream = ChangeStream::new(1024);

    // Two matching Tenant B events arrive before the caller's Tenant A event.
    // A limit applied before tenant filtering would consume the result budget
    // and return no event to Tenant A.
    for (lsn, document_id) in [(Lsn::new(1), "l_b_1"), (Lsn::new(2), "l_b_2")] {
        stream.publish(ChangeEvent {
            collection: "logs".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1_000,
            tenant_id: TenantId::new(TENANT_B),
            lsn,
            after: None,
        });
    }
    stream.publish(ChangeEvent {
        collection: "logs".into(),
        document_id: RowIdentity::from_user_key("l_a"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1_000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(3),
        after: None,
    });

    let a_events = stream
        .query_changes(
            TenantId::new(TENANT_A),
            Some("logs"),
            ReplayStart::Timestamp(0),
            1,
        )
        .expect("timestamp replay cannot expire")
        .events;

    assert_eq!(
        a_events.len(),
        1,
        "tenant filtering must precede limit so Tenant A receives its matching event"
    );
    assert_eq!(a_events[0].tenant_id, TenantId::new(TENANT_A));
    assert_eq!(a_events[0].document_id.as_str(), "l_a");
}

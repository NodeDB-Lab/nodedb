// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: CDC (Change Data Capture).
//!
//! Writes by Tenant A must NOT appear in Tenant B's change stream subscription.
//! The ChangeStream is scoped by `(collection, tenant_id)` — this test verifies it.

use super::helpers::{TENANT_A, TENANT_B};
use nodedb::control::change_stream::{ChangeEvent, ChangeOperation, ChangeStream, ReplayStart};
use nodedb::types::{Lsn, TenantId};
use nodedb_types::RowIdentity;

#[test]
fn cdc_stream_isolated_between_tenants() {
    let stream = ChangeStream::new(1024);

    // Subscribe Tenant B to "orders" — should only see Tenant B's events.
    let _sub_b = stream.subscribe(Some("orders".into()), Some(TenantId::new(TENANT_B)));

    // Publish a change event for Tenant A on "orders".
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("order_1"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(1),
        after: None,
    });

    // Publish a change event for Tenant B on "orders".
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("order_2"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 2000,
        tenant_id: TenantId::new(TENANT_B),
        lsn: Lsn::new(2),
        after: None,
    });

    // Ring-buffer replay requires a tenant and returns only that tenant's events.
    let a_events = stream
        .query_changes(
            TenantId::new(TENANT_A),
            Some("orders"),
            ReplayStart::Timestamp(0),
            100,
        )
        .expect("timestamp replay cannot expire")
        .events;
    let b_events = stream
        .query_changes(
            TenantId::new(TENANT_B),
            Some("orders"),
            ReplayStart::Timestamp(0),
            100,
        )
        .expect("timestamp replay cannot expire")
        .events;

    assert_eq!(a_events.len(), 1);
    assert_eq!(a_events[0].tenant_id, TenantId::new(TENANT_A));
    assert_eq!(a_events[0].document_id.as_str(), "order_1");
    assert_eq!(b_events.len(), 1);
    assert_eq!(b_events[0].tenant_id, TenantId::new(TENANT_B));
    assert_eq!(b_events[0].document_id.as_str(), "order_2");
}

#[test]
fn cdc_opaque_cursor_keeps_same_millisecond_events_pageable() {
    let stream = ChangeStream::new(1024);
    let tenant_id = TenantId::new(TENANT_A);

    for (lsn, document_id) in [(1, "first"), (2, "second"), (3, "third")] {
        stream.publish(ChangeEvent {
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document_id),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1_000,
            tenant_id,
            lsn: Lsn::new(lsn),
            after: None,
        });
    }

    let first_page = stream
        .query_changes(tenant_id, Some("orders"), ReplayStart::Timestamp(0), 1)
        .expect("timestamp replay cannot expire");
    assert_eq!(first_page.events.len(), 1);
    assert_eq!(first_page.events[0].document_id.as_str(), "first");

    let second_page = stream
        .query_changes(
            tenant_id,
            Some("orders"),
            ReplayStart::Cursor(first_page.events[0].cursor()),
            1,
        )
        .expect("fresh cursor must resume");
    assert_eq!(second_page.events.len(), 1);
    assert_eq!(second_page.events[0].document_id.as_str(), "second");
}

#[test]
fn cdc_different_collections_isolated() {
    let stream = ChangeStream::new(1024);

    // Same tenant, different collections.
    stream.publish(ChangeEvent {
        collection: "orders".into(),
        document_id: RowIdentity::from_user_key("o1"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 1000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(1),
        after: None,
    });
    stream.publish(ChangeEvent {
        collection: "users".into(),
        document_id: RowIdentity::from_user_key("u1"),
        operation: ChangeOperation::Insert,
        timestamp_ms: 2000,
        tenant_id: TenantId::new(TENANT_A),
        lsn: Lsn::new(2),
        after: None,
    });

    // Query only "orders" — should not include "users".
    let order_events = stream
        .query_changes(
            TenantId::new(TENANT_A),
            Some("orders"),
            ReplayStart::Timestamp(0),
            100,
        )
        .expect("timestamp replay cannot expire")
        .events;
    for event in &order_events {
        assert_eq!(event.collection, "orders");
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for Event Plane durable topics.
//!
//! Tests: topic registry, publish+consume, retention eviction.

use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::buffer::StreamBuffer;
use nodedb::event::cdc::event::CdcEvent;
use nodedb::event::cdc::stream_def::RetentionConfig;
use nodedb::event::topic::registry::EpTopicRegistry;
use nodedb::event::topic::types::TopicDef;
use nodedb::types::DatabaseId;
use nodedb_test_support::now_ms;

#[test]
fn topic_registry_crud() {
    let registry = EpTopicRegistry::new();

    let def = TopicDef {
        database_id: DatabaseId::new(7),
        tenant_id: 1,
        name: "events".into(),
        retention: RetentionConfig {
            max_events: 10_000,
            max_age_secs: 3600,
        },
        owner: "admin".into(),
        created_at: 1000,
        last_sequence: 0,
        last_lsn: 0,
    };
    registry.register(def);

    assert!(registry.get(DatabaseId::new(7), 1, "events").is_some());
    assert!(registry.get(DatabaseId::new(7), 1, "nonexistent").is_none());
    assert!(registry.get(DatabaseId::new(7), 2, "events").is_none()); // Different tenant.

    registry.unregister(DatabaseId::new(7), 1, "events");
    assert!(registry.get(DatabaseId::new(7), 1, "events").is_none());
}

#[test]
fn topic_buffer_publish_and_consume() {
    let retention = RetentionConfig {
        max_events: 100,
        max_age_secs: 3600,
    };
    let buf = StreamBuffer::new("topic:events".to_string(), retention);

    // Simulate publishing 3 messages.
    for i in 1..=3 {
        buf.push(CdcEvent {
            sequence: i,
            partition: 0,
            collection: "topic:events".into(),
            op: "PUBLISH".into(),
            row_id: format!("msg-{i}"),
            event_time: now_ms(),
            lsn: i,
            database_id: DatabaseId::new(7),
            tenant_id: 1,
            new_value: Some(serde_json::json!({"data": format!("message {i}")})),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: nodedb::event::EventSource::User,
        });
    }

    // Consume all from the initial composite position.
    let events = buf.read_from(CdcOffset::ZERO, 100);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].row_id, "msg-1");
    assert_eq!(events[2].row_id, "msg-3");

    // A legacy LSN cursor acknowledges the complete first LSN.
    let events = buf.read_from(CdcOffset::legacy_lsn(1), 100);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].lsn, 2);
}

#[test]
fn topic_retention_eviction() {
    let retention = RetentionConfig {
        max_events: 2,
        max_age_secs: 86_400,
    };
    let buf = StreamBuffer::new("topic:logs".to_string(), retention);

    for i in 1..=5 {
        buf.push(CdcEvent {
            sequence: i,
            partition: 0,
            collection: "topic:logs".into(),
            op: "PUBLISH".into(),
            row_id: format!("msg-{i}"),
            event_time: now_ms(),
            lsn: i,
            database_id: DatabaseId::new(7),
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: nodedb::event::EventSource::User,
        });
    }

    let events = buf.read_from(CdcOffset::ZERO, 100);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 4);
    assert_eq!(events[1].sequence, 5);
}

#[test]
fn topic_list_all() {
    let registry = EpTopicRegistry::new();

    for name in ["t1", "t2", "t3"] {
        registry.register(TopicDef {
            database_id: DatabaseId::new(7),
            tenant_id: 1,
            name: name.into(),
            retention: RetentionConfig::default(),
            owner: "admin".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        });
    }

    let all = registry.list_for_database_tenant(DatabaseId::new(7), 1);
    assert_eq!(all.len(), 3);
}

//! Integration tests for Event Plane durable topics.
//!
//! Tests: topic registry, publish+consume, retention eviction.

mod common;

use common::now_ms;
use nodedb::event::cdc::buffer::StreamBuffer;
use nodedb::event::cdc::event::CdcEvent;
use nodedb::event::cdc::stream_def::RetentionConfig;
use nodedb::event::topic::registry::EpTopicRegistry;
use nodedb::event::topic::types::TopicDef;

#[test]
fn topic_registry_crud() {
    let registry = EpTopicRegistry::new();

    let def = TopicDef {
        tenant_id: 1,
        name: "events".into(),
        retention: RetentionConfig {
            max_events: 10_000,
            max_age_secs: 3600,
        },
        owner: "admin".into(),
        created_at: 1000,
    };
    registry.register(def);

    assert!(registry.get(1, "events").is_some());
    assert!(registry.get(1, "nonexistent").is_none());
    assert!(registry.get(2, "events").is_none()); // Different tenant.

    registry.unregister(1, "events");
    assert!(registry.get(1, "events").is_none());
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
            tenant_id: 1,
            new_value: Some(serde_json::json!({"data": format!("message {i}")})),
            old_value: None,
            schema_version: 0,
        });
    }

    // Consume all from LSN 0.
    let events = buf.read_from_lsn(0, 100);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].row_id, "msg-1");
    assert_eq!(events[2].row_id, "msg-3");

    // Consume from LSN 1 (skip first message, get events with lsn > 1).
    let events = buf.read_from_lsn(1, 100);
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
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
        });
    }

    let events = buf.read_from_lsn(0, 100);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 4);
    assert_eq!(events[1].sequence, 5);
}

#[test]
fn topic_list_all() {
    let registry = EpTopicRegistry::new();

    for name in ["t1", "t2", "t3"] {
        registry.register(TopicDef {
            tenant_id: 1,
            name: name.into(),
            retention: RetentionConfig::default(),
            owner: "admin".into(),
            created_at: 0,
        });
    }

    let all = registry.list_for_tenant(1);
    assert_eq!(all.len(), 3);
}

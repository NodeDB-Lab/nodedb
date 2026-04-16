//! Regression coverage: StreamRegistry must index streams by
//! `(tenant_id, collection)` and return shared references for the routing
//! hot path.
//!
//! Today `find_matching` does a full-scan of every registered stream across
//! every tenant on every WriteEvent and deep-clones each match. Both costs
//! scale with `O(total_streams)` instead of `O(matching_streams)`.
//!
//! This file locks in two properties:
//!   * The return type is `Vec<Arc<ChangeStreamDef>>` — fan-out to the router
//!     must not deep-copy a kilobyte-sized `ChangeStreamDef` per match.
//!   * Lookup cost is bounded by match count, not total stream count
//!     (register many off-collection streams, still observe a single match).

use std::sync::Arc;

use nodedb::event::cdc::registry::StreamRegistry;
use nodedb::event::cdc::stream_def::{
    ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
};

fn stream_def(tenant: u32, name: &str, collection: &str) -> ChangeStreamDef {
    ChangeStreamDef {
        tenant_id: tenant,
        name: name.into(),
        collection: collection.into(),
        op_filter: OpFilter::all(),
        format: StreamFormat::Json,
        retention: RetentionConfig::default(),
        compaction: CompactionConfig::default(),
        webhook: nodedb::event::webhook::WebhookConfig::default(),
        late_data: LateDataPolicy::default(),
        kafka: nodedb::event::kafka::KafkaDeliveryConfig::default(),
        owner: "admin".into(),
        created_at: 0,
    }
}

#[test]
fn find_matching_returns_shared_refs_not_deep_clones() {
    let reg = StreamRegistry::new();
    reg.register(stream_def(1, "orders_stream_a", "orders"));
    reg.register(stream_def(1, "orders_stream_b", "orders"));

    // The router calls find_matching on every WriteEvent. Returning
    // `Vec<Arc<ChangeStreamDef>>` keeps the hot path O(match_count refcount
    // bumps) instead of O(match_count * sizeof(ChangeStreamDef)) clones.
    let matches: Vec<Arc<ChangeStreamDef>> = reg.find_matching(1, "orders");
    assert_eq!(matches.len(), 2);

    // Repeat lookups should hand out refcount-equal handles to the same
    // underlying definition — no fresh clone per call.
    let again: Vec<Arc<ChangeStreamDef>> = reg.find_matching(1, "orders");
    let a0 = matches
        .iter()
        .find(|d| d.name == "orders_stream_a")
        .unwrap();
    let b0 = again.iter().find(|d| d.name == "orders_stream_a").unwrap();
    assert!(
        Arc::ptr_eq(a0, b0),
        "find_matching must hand out shared Arcs, not clone the definition per call"
    );
}

#[test]
fn find_matching_scales_with_match_count_not_total_streams() {
    // Seed a large fleet of streams that do NOT match. If find_matching
    // is indexed by (tenant, collection), these never enter the scan.
    let reg = StreamRegistry::new();
    for t in 0..50 {
        for i in 0..200 {
            reg.register(stream_def(t, &format!("other_{t}_{i}"), "unrelated"));
        }
    }
    // Two actual matches for the hot collection.
    reg.register(stream_def(1, "hot_a", "orders"));
    reg.register(stream_def(1, "hot_b", "orders"));

    let matches: Vec<Arc<ChangeStreamDef>> = reg.find_matching(1, "orders");
    assert_eq!(
        matches.len(),
        2,
        "must return only the matching streams, not all 10_002 streams"
    );

    // Wildcard registrations are the one documented exception — they are
    // indexed on a separate wildcard bucket and still scanned, but only
    // that small bucket (not the full 10k).
    reg.register(stream_def(1, "firehose", "*"));
    let with_wildcard: Vec<Arc<ChangeStreamDef>> = reg.find_matching(1, "orders");
    assert_eq!(
        with_wildcard.len(),
        3,
        "wildcard stream must still match the collection"
    );
}

#[test]
fn find_matching_isolates_by_tenant() {
    // An indexed registry must use tenant as part of the key — it's the
    // single most important dimension for multi-tenant deployments.
    let reg = StreamRegistry::new();
    reg.register(stream_def(1, "t1_stream", "orders"));
    reg.register(stream_def(2, "t2_stream", "orders"));

    let t1: Vec<Arc<ChangeStreamDef>> = reg.find_matching(1, "orders");
    assert_eq!(t1.len(), 1);
    assert_eq!(t1[0].tenant_id, 1);

    let t2: Vec<Arc<ChangeStreamDef>> = reg.find_matching(2, "orders");
    assert_eq!(t2.len(), 1);
    assert_eq!(t2[0].tenant_id, 2);
}

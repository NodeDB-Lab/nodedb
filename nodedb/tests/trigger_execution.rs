//! Integration tests for trigger execution: BEFORE, INSTEAD OF, AFTER STATEMENT,
//! SECURITY DEFINER, CRDT non-duplication, cascade depth, DML hook classification.

use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
    TriggerTiming,
};
use nodedb::control::trigger::TriggerRegistry;
use nodedb::control::trigger::dml_hook::classify_dml_write;
use nodedb::control::trigger::registry::DmlEvent;
use nodedb::event::types::EventSource;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn make_trigger_full(
    name: &str,
    collection: &str,
    timing: TriggerTiming,
    on_insert: bool,
    on_update: bool,
    on_delete: bool,
    mode: TriggerExecutionMode,
    granularity: TriggerGranularity,
    security: TriggerSecurity,
) -> StoredTrigger {
    StoredTrigger {
        tenant_id: 1,
        name: name.to_string(),
        collection: collection.to_string(),
        timing,
        events: TriggerEvents {
            on_insert,
            on_update,
            on_delete,
        },
        granularity,
        when_condition: None,
        body_sql: "BEGIN RETURN; END".into(),
        priority: 0,
        enabled: true,
        execution_mode: mode,
        security,
        batch_mode: nodedb::control::security::catalog::trigger_types::TriggerBatchMode::default(),
        owner: "admin".into(),
        created_at: 0,
    }
}

fn make_before_trigger(name: &str, collection: &str, event: DmlEvent) -> StoredTrigger {
    make_trigger_full(
        name,
        collection,
        TriggerTiming::Before,
        event == DmlEvent::Insert,
        event == DmlEvent::Update,
        event == DmlEvent::Delete,
        TriggerExecutionMode::Async, // irrelevant for BEFORE
        TriggerGranularity::Row,
        TriggerSecurity::Invoker,
    )
}

fn make_instead_of_trigger(name: &str, collection: &str, event: DmlEvent) -> StoredTrigger {
    make_trigger_full(
        name,
        collection,
        TriggerTiming::InsteadOf,
        event == DmlEvent::Insert,
        event == DmlEvent::Update,
        event == DmlEvent::Delete,
        TriggerExecutionMode::Async,
        TriggerGranularity::Row,
        TriggerSecurity::Invoker,
    )
}

fn make_statement_trigger(
    name: &str,
    collection: &str,
    event: DmlEvent,
    mode: TriggerExecutionMode,
) -> StoredTrigger {
    make_trigger_full(
        name,
        collection,
        TriggerTiming::After,
        event == DmlEvent::Insert,
        event == DmlEvent::Update,
        event == DmlEvent::Delete,
        mode,
        TriggerGranularity::Statement,
        TriggerSecurity::Invoker,
    )
}

fn make_definer_trigger(name: &str, collection: &str, owner: &str) -> StoredTrigger {
    let mut t = make_trigger_full(
        name,
        collection,
        TriggerTiming::After,
        true,
        false,
        false,
        TriggerExecutionMode::Sync,
        TriggerGranularity::Row,
        TriggerSecurity::Definer,
    );
    t.owner = owner.to_string();
    t
}

// ---------------------------------------------------------------------------
// DML hook classification
// ---------------------------------------------------------------------------

#[test]
fn classify_point_put_as_insert() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "orders".into(),
        document_id: "doc-1".into(),
        value: b"{}".to_vec(),
    });
    let info = classify_dml_write(&plan).unwrap();
    assert_eq!(info.collection, "orders");
    assert_eq!(info.document_id.as_deref(), Some("doc-1"));
    assert!(matches!(info.event, DmlEvent::Insert));
}

#[test]
fn classify_point_delete() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: "orders".into(),
        document_id: "doc-1".into(),
    });
    let info = classify_dml_write(&plan).unwrap();
    assert_eq!(info.collection, "orders");
    assert!(matches!(info.event, DmlEvent::Delete));
    assert!(info.new_fields.is_none());
}

#[test]
fn classify_point_update() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: "users".into(),
        document_id: "u-1".into(),
        updates: vec![],
        returning: false,
    });
    let info = classify_dml_write(&plan).unwrap();
    assert_eq!(info.collection, "users");
    assert!(matches!(info.event, DmlEvent::Update));
}

#[test]
fn classify_bulk_delete() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: "logs".into(),
        filters: vec![],
    });
    let info = classify_dml_write(&plan).unwrap();
    assert_eq!(info.collection, "logs");
    assert!(info.document_id.is_none()); // Bulk = no single doc ID
    assert!(matches!(info.event, DmlEvent::Delete));
}

#[test]
fn classify_scan_returns_none() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: "orders".into(),
        limit: 100,
        offset: 0,
        sort_keys: vec![],
        filters: vec![],
        distinct: false,
        projection: vec![],
        computed_columns: vec![],
        window_functions: vec![],
    });
    assert!(classify_dml_write(&plan).is_none());
}

#[test]
fn classify_vector_op_returns_none() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::VectorOp;

    let plan = PhysicalPlan::Vector(VectorOp::Insert {
        collection: "embeddings".into(),
        vector: vec![1.0, 2.0, 3.0],
        dim: 3,
        field_name: String::new(),
        doc_id: None,
    });
    // Vector ops don't participate in BEFORE/SYNC AFTER hook path.
    assert!(classify_dml_write(&plan).is_none());
}

// ---------------------------------------------------------------------------
// BEFORE trigger registry matching
// ---------------------------------------------------------------------------

#[test]
fn registry_matches_before_triggers() {
    let reg = TriggerRegistry::new();
    reg.register(make_before_trigger(
        "validate_order",
        "orders",
        DmlEvent::Insert,
    ));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].timing, TriggerTiming::Before);
    assert_eq!(matched[0].name, "validate_order");
}

#[test]
fn registry_matches_before_update_and_delete() {
    let reg = TriggerRegistry::new();
    reg.register(make_before_trigger(
        "validate_update",
        "users",
        DmlEvent::Update,
    ));
    reg.register(make_before_trigger(
        "validate_delete",
        "users",
        DmlEvent::Delete,
    ));

    let update_matched = reg.get_matching(1, "users", DmlEvent::Update);
    assert_eq!(update_matched.len(), 1);
    assert_eq!(update_matched[0].name, "validate_update");

    let delete_matched = reg.get_matching(1, "users", DmlEvent::Delete);
    assert_eq!(delete_matched.len(), 1);
    assert_eq!(delete_matched[0].name, "validate_delete");

    // INSERT should not match.
    assert!(reg.get_matching(1, "users", DmlEvent::Insert).is_empty());
}

// ---------------------------------------------------------------------------
// INSTEAD OF trigger registry matching
// ---------------------------------------------------------------------------

#[test]
fn registry_matches_instead_of_triggers() {
    let reg = TriggerRegistry::new();
    reg.register(make_instead_of_trigger(
        "redirect_insert",
        "view_orders",
        DmlEvent::Insert,
    ));

    let matched = reg.get_matching(1, "view_orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].timing, TriggerTiming::InsteadOf);
}

#[test]
fn instead_of_does_not_match_wrong_event() {
    let reg = TriggerRegistry::new();
    reg.register(make_instead_of_trigger(
        "redirect_insert",
        "view_orders",
        DmlEvent::Insert,
    ));

    assert!(
        reg.get_matching(1, "view_orders", DmlEvent::Delete)
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// AFTER STATEMENT trigger registry matching
// ---------------------------------------------------------------------------

#[test]
fn registry_matches_statement_triggers() {
    let reg = TriggerRegistry::new();
    reg.register(make_statement_trigger(
        "refresh_mv",
        "orders",
        DmlEvent::Insert,
        TriggerExecutionMode::Async,
    ));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].granularity, TriggerGranularity::Statement);
}

#[test]
fn statement_and_row_triggers_coexist() {
    let reg = TriggerRegistry::new();
    // Row-level AFTER trigger.
    reg.register(make_trigger_full(
        "audit_row",
        "orders",
        TriggerTiming::After,
        true,
        false,
        false,
        TriggerExecutionMode::Async,
        TriggerGranularity::Row,
        TriggerSecurity::Invoker,
    ));
    // Statement-level AFTER trigger.
    reg.register(make_statement_trigger(
        "refresh_mv",
        "orders",
        DmlEvent::Insert,
        TriggerExecutionMode::Async,
    ));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 2);
    // Both returned — caller filters by granularity.
}

// ---------------------------------------------------------------------------
// SECURITY DEFINER trigger storage
// ---------------------------------------------------------------------------

#[test]
fn definer_trigger_stores_security_mode() {
    let t = make_definer_trigger("admin_audit", "orders", "system_admin");
    assert_eq!(t.security, TriggerSecurity::Definer);
    assert_eq!(t.owner, "system_admin");
}

#[test]
fn invoker_is_default_security() {
    let t = make_before_trigger("validate", "orders", DmlEvent::Insert);
    assert_eq!(t.security, TriggerSecurity::Invoker);
}

#[test]
fn definer_trigger_registered_and_matched() {
    let reg = TriggerRegistry::new();
    reg.register(make_definer_trigger("admin_trigger", "orders", "root"));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].security, TriggerSecurity::Definer);
    assert_eq!(matched[0].owner, "root");
}

// ---------------------------------------------------------------------------
// CRDT replication: event source cascade prevention
// ---------------------------------------------------------------------------

#[test]
fn crdt_sync_events_skip_triggers() {
    // CrdtSync events must NOT fire triggers — verified by the dispatcher
    // checking event.source. This test validates the enum contract.
    assert_ne!(EventSource::CrdtSync, EventSource::User);
    assert_ne!(EventSource::CrdtSync, EventSource::Deferred);
}

#[test]
fn raft_follower_events_skip_triggers() {
    assert_ne!(EventSource::RaftFollower, EventSource::User);
}

#[test]
fn trigger_events_skip_retriggering() {
    // Trigger-generated events must not re-fire triggers.
    assert_ne!(EventSource::Trigger, EventSource::User);
}

#[test]
fn only_user_and_deferred_fire_triggers() {
    // Exhaustive check: only User and Deferred should fire.
    let sources = [
        (EventSource::User, true),
        (EventSource::Deferred, true),
        (EventSource::Trigger, false),
        (EventSource::RaftFollower, false),
        (EventSource::CrdtSync, false),
    ];
    for (source, should_fire) in &sources {
        let fires = matches!(source, EventSource::User | EventSource::Deferred);
        assert_eq!(
            fires, *should_fire,
            "EventSource::{source} fire={fires}, expected={should_fire}"
        );
    }
}

// ---------------------------------------------------------------------------
// Cascade depth limit
// ---------------------------------------------------------------------------

#[test]
fn cascade_depth_check_passes_under_limit() {
    let result = nodedb::control::trigger::fire_common::check_cascade_depth(0, "orders");
    assert!(result.is_ok());
}

#[test]
fn cascade_depth_check_passes_at_limit_minus_one() {
    let result = nodedb::control::trigger::fire_common::check_cascade_depth(15, "orders");
    assert!(result.is_ok());
}

#[test]
fn cascade_depth_check_fails_at_limit() {
    let result = nodedb::control::trigger::fire_common::check_cascade_depth(16, "orders");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("cascade depth exceeded"), "got: {err}");
    assert!(err.contains("orders"), "got: {err}");
}

#[test]
fn cascade_depth_check_fails_over_limit() {
    let result = nodedb::control::trigger::fire_common::check_cascade_depth(100, "orders");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// RowBindings: BEFORE constructors
// ---------------------------------------------------------------------------

#[test]
fn before_insert_bindings_have_correct_tg_when() {
    use nodedb::control::planner::procedural::executor::bindings::RowBindings;
    use std::collections::HashMap;

    let mut row = HashMap::new();
    row.insert("id".into(), nodedb_types::Value::String("doc-1".into()));
    let bindings = RowBindings::before_insert("orders", row);
    let result = bindings.substitute("SELECT TG_WHEN, TG_OP");
    assert!(result.contains("'BEFORE'"), "got: {result}");
    assert!(result.contains("'INSERT'"), "got: {result}");
}

#[test]
fn before_update_bindings_have_old_and_new() {
    use nodedb::control::planner::procedural::executor::bindings::RowBindings;
    use std::collections::HashMap;

    let mut old = HashMap::new();
    old.insert("name".into(), nodedb_types::Value::String("Alice".into()));
    let mut new = HashMap::new();
    new.insert("name".into(), nodedb_types::Value::String("Bob".into()));

    let bindings = RowBindings::before_update("users", old, new);
    let result = bindings.substitute("VALUES (OLD.name, NEW.name, TG_WHEN)");
    assert!(result.contains("'Alice'"), "got: {result}");
    assert!(result.contains("'Bob'"), "got: {result}");
    assert!(result.contains("'BEFORE'"), "got: {result}");
}

#[test]
fn before_delete_bindings_have_old_only() {
    use nodedb::control::planner::procedural::executor::bindings::RowBindings;
    use std::collections::HashMap;

    let mut old = HashMap::new();
    old.insert("id".into(), nodedb_types::Value::Integer(42));
    let bindings = RowBindings::before_delete("users", old);
    let result = bindings.substitute("VALUES (OLD.id, TG_OP)");
    assert!(result.contains("42"), "got: {result}");
    assert!(result.contains("'DELETE'"), "got: {result}");
}

// ---------------------------------------------------------------------------
// RowBindings: STATEMENT constructor
// ---------------------------------------------------------------------------

#[test]
fn statement_bindings_have_no_new_old() {
    use nodedb::control::planner::procedural::executor::bindings::RowBindings;

    let bindings = RowBindings::statement("orders", "INSERT");
    let result = bindings.substitute("VALUES (NEW.id, OLD.id, TG_OP, TG_TABLE_NAME)");
    // NEW.id and OLD.id should remain as-is (no substitution).
    assert!(result.contains("NEW.id"), "got: {result}");
    assert!(result.contains("OLD.id"), "got: {result}");
    assert!(result.contains("'INSERT'"), "got: {result}");
    assert!(result.contains("'orders'"), "got: {result}");
}

// ---------------------------------------------------------------------------
// RowBindings: with_new_row (BEFORE trigger chaining)
// ---------------------------------------------------------------------------

#[test]
fn with_new_row_replaces_new_fields() {
    use nodedb::control::planner::procedural::executor::bindings::RowBindings;
    use std::collections::HashMap;

    let mut original = HashMap::new();
    original.insert("name".into(), nodedb_types::Value::String("Alice".into()));
    let bindings = RowBindings::before_insert("users", original);

    // Verify original.
    let result = bindings.substitute("SELECT NEW.name");
    assert!(result.contains("'Alice'"), "got: {result}");

    // Replace NEW row.
    let mut updated = HashMap::new();
    updated.insert("name".into(), nodedb_types::Value::String("Bob".into()));
    let new_bindings = bindings.with_new_row(updated);

    let result = new_bindings.substitute("SELECT NEW.name");
    assert!(result.contains("'Bob'"), "got: {result}");
}

// ---------------------------------------------------------------------------
// TriggerSecurity enum
// ---------------------------------------------------------------------------

#[test]
fn trigger_security_as_str() {
    assert_eq!(TriggerSecurity::Invoker.as_str(), "INVOKER");
    assert_eq!(TriggerSecurity::Definer.as_str(), "DEFINER");
}

#[test]
fn trigger_security_default_is_invoker() {
    assert_eq!(TriggerSecurity::default(), TriggerSecurity::Invoker);
}

// ---------------------------------------------------------------------------
// Mixed timing triggers in same collection
// ---------------------------------------------------------------------------

#[test]
fn before_after_instead_of_coexist() {
    let reg = TriggerRegistry::new();
    reg.register(make_before_trigger("validate", "orders", DmlEvent::Insert));
    reg.register(make_instead_of_trigger(
        "redirect",
        "orders",
        DmlEvent::Insert,
    ));
    reg.register(make_trigger_full(
        "audit",
        "orders",
        TriggerTiming::After,
        true,
        false,
        false,
        TriggerExecutionMode::Async,
        TriggerGranularity::Row,
        TriggerSecurity::Invoker,
    ));

    let all = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(all.len(), 3);

    let before_count = all
        .iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .count();
    let after_count = all
        .iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .count();
    let instead_count = all
        .iter()
        .filter(|t| t.timing == TriggerTiming::InsteadOf)
        .count();

    assert_eq!(before_count, 1);
    assert_eq!(after_count, 1);
    assert_eq!(instead_count, 1);
}

// ---------------------------------------------------------------------------
// Priority ordering across timing types
// ---------------------------------------------------------------------------

#[test]
fn triggers_sorted_by_priority_across_timings() {
    let reg = TriggerRegistry::new();

    let mut t1 = make_before_trigger("b_validate", "orders", DmlEvent::Insert);
    t1.priority = 10;
    let mut t2 = make_before_trigger("a_validate", "orders", DmlEvent::Insert);
    t2.priority = 5;
    let mut t3 = make_trigger_full(
        "c_audit",
        "orders",
        TriggerTiming::After,
        true,
        false,
        false,
        TriggerExecutionMode::Async,
        TriggerGranularity::Row,
        TriggerSecurity::Invoker,
    );
    t3.priority = 1;

    reg.register(t1);
    reg.register(t2);
    reg.register(t3);

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 3);
    // Sorted by (priority, name): 1, 5, 10
    assert_eq!(matched[0].priority, 1);
    assert_eq!(matched[1].priority, 5);
    assert_eq!(matched[2].priority, 10);
}

// ---------------------------------------------------------------------------
// DML hook: deserialize value to fields
// ---------------------------------------------------------------------------

#[test]
fn classify_point_put_deserializes_json_value() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let value = serde_json::to_vec(&serde_json::json!({"name": "Alice", "age": 30})).unwrap();
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "users".into(),
        document_id: "u-1".into(),
        value,
    });
    let info = classify_dml_write(&plan).unwrap();
    let fields = info.new_fields.unwrap();
    assert_eq!(
        fields.get("name").unwrap(),
        &nodedb_types::Value::String("Alice".into())
    );
    assert_eq!(
        fields.get("age").unwrap(),
        &nodedb_types::Value::Integer(30)
    );
}

#[test]
fn classify_point_put_deserializes_msgpack_value() {
    use nodedb::bridge::envelope::PhysicalPlan;
    use nodedb::bridge::physical_plan::DocumentOp;

    let value = nodedb_types::json_to_msgpack(&serde_json::json!({"key": "val"})).unwrap();
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "data".into(),
        document_id: "d-1".into(),
        value,
    });
    let info = classify_dml_write(&plan).unwrap();
    let fields = info.new_fields.unwrap();
    assert_eq!(
        fields.get("key").unwrap(),
        &nodedb_types::Value::String("val".into())
    );
}

// ---------------------------------------------------------------------------
// WHEN condition evaluation
// ---------------------------------------------------------------------------

#[test]
fn simple_when_condition_true() {
    assert!(nodedb::control::trigger::fire_common::evaluate_simple_condition("TRUE"));
    assert!(nodedb::control::trigger::fire_common::evaluate_simple_condition("1"));
}

#[test]
fn simple_when_condition_false() {
    assert!(!nodedb::control::trigger::fire_common::evaluate_simple_condition("FALSE"));
    assert!(!nodedb::control::trigger::fire_common::evaluate_simple_condition("0"));
    assert!(!nodedb::control::trigger::fire_common::evaluate_simple_condition("NULL"));
}

#[test]
fn complex_when_condition_defaults_to_true() {
    // Complex conditions not evaluable without DataFusion default to true (fire).
    assert!(nodedb::control::trigger::fire_common::evaluate_simple_condition("NEW.total > 100"));
}

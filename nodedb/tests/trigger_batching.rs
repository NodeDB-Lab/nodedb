//! Integration tests for trigger batching infrastructure:
//! collector, classify, WHEN filter, batch config.

use nodedb::control::security::catalog::trigger_types::TriggerBatchMode;
use nodedb::control::trigger::batch::BatchConfig;
use nodedb::control::trigger::batch::classify::classify_trigger_body;
use nodedb::control::trigger::batch::collector::{TriggerBatchCollector, TriggerBatchRow};
use nodedb::control::trigger::batch::when_filter::{count_passing, filter_batch_by_when};

// ---------------------------------------------------------------------------
// BatchConfig
// ---------------------------------------------------------------------------

#[test]
fn default_batch_size_is_1024() {
    assert_eq!(BatchConfig::default().batch_size, 1024);
}

// ---------------------------------------------------------------------------
// Collector: accumulate + flush
// ---------------------------------------------------------------------------

fn row(id: &str) -> TriggerBatchRow {
    TriggerBatchRow::from_decoded(Some(std::collections::HashMap::new()), None, id.to_string())
}

#[test]
fn collector_batches_at_threshold() {
    let mut c = TriggerBatchCollector::new(3);
    assert!(c.push("orders", "INSERT", 1, row("a")).is_none());
    assert!(c.push("orders", "INSERT", 1, row("b")).is_none());
    let batch = c.push("orders", "INSERT", 1, row("c")).unwrap();
    assert_eq!(batch.rows.len(), 3);
    assert_eq!(batch.collection, "orders");
    assert_eq!(batch.operation, "INSERT");
}

#[test]
fn collector_flush_partial() {
    let mut c = TriggerBatchCollector::new(100);
    c.push("t", "INSERT", 1, row("a"));
    c.push("t", "INSERT", 1, row("b"));
    let batch = c.flush().unwrap();
    assert_eq!(batch.rows.len(), 2);
    assert!(!c.has_pending());
}

#[test]
fn collector_flushes_on_collection_change() {
    let mut c = TriggerBatchCollector::new(100);
    c.push("orders", "INSERT", 1, row("1"));
    c.push("orders", "INSERT", 1, row("2"));
    let flushed = c.push("users", "INSERT", 1, row("3")).unwrap();
    assert_eq!(flushed.collection, "orders");
    assert_eq!(flushed.rows.len(), 2);
    let remaining = c.flush().unwrap();
    assert_eq!(remaining.collection, "users");
}

#[test]
fn collector_flushes_on_operation_change() {
    let mut c = TriggerBatchCollector::new(100);
    c.push("t", "INSERT", 1, row("1"));
    let flushed = c.push("t", "DELETE", 1, row("2")).unwrap();
    assert_eq!(flushed.operation, "INSERT");
}

#[test]
fn collector_empty_flush_none() {
    let mut c = TriggerBatchCollector::new(10);
    assert!(c.flush().is_none());
}

// ---------------------------------------------------------------------------
// Classify: batch mode detection
// ---------------------------------------------------------------------------

#[test]
fn classify_single_insert_batch_safe() {
    let body = "BEGIN INSERT INTO audit (id) VALUES (NEW.id); END";
    assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
}

#[test]
fn classify_same_target_batch_safe() {
    let body = "BEGIN \
        INSERT INTO audit (id, op) VALUES (NEW.id, 'a'); \
        INSERT INTO audit (id, op) VALUES (NEW.id, 'b'); \
    END";
    assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
}

#[test]
fn classify_different_targets_row_at_a_time() {
    let body = "BEGIN \
        INSERT INTO audit (id) VALUES (NEW.id); \
        INSERT INTO vectors (id) VALUES (NEW.id); \
    END";
    assert_eq!(classify_trigger_body(body), TriggerBatchMode::RowAtATime);
}

#[test]
fn classify_conditional_different_targets() {
    let body = "BEGIN \
        IF NEW.active = TRUE THEN \
            INSERT INTO active (id) VALUES (NEW.id); \
        ELSE \
            INSERT INTO archive (id) VALUES (NEW.id); \
        END IF; \
    END";
    assert_eq!(classify_trigger_body(body), TriggerBatchMode::RowAtATime);
}

#[test]
fn classify_conditional_same_target_safe() {
    let body = "BEGIN \
        IF NEW.total > 100 THEN \
            INSERT INTO audit (id, note) VALUES (NEW.id, 'high'); \
        ELSE \
            INSERT INTO audit (id, note) VALUES (NEW.id, 'low'); \
        END IF; \
    END";
    assert_eq!(classify_trigger_body(body), TriggerBatchMode::BatchSafe);
}

#[test]
fn classify_invalid_body_row_at_a_time() {
    assert_eq!(
        classify_trigger_body("not valid"),
        TriggerBatchMode::RowAtATime
    );
}

// ---------------------------------------------------------------------------
// WHEN filter
// ---------------------------------------------------------------------------

#[test]
fn when_none_all_pass() {
    let rows = vec![row("a"), row("b"), row("c")];
    let mask = filter_batch_by_when(&rows, "c", "INSERT", None);
    assert_eq!(mask, vec![true, true, true]);
}

#[test]
fn when_true_all_pass() {
    let rows = vec![row("a"), row("b")];
    let mask = filter_batch_by_when(&rows, "c", "INSERT", Some("TRUE"));
    assert_eq!(mask, vec![true, true]);
}

#[test]
fn when_false_none_pass() {
    let rows = vec![row("a"), row("b")];
    let mask = filter_batch_by_when(&rows, "c", "INSERT", Some("FALSE"));
    assert_eq!(mask, vec![false, false]);
}

#[test]
fn when_null_none_pass() {
    let rows = vec![row("a")];
    let mask = filter_batch_by_when(&rows, "c", "INSERT", Some("NULL"));
    assert_eq!(mask, vec![false]);
}

#[test]
fn count_passing_works() {
    assert_eq!(count_passing(&[true, false, true]), 2);
    assert_eq!(count_passing(&[false, false]), 0);
    assert_eq!(count_passing(&[]), 0);
}

// ---------------------------------------------------------------------------
// TriggerBatchMode enum
// ---------------------------------------------------------------------------

#[test]
fn batch_mode_default_is_batch_safe() {
    assert_eq!(TriggerBatchMode::default(), TriggerBatchMode::BatchSafe);
}

#[test]
fn batch_mode_as_str() {
    assert_eq!(TriggerBatchMode::BatchSafe.as_str(), "BATCH_SAFE");
    assert_eq!(TriggerBatchMode::RowAtATime.as_str(), "ROW_AT_A_TIME");
}

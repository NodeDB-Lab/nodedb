//! Streaming MV processor: updates aggregate state from CdcEvents.
//!
//! Called from the Event Plane consumer for each event. Finds all
//! streaming MVs sourced from the event's stream, extracts GROUP BY
//! values and aggregate inputs from the event's new_value, and
//! incrementally updates each MV's state.

use sonic_rs;
use tracing::trace;

use crate::event::cdc::event::CdcEvent;
use crate::event::types::WriteEvent;

use super::registry::MvRegistry;
use super::state::MvState;
use super::types::AggFunction;

/// Process a WriteEvent against all streaming MVs sourced from a given stream.
///
/// Converts the WriteEvent's raw payload to JSON for field extraction,
/// then updates each matching MV's aggregate state.
pub fn process_write_event_for_mvs(event: &WriteEvent, registry: &MvRegistry, stream_name: &str) {
    let mv_states = registry.find_by_source(event.tenant_id.as_u32(), stream_name);
    if mv_states.is_empty() {
        return;
    }

    // Deserialize new_value for field extraction.
    let new_value: Option<serde_json::Value> = event.new_value.as_ref().and_then(|bytes| {
        nodedb_types::json_from_msgpack(bytes)
            .ok()
            .or_else(|| sonic_rs::from_slice(bytes).ok())
    });

    let op_str = event.op.to_string();

    // Build a lightweight CdcEvent for the MV processor.
    let cdc_event = CdcEvent {
        sequence: event.sequence,
        partition: event.vshard_id.as_u16(),
        collection: event.collection.to_string(),
        op: op_str,
        row_id: event.row_id.as_str().to_string(),
        event_time: 0,
        lsn: event.lsn.as_u64(),
        tenant_id: event.tenant_id.as_u32(),
        new_value,
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: event.system_time_ms,
        valid_time_ms: event.valid_time_ms,
    };

    for mv_state in &mv_states {
        update_mv(&cdc_event, mv_state);
    }
}

/// Process a CDC event against all streaming MVs sourced from its stream.
pub fn process_event_for_mvs(event: &CdcEvent, registry: &MvRegistry, stream_name: &str) {
    let mv_states = registry.find_by_source(event.tenant_id, stream_name);
    if mv_states.is_empty() {
        return;
    }

    for mv_state in &mv_states {
        update_mv(event, mv_state);
    }
}

/// Backfill a streaming MV from the source stream's retention buffer.
///
/// Called on `CREATE MATERIALIZED VIEW ... STREAMING` to bootstrap the MV
/// with all events currently in the stream's buffer. Without backfill,
/// new MVs start empty and only aggregate future events.
pub fn backfill_from_buffer(
    mv_state: &MvState,
    buffer: &crate::event::cdc::buffer::StreamBuffer,
) -> u64 {
    let events = buffer.read_from_lsn(0, usize::MAX);
    let mut processed = 0u64;

    for event in &events {
        update_mv(event, mv_state);
        processed += 1;
    }

    if processed > 0 {
        tracing::info!(
            mv = %mv_state.name,
            events = processed,
            "streaming MV backfilled from buffer"
        );
    }

    processed
}

/// Update a single MV's state from a CdcEvent.
fn update_mv(event: &CdcEvent, mv_state: &MvState) {
    // Extract GROUP BY key from the event.
    let group_key = extract_group_key(event, &mv_state.group_by_columns);

    // Extract aggregate values from the event.
    let agg_values: Vec<f64> = mv_state
        .aggregates
        .iter()
        .map(|agg| extract_agg_value(event, agg.function, &agg.input_expr))
        .collect();

    mv_state.update_with_time(&group_key, &agg_values, event.event_time);

    trace!(
        mv = %mv_state.name,
        group_key = %group_key,
        "streaming MV updated"
    );
}

/// Extract the GROUP BY key by concatenating column values from the event.
///
/// Looks for column values in:
/// 1. CdcEvent's own fields (event_type, collection, partition, etc.)
/// 2. new_value JSON object fields
fn extract_group_key(event: &CdcEvent, group_by_columns: &[String]) -> String {
    let mut parts = Vec::with_capacity(group_by_columns.len());

    for col in group_by_columns {
        let val = match col.as_str() {
            "event_type" | "op" => event.op.clone(),
            "collection" => event.collection.clone(),
            "partition" => event.partition.to_string(),
            "row_id" => event.row_id.clone(),
            "tenant_id" => event.tenant_id.to_string(),
            // Look in new_value JSON.
            field => event
                .new_value
                .as_ref()
                .and_then(|v| v.get(field))
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "NULL".to_string()),
        };
        parts.push(val);
    }

    parts.join(":")
}

/// Extract a numeric value for an aggregate function from the event.
///
/// For COUNT, always returns 1.0 (each event counts as one).
/// For SUM/MIN/MAX/AVG, parses the input expression as a field path
/// and extracts the numeric value from new_value.
fn extract_agg_value(event: &CdcEvent, func: AggFunction, input_expr: &str) -> f64 {
    if func == AggFunction::Count {
        return 1.0; // Each event counts as one.
    }

    // Parse input_expr as a field name (simple case).
    // Supports: "field_name" or "doc_get(new_value, '$.field')".
    let field_name = if input_expr.contains("doc_get") {
        // Extract field path from doc_get(new_value, '$.field').
        input_expr
            .split("'$.")
            .nth(1)
            .and_then(|s| s.split('\'').next())
            .unwrap_or(input_expr)
    } else {
        input_expr.trim()
    };

    // Look up the field in new_value.
    event
        .new_value
        .as_ref()
        .and_then(|v| v.get(field_name))
        .and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_f64(),
            serde_json::Value::String(s) => s.parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(f64::NAN) // NaN → skipped by GroupState::update.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::streaming_mv::types::AggDef;

    fn make_event(op: &str, total: f64) -> CdcEvent {
        CdcEvent {
            sequence: 1,
            partition: 0,
            collection: "orders".into(),
            op: op.into(),
            row_id: "o-1".into(),
            event_time: 0,
            lsn: 100,
            tenant_id: 1,
            new_value: Some(serde_json::json!({"total": total, "status": "active"})),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
        }
    }

    #[test]
    fn extract_group_key_from_event_field() {
        let event = make_event("INSERT", 99.0);
        let key = extract_group_key(&event, &["event_type".into()]);
        assert_eq!(key, "INSERT");
    }

    #[test]
    fn extract_group_key_from_new_value() {
        let event = make_event("INSERT", 99.0);
        let key = extract_group_key(&event, &["status".into()]);
        assert_eq!(key, "active");
    }

    #[test]
    fn extract_group_key_multi_column() {
        let event = make_event("INSERT", 99.0);
        let key = extract_group_key(&event, &["event_type".into(), "status".into()]);
        assert_eq!(key, "INSERT:active");
    }

    #[test]
    fn extract_agg_value_count() {
        let event = make_event("INSERT", 99.0);
        assert_eq!(extract_agg_value(&event, AggFunction::Count, ""), 1.0);
    }

    #[test]
    fn extract_agg_value_sum() {
        let event = make_event("INSERT", 42.5);
        assert_eq!(extract_agg_value(&event, AggFunction::Sum, "total"), 42.5);
    }

    #[test]
    fn process_event_updates_mv() {
        let registry = MvRegistry::new();
        let def = crate::event::streaming_mv::types::StreamingMvDef {
            tenant_id: 1,
            name: "order_stats".into(),
            source_stream: "orders_stream".into(),
            group_by_columns: vec!["event_type".into()],
            aggregates: vec![
                AggDef {
                    output_name: "cnt".into(),
                    function: AggFunction::Count,
                    input_expr: String::new(),
                },
                AggDef {
                    output_name: "total_sum".into(),
                    function: AggFunction::Sum,
                    input_expr: "total".into(),
                },
            ],
            filter_expr: None,
            owner: "admin".into(),
            created_at: 0,
        };
        registry.register(def);

        let e1 = make_event("INSERT", 100.0);
        let e2 = make_event("INSERT", 50.0);
        let e3 = make_event("UPDATE", 30.0);

        process_event_for_mvs(&e1, &registry, "orders_stream");
        process_event_for_mvs(&e2, &registry, "orders_stream");
        process_event_for_mvs(&e3, &registry, "orders_stream");

        let state = registry.get_state(1, "order_stats").unwrap();
        let results = state.read_results();

        let insert_row = results.iter().find(|(k, _)| k == "INSERT").unwrap();
        assert_eq!(insert_row.1[0].1, 2.0); // COUNT = 2
        assert_eq!(insert_row.1[1].1, 150.0); // SUM = 150

        let update_row = results.iter().find(|(k, _)| k == "UPDATE").unwrap();
        assert_eq!(update_row.1[0].1, 1.0); // COUNT = 1
    }
}

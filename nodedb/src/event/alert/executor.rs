// SPDX-License-Identifier: BUSL-1.1

//! Alert evaluation loop on the Event Plane.
//!
//! Background Tokio task that periodically evaluates all enabled alert rules.
//! For each alert:
//! 1. Build and execute an aggregate SQL query over the alert's window.
//! 2. Compare each group's result against the condition.
//! 3. Feed results through hysteresis state machine.
//! 4. Dispatch notifications on state transitions (Fired / Recovered).
//!
//! Runs on the Event Plane (Send + Sync). NEVER does storage I/O directly —
//! SQL queries are dispatched through the Control Plane → Data Plane path.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_physical::physical_plan::timeseries::TimeseriesOp;

use super::hysteresis::HysteresisTransition;
use super::notify::dispatch_notifications;
use super::registry::AlertRegistry;
use super::types::AlertDef;

/// Spawn the alert evaluation loop as a background Tokio task.
pub fn spawn_alert_eval_loop(
    shared_state: Arc<SharedState>,
    registry: Arc<AlertRegistry>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        alert_eval_loop(shared_state, registry, shutdown).await;
    })
}

async fn alert_eval_loop(
    state: Arc<SharedState>,
    registry: Arc<AlertRegistry>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Initial delay to let the system warm up. Shutdown ends the delay: a
    // server stopped within it must not wait it out.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        _ = shutdown.wait_for(|stopping| *stopping) => return,
    }

    // Use the shared hysteresis manager from SharedState so DROP/ALTER handlers
    // and the eval loop operate on the same state.
    let hysteresis = &state.alert_hysteresis;

    loop {
        // Sleep for the shortest alert window (minimum 1 second).
        let sleep_ms = next_eval_interval_ms(&registry);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("alert eval loop shutting down");
                    return;
                }
            }
        }

        let alerts = registry.list_all_enabled();
        if alerts.is_empty() {
            continue;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for alert in &alerts {
            if let Err(e) = evaluate_alert(&state, alert, hysteresis, now_ms).await {
                warn!(
                    alert = alert.name,
                    collection = alert.collection,
                    error = %e,
                    "alert evaluation failed"
                );
            }
        }
    }
}

/// Evaluate a single alert rule: execute aggregate query, check condition per group,
/// feed through hysteresis, dispatch notifications.
async fn evaluate_alert(
    state: &Arc<SharedState>,
    alert: &AlertDef,
    hysteresis: &super::hysteresis::HysteresisManager,
    now_ms: u64,
) -> crate::Result<()> {
    let tenant_id = TenantId::new(alert.tenant_id);
    let window_start = now_ms.saturating_sub(alert.window_ms);

    debug!(alert = alert.name, %window_start, %now_ms, "evaluating alert");

    // Dispatch aggregate scan directly to Data Plane via SPSC bridge.
    let results =
        execute_aggregate_scan(state, tenant_id, alert, window_start as i64, now_ms as i64).await?;

    // Process each group's result through hysteresis.
    for (group_key, agg_value) in &results {
        let condition_met = alert
            .condition
            .op
            .evaluate(*agg_value, alert.condition.threshold);

        let transition = hysteresis.evaluate(super::hysteresis::EvaluateParams {
            database_id: alert.database_id,
            tenant_id: alert.tenant_id,
            alert_name: &alert.name,
            group_key,
            condition_met,
            value: *agg_value,
            fire_after: alert.fire_after,
            recover_after: alert.recover_after,
            now_ms,
        });

        if transition != HysteresisTransition::NoChange {
            info!(
                alert = alert.name,
                group = group_key,
                value = agg_value,
                threshold = alert.condition.threshold,
                transition = ?transition,
                "alert state transition"
            );

            dispatch_notifications(state, alert, group_key, *agg_value, transition, now_ms).await;
        }
    }

    Ok(())
}

/// Dispatch an aggregate scan to the Data Plane via the SPSC bridge.
///
/// Builds a `TimeseriesOp::Scan` with the alert's aggregate function and
/// group-by columns, dispatches via `dispatch_system`, and decodes the
/// MessagePack response into `(group_key, agg_value)` pairs.
async fn execute_aggregate_scan(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    alert: &AlertDef,
    window_start: i64,
    window_end: i64,
) -> crate::Result<Vec<(String, f64)>> {
    // Encode WHERE filter as MessagePack for the Data Plane filter evaluator.
    let filters = match &alert.where_filter {
        Some(f) => zerompk::to_msgpack_vec(f).unwrap_or_default(),
        None => Vec::new(),
    };

    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(
            crate::types::DatabaseId::new(alert.database_id),
            &alert.collection,
        ),
        time_range: (window_start, window_end),
        projection: Vec::new(),
        limit: 10_000, // Safety cap on group cardinality.
        filters,
        sort_keys: Vec::new(),
        bucket_interval_ms: 0, // No time bucketing — aggregate over entire window.
        group_by: alert.group_by.clone(),
        aggregates: vec![(
            alert.condition.agg_func.clone(),
            alert.condition.column.clone(),
        )],
        gap_fill: String::new(),
        computed_columns: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });

    let payload = sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::EventPlane,
            tenant_id,
            crate::types::DatabaseId::new(alert.database_id),
            &alert.collection,
            plan,
        ),
        Duration::from_secs(30),
    )
    .await?;

    decode_aggregate_response(&payload, &alert.group_by, &alert.condition.agg_func)
}

/// Decode the Data Plane's MessagePack response into (group_key, agg_value) pairs.
///
/// The response is an `rmpv::Value::Array` of `Map` rows. Each row contains
/// the group-by columns and the aggregate value (e.g., `"avg_temperature"`).
fn decode_aggregate_response(
    payload: &[u8],
    group_by: &[String],
    agg_func: &str,
) -> crate::Result<Vec<(String, f64)>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let value: rmpv::Value =
        crate::util::bounded_msgpack::read_value(payload).map_err(|e| crate::Error::Internal {
            detail: format!("decode aggregate response: {e}"),
        })?;

    let rows = match value {
        rmpv::Value::Array(rows) => rows,
        _ => return Ok(Vec::new()),
    };

    let mut results = Vec::with_capacity(rows.len());
    for row in &rows {
        let rmpv::Value::Map(fields) = row else {
            continue;
        };

        // Extract group key: concatenate group-by column values.
        let group_key = if group_by.is_empty() {
            "__all__".to_string()
        } else {
            group_by
                .iter()
                .filter_map(|col| {
                    fields.iter().find_map(|(k, v)| {
                        if k.as_str().is_some_and(|s| s == col) {
                            Some(v.as_str().unwrap_or("").to_string())
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>()
                .join("\0")
        };

        // Extract aggregate value by matching the agg key pattern: "{func}_{column}".
        let agg_value = fields.iter().find_map(|(k, v)| {
            let key_str = k.as_str()?;
            if key_str.starts_with(agg_func) {
                v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
            } else {
                None
            }
        });

        if let Some(val) = agg_value {
            results.push((group_key, val));
        }
    }

    Ok(results)
}

/// Determine the eval loop sleep interval.
/// Uses the shortest window_ms among all enabled alerts (minimum 1s).
fn next_eval_interval_ms(registry: &AlertRegistry) -> u64 {
    let alerts = registry.list_all_enabled();
    alerts
        .iter()
        .map(|a| a.window_ms)
        .filter(|&ms| ms > 0)
        .min()
        .unwrap_or(60_000) // Default: 1 minute
        .max(1_000) // Minimum: 1 second
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::alert::types::{AlertCondition, CompareOp};

    fn make_alert(name: &str) -> AlertDef {
        AlertDef {
            database_id: 0,
            tenant_id: 1,
            name: name.into(),
            collection: "sensor_data".into(),
            where_filter: Some("device_type = 'compressor'".into()),
            condition: AlertCondition {
                agg_func: "avg".into(),
                column: "temperature".into(),
                op: CompareOp::Gt,
                threshold: 90.0,
            },
            group_by: vec!["device_id".into()],
            window_ms: 300_000,
            fire_after: 3,
            recover_after: 2,
            severity: "critical".into(),
            notify_targets: Vec::new(),
            enabled: true,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn next_interval_uses_shortest() {
        let reg = AlertRegistry::new();
        let mut a1 = make_alert("a1");
        a1.window_ms = 300_000;
        reg.register(a1);
        let mut a2 = make_alert("a2");
        a2.window_ms = 60_000;
        a2.name = "a2".into();
        a2.collection = "other".into();
        reg.register(a2);
        assert_eq!(next_eval_interval_ms(&reg), 60_000);
    }

    #[test]
    fn next_interval_minimum_1s() {
        let reg = AlertRegistry::new();
        let mut a = make_alert("fast");
        a.window_ms = 500;
        reg.register(a);
        assert_eq!(next_eval_interval_ms(&reg), 1_000);
    }

    #[test]
    fn next_interval_default_1min() {
        let reg = AlertRegistry::new();
        assert_eq!(next_eval_interval_ms(&reg), 60_000);
    }

    #[test]
    fn decode_empty_payload() {
        let result = decode_aggregate_response(&[], &[], "avg").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_grouped_response() {
        // Build a response: [{device_id: "pump-1", avg_temperature: 95.5}]
        let row = rmpv::Value::Map(vec![
            (
                rmpv::Value::String("device_id".into()),
                rmpv::Value::String("pump-1".into()),
            ),
            (
                rmpv::Value::String("avg_temperature".into()),
                rmpv::Value::F64(95.5),
            ),
        ]);
        let array = rmpv::Value::Array(vec![row]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap();

        let results = decode_aggregate_response(&buf, &["device_id".to_string()], "avg").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "pump-1");
        assert!((results[0].1 - 95.5).abs() < 1e-12);
    }

    #[test]
    fn decode_no_group_response() {
        let row = rmpv::Value::Map(vec![(
            rmpv::Value::String("max_cpu".into()),
            rmpv::Value::F64(99.1),
        )]);
        let array = rmpv::Value::Array(vec![row]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap();

        let results = decode_aggregate_response(&buf, &[], "max").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "__all__");
        assert!((results[0].1 - 99.1).abs() < 1e-12);
    }
}

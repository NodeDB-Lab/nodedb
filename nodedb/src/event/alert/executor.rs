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

use nodedb_query::agg_key::canonical_agg_key;

use super::group_key::{AlertGroupCell, AlertGroupKey};
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
            // The source notifies for a mirror's rows. A second notification
            // starts at PROMOTE.
            if state
                .mirror
                .fence
                .is_read_only(crate::types::DatabaseId::new(alert.database_id))
            {
                continue;
            }
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
                group = %group_key,
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
) -> crate::Result<Vec<(AlertGroupKey, f64)>> {
    // Encode WHERE filter as MessagePack for the Data Plane filter evaluator.
    // An encode error fails the evaluation: an empty payload would mean "no
    // filter" and aggregate every row of the window.
    let filters = match &alert.where_filter {
        Some(f) => zerompk::to_msgpack_vec(f).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("alert '{}' WHERE filter: {e}", alert.name),
        })?,
        None => Vec::new(),
    };

    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(
            crate::types::DatabaseId::new(alert.database_id),
            &alert.collection,
        ),
        time_range: (window_start, window_end),
        projection: Vec::new(),
        // Every group of the window is evaluated: a cut would leave groups
        // past it unalerted with nothing saying so. The Data Plane bounds the
        // read by its scan byte budget and fails loudly past it.
        limit: usize::MAX,
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
        having: Vec::new(),
        tiering: None,
        // The evaluator reads each group's value under its canonical key.
        output_columns: Vec::new(),
    });

    let payload = sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::EventPlane,
            tenant_id,
            nodedb_types::CollectionKey::from_bare(
                crate::types::DatabaseId::new(alert.database_id),
                &alert.collection,
            ),
            plan,
        ),
        Duration::from_secs(30),
    )
    .await?;

    decode_aggregate_response(
        &payload,
        &alert.group_by,
        &canonical_agg_key(&alert.condition.agg_func, &alert.condition.column),
    )
}

/// Decode the Data Plane's MessagePack response into typed
/// `(group_key, agg_value)` pairs.
///
/// The response is an array of map rows, one per group. Each row holds every
/// GROUP BY column and the aggregate under its canonical key (for example
/// `avg(temperature)`). A row that is not a map, a missing column, or a
/// non-numeric aggregate is an error. A NULL aggregate (every input value
/// NULL) gives the group nothing to compare, so the group is not evaluated
/// this window.
fn decode_aggregate_response(
    payload: &[u8],
    group_by: &[String],
    agg_key: &str,
) -> crate::Result<Vec<(AlertGroupKey, f64)>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let value: rmpv::Value =
        crate::util::bounded_msgpack::read_value(payload).map_err(|e| crate::Error::Internal {
            detail: format!("decode aggregate response: {e}"),
        })?;

    let rows = match value {
        rmpv::Value::Array(rows) => rows,
        other => {
            return Err(crate::Error::Internal {
                detail: format!("aggregate response is not a row array: {other}"),
            });
        }
    };

    let mut results = Vec::with_capacity(rows.len());
    for row in &rows {
        let rmpv::Value::Map(fields) = row else {
            return Err(crate::Error::Internal {
                detail: format!("aggregate response row is not a map: {row}"),
            });
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k.as_str() == Some(name))
                .map(|(_, v)| v)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("aggregate response row has no column '{name}'"),
                })
        };

        let mut cells = Vec::with_capacity(group_by.len());
        for column in group_by {
            cells.push(AlertGroupCell::from_rmpv(column, field(column)?)?);
        }
        let group_key = AlertGroupKey::from_cells(cells);

        let agg_value = match field(agg_key)? {
            rmpv::Value::Nil => continue,
            rmpv::Value::F64(f) => *f,
            rmpv::Value::F32(f) => f64::from(*f),
            rmpv::Value::Integer(n) => match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => i as f64,
                (None, Some(u)) => u as f64,
                (None, None) => {
                    return Err(crate::Error::Internal {
                        detail: format!("aggregate '{agg_key}' is an integer out of range"),
                    });
                }
            },
            other => {
                return Err(crate::Error::Internal {
                    detail: format!("aggregate '{agg_key}' is not numeric: {other}"),
                });
            }
        };
        results.push((group_key, agg_value));
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

    fn encode_rows(rows: Vec<rmpv::Value>) -> Vec<u8> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv::Value::Array(rows)).unwrap();
        buf
    }

    fn row(fields: Vec<(&str, rmpv::Value)>) -> rmpv::Value {
        rmpv::Value::Map(
            fields
                .into_iter()
                .map(|(k, v)| (rmpv::Value::String(k.into()), v))
                .collect(),
        )
    }

    #[test]
    fn decode_empty_payload() {
        let result = decode_aggregate_response(&[], &[], "avg(t)").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_grouped_response() {
        let buf = encode_rows(vec![row(vec![
            ("device_id", rmpv::Value::String("pump-1".into())),
            ("avg(temperature)", rmpv::Value::F64(95.5)),
        ])]);
        let results =
            decode_aggregate_response(&buf, &["device_id".to_string()], "avg(temperature)")
                .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].0,
            AlertGroupKey::from_cells(vec![AlertGroupCell::Text("pump-1".into())])
        );
        assert!((results[0].1 - 95.5).abs() < 1e-12);
    }

    #[test]
    fn decode_no_group_response() {
        let buf = encode_rows(vec![row(vec![("max(cpu)", rmpv::Value::F64(99.1))])]);
        let results = decode_aggregate_response(&buf, &[], "max(cpu)").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, AlertGroupKey::whole_window());
        assert_eq!(results[0].0.to_string(), "__all__");
        assert!((results[0].1 - 99.1).abs() < 1e-12);
    }

    /// Integer `1`, text `'1'`, and NULL group cells are three groups. A
    /// string join of the cells once read every non-text cell as `""`, so
    /// integer and NULL groups shared one hysteresis state.
    #[test]
    fn typed_group_cells_stay_distinct() {
        let agg = "avg(t)";
        let buf = encode_rows(vec![
            row(vec![
                ("g", rmpv::Value::Integer(1.into())),
                (agg, rmpv::Value::F64(1.0)),
            ]),
            row(vec![
                ("g", rmpv::Value::String("1".into())),
                (agg, rmpv::Value::F64(2.0)),
            ]),
            row(vec![("g", rmpv::Value::Nil), (agg, rmpv::Value::F64(3.0))]),
            row(vec![
                ("g", rmpv::Value::Integer(2.into())),
                (agg, rmpv::Value::F64(4.0)),
            ]),
        ]);
        let results = decode_aggregate_response(&buf, &["g".to_string()], agg).unwrap();
        let keys: std::collections::HashSet<_> = results.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys.len(), 4, "every typed group keeps its own key");
        let rendered: Vec<String> = results.iter().map(|(k, _)| k.to_string()).collect();
        assert_eq!(rendered, vec!["1", "'1'", "NULL", "2"]);
    }

    #[test]
    fn a_row_that_is_not_a_map_is_an_error() {
        let buf = encode_rows(vec![rmpv::Value::Integer(7.into())]);
        assert!(matches!(
            decode_aggregate_response(&buf, &[], "avg(t)"),
            Err(crate::Error::Internal { .. })
        ));
    }

    #[test]
    fn a_missing_group_column_is_an_error() {
        let buf = encode_rows(vec![row(vec![("avg(t)", rmpv::Value::F64(1.0))])]);
        match decode_aggregate_response(&buf, &["zone".to_string()], "avg(t)") {
            Err(crate::Error::Internal { detail }) => {
                assert!(detail.contains("'zone'"), "{detail}")
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn a_non_numeric_aggregate_is_an_error_and_null_is_skipped() {
        let text = encode_rows(vec![row(vec![(
            "max(t)",
            rmpv::Value::String("hot".into()),
        )])]);
        assert!(decode_aggregate_response(&text, &[], "max(t)").is_err());
        let null = encode_rows(vec![row(vec![("avg(t)", rmpv::Value::Nil)])]);
        assert!(
            decode_aggregate_response(&null, &[], "avg(t)")
                .unwrap()
                .is_empty()
        );
    }
}

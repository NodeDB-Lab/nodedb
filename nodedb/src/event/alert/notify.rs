// SPDX-License-Identifier: BUSL-1.1

//! Alert notification dispatch: TOPIC, WEBHOOK, INSERT INTO.
//!
//! Called by the eval loop when a hysteresis transition occurs (Fired or Recovered).
//! Runs on the Event Plane (Send + Sync, Tokio). NEVER does storage I/O directly.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::{AtomicBody, StatementExecutor};
use crate::control::security::egress::{EgressError, EgressFault, EgressPolicy, classify};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_types::Value;

use super::group_key::AlertGroupKey;
use super::hysteresis::HysteresisTransition;
use super::types::{AlertDef, AlertEvent, NotifyTarget};

/// Dispatch notifications for an alert transition.
///
/// Sends to all configured targets (TOPIC, WEBHOOK, INSERT INTO).
/// Errors are logged but do not prevent other targets from being notified.
pub async fn dispatch_notifications(
    state: &Arc<SharedState>,
    alert: &AlertDef,
    group_key: &AlertGroupKey,
    value: f64,
    transition: HysteresisTransition,
    now_ms: u64,
) {
    let status_str = match transition {
        HysteresisTransition::Fired => "ACTIVE",
        HysteresisTransition::Recovered => "CLEARED",
        HysteresisTransition::NoChange => return, // No notification needed.
    };

    let event = AlertEvent {
        alert_name: alert.name.clone(),
        group_key: group_key.to_string(),
        severity: alert.severity.clone(),
        status: status_str.to_string(),
        value,
        threshold: alert.condition.threshold,
        timestamp_ms: now_ms,
        collection: alert.collection.clone(),
    };

    for target in &alert.notify_targets {
        match target {
            NotifyTarget::Topic { name } => {
                notify_topic(
                    state,
                    crate::types::DatabaseId::new(alert.database_id),
                    alert.tenant_id,
                    name,
                    &event,
                )
                .await;
            }
            NotifyTarget::Webhook { url } => {
                let timeout = Duration::from_secs(state.tuning.scheduler.webhook_timeout_secs);
                notify_webhook_with_client(
                    state.http_client(),
                    state.remote_sources.egress(),
                    url,
                    &event,
                    timeout,
                )
                .await;
            }
            NotifyTarget::InsertInto { table, columns } => {
                notify_insert(
                    state,
                    crate::types::DatabaseId::new(alert.database_id),
                    alert.tenant_id,
                    &alert.owner,
                    table,
                    columns,
                    &event,
                )
                .await;
            }
        }
    }
}

/// Publish alert event to a CDC topic.
async fn notify_topic(
    state: &SharedState,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    event: &AlertEvent,
) {
    let payload = match sonic_rs::to_string(event) {
        Ok(p) => p,
        Err(e) => {
            warn!(alert = event.alert_name, error = %e, "failed to serialize alert event");
            return;
        }
    };

    match crate::event::topic::publish::publish_to_topic(
        state,
        database_id,
        tenant_id,
        topic_name,
        &payload,
    )
    .await
    {
        Ok(seq) => {
            info!(
                alert = event.alert_name,
                topic = topic_name,
                seq,
                status = event.status,
                "alert event published to topic"
            );
        }
        Err(e) => {
            warn!(
                alert = event.alert_name,
                topic = topic_name,
                error = %e,
                "failed to publish alert event to topic"
            );
        }
    }
}

/// What one alert webhook delivery came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookOutcome {
    /// The endpoint accepted the event.
    Delivered,
    /// The `[egress]` policy refused the URL or an address it resolved to.
    /// Nothing was sent, and no retry runs.
    Refused(EgressError),
    /// The endpoint answered a permanent client error with this status.
    Rejected(u16),
    /// Every attempt failed, or the event did not serialize.
    Failed,
}

/// HTTP POST alert event to a webhook URL using a shared `reqwest::Client`.
///
/// The URL is user-supplied: it passes `policy` before any request, and
/// `client` is the egress client built on `policy`, so every resolved
/// address is checked at connect time.
///
/// Retries with exponential backoff (3 attempts, 100ms base). Reusing the
/// client avoids re-building the connection pool / TLS session cache per
/// notification — `CREATE ALERT` rules firing at high rate would otherwise
/// cause a SYN flood and TLS-handshake-dominated CPU.
pub async fn notify_webhook_with_client(
    client: &reqwest::Client,
    policy: &EgressPolicy,
    url: &str,
    event: &AlertEvent,
    per_request_timeout: Duration,
) -> WebhookOutcome {
    // An IP-literal host is never resolved, so only this check refuses it.
    if let Err(refused) = policy.check_url(url) {
        warn!(
            alert = event.alert_name,
            error = %refused,
            "alert webhook URL fails the [egress] policy, not sending"
        );
        return WebhookOutcome::Refused(refused);
    }
    let body = match sonic_rs::to_string(event) {
        Ok(b) => b,
        Err(e) => {
            warn!(alert = event.alert_name, error = %e, "failed to serialize alert event");
            return WebhookOutcome::Failed;
        }
    };

    let max_retries = 3u32;
    for attempt in 0..max_retries {
        match client
            .post(url)
            .header("Content-Type", "application/json")
            .timeout(per_request_timeout)
            .body(body.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    alert = event.alert_name,
                    url,
                    status = event.status,
                    "alert webhook delivered"
                );
                return WebhookOutcome::Delivered;
            }
            Ok(resp) if resp.status().is_client_error() && resp.status().as_u16() != 429 => {
                // 4xx (except 429) is permanent failure.
                warn!(
                    alert = event.alert_name,
                    url,
                    status_code = resp.status().as_u16(),
                    "alert webhook permanently rejected"
                );
                return WebhookOutcome::Rejected(resp.status().as_u16());
            }
            Ok(resp) => {
                warn!(
                    alert = event.alert_name,
                    url,
                    status_code = resp.status().as_u16(),
                    attempt,
                    "alert webhook delivery failed, retrying"
                );
            }
            Err(e) => {
                if let EgressFault::Refused(refused) = classify(&e) {
                    warn!(
                        alert = event.alert_name,
                        error = %refused,
                        "alert webhook refused by the egress policy, not retrying"
                    );
                    return WebhookOutcome::Refused(refused);
                }
                warn!(
                    alert = event.alert_name,
                    url,
                    attempt,
                    error = %e.without_url(),
                    "alert webhook delivery error, retrying"
                );
            }
        }

        // Exponential backoff: 100ms, 200ms, 400ms.
        let backoff = Duration::from_millis(100 * (1 << attempt));
        tokio::time::sleep(backoff).await;
    }

    warn!(
        alert = event.alert_name,
        url, "alert webhook delivery failed after all retries"
    );
    WebhookOutcome::Failed
}

fn build_alert_insert_sql(table: &str, columns: &[String], event: &AlertEvent) -> Option<String> {
    if !columns.is_empty() && columns.len() != 7 {
        return None;
    }
    let default_columns = [
        "alert_name",
        "group_key",
        "severity",
        "status",
        "value",
        "threshold",
        "timestamp_ms",
    ];
    let column_names = if columns.is_empty() {
        default_columns.to_vec()
    } else {
        columns.iter().map(String::as_str).collect::<Vec<_>>()
    };
    let col_list = column_names
        .into_iter()
        .map(::nodedb_types::quote_ident)
        .collect::<Vec<_>>()
        .join(", ");
    let timestamp_ms = i64::try_from(event.timestamp_ms).ok()?;
    let values = [
        Value::String(event.alert_name.clone()),
        Value::String(event.group_key.clone()),
        Value::String(event.severity.clone()),
        Value::String(event.status.clone()),
        Value::Float(event.value),
        Value::Float(event.threshold),
        Value::Integer(timestamp_ms),
    ]
    .iter()
    .map(::nodedb_types::Value::to_sql_literal)
    .collect::<Vec<_>>()
    .join(", ");
    let table = ::nodedb_types::quote_ident(table);
    Some(format!(
        "BEGIN INSERT INTO {table} ({col_list}) VALUES ({values}); END"
    ))
}

/// INSERT alert event into a history table via StatementExecutor.
async fn notify_insert(
    state: &SharedState,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    owner: &str,
    table: &str,
    columns: &[String],
    event: &AlertEvent,
) {
    let Some(sql) = build_alert_insert_sql(table, columns, event) else {
        warn!(
            alert = event.alert_name,
            table,
            column_count = columns.len(),
            timestamp_ms = event.timestamp_ms,
            "alert history target has invalid column arity or timestamp"
        );
        return;
    };

    let identity = alert_identity(TenantId::new(tenant_id), owner);
    let block = match crate::control::planner::procedural::parse_block(&sql) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                alert = event.alert_name,
                table,
                error = %e,
                "failed to parse INSERT statement for alert history"
            );
            return;
        }
    };

    let executor = StatementExecutor::with_source_in_database(
        state,
        identity,
        TenantId::new(tenant_id),
        database_id,
        0,
        crate::event::EventSource::User,
    )
    .with_atomic_body(AtomicBody::alert(&event.alert_name));
    let bindings = RowBindings::empty();

    if let Err(e) = executor.execute_block(&block, &bindings).await {
        warn!(
            alert = event.alert_name,
            table,
            error = %e,
            "failed to INSERT alert event into history table"
        );
    } else {
        info!(
            alert = event.alert_name,
            table,
            status = event.status,
            "alert event inserted into history table"
        );
    }
}

/// Create a system identity for alert notification execution.
fn alert_identity(tenant_id: TenantId, owner: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        owner,
        tenant_id,
        vec![Role::Superuser],
        true,
        None,
        crate::control::security::identity::DatabaseSet::All,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_alert_sql_quotes_complete_statement_and_enforces_arity() {
        let event = AlertEvent {
            alert_name: "cpu'; DROP TABLE users; --".into(),
            group_key: "group".into(),
            severity: "high".into(),
            status: "ACTIVE".into(),
            value: 1.5,
            threshold: 1.0,
            timestamp_ms: 42,
            collection: "metrics".into(),
        };
        let columns = ["a", "b", "c", "d", "e", "f", "g"]
            .map(str::to_string)
            .to_vec();
        let sql = build_alert_insert_sql("history; DROP TABLE audit", &columns, &event)
            .expect("valid alert SQL");
        assert!(sql.contains("\"history; DROP TABLE audit\""));
        assert!(sql.contains("'cpu''; DROP TABLE users; --'"));
        assert!(sql.contains("(\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\")"));
        assert!(build_alert_insert_sql("history", &columns[..6], &event).is_none());
    }

    /// An alert webhook URL is user-supplied. One the egress policy refuses
    /// is never contacted, and no retry runs.
    #[tokio::test]
    async fn a_webhook_the_egress_policy_refuses_is_never_contacted() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let policy = Arc::new(EgressPolicy::strict());
        let client =
            crate::control::security::egress::outbound_client(Arc::clone(&policy)).expect("client");
        let event = AlertEvent {
            alert_name: "cpu".into(),
            group_key: "group".into(),
            severity: "high".into(),
            status: "ACTIVE".into(),
            value: 1.5,
            threshold: 1.0,
            timestamp_ms: 42,
            collection: "metrics".into(),
        };
        for (url, expected) in [
            (
                format!("http://localhost:{port}/hook"),
                EgressError::NonHttps,
            ),
            (
                format!("https://127.0.0.1:{port}/hook"),
                EgressError::IpLiteral,
            ),
            (
                format!("https://localhost:{port}/hook"),
                EgressError::PrivateAddress,
            ),
        ] {
            let outcome =
                notify_webhook_with_client(&client, &policy, &url, &event, Duration::from_secs(2))
                    .await;
            assert_eq!(outcome, WebhookOutcome::Refused(expected), "{url}");
        }
        let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(accepted.is_err(), "a refused webhook reached the listener");
    }
}

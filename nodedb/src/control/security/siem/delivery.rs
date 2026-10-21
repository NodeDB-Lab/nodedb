// SPDX-License-Identifier: BUSL-1.1

//! Webhook delivery of buffered SIEM events.
//!
//! Delivery is Control/Event-Plane async work: an HTTP POST and nothing else.
//! A failed attempt returns every drained event to the head of its buffer, so
//! a webhook outage retries on the next tick instead of discarding audit
//! evidence; the failure is counted and logged so the outage is visible on
//! `/metrics` and not only in the buffer depth.

use tracing::{info, warn};

use super::exporter::SiemExporter;
use crate::control::security::audit::AuditEntry;

/// Result of one webhook delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// No webhook configured, or nothing buffered to send.
    Idle,
    /// The batch was accepted by the webhook; carries the event count.
    Delivered(usize),
    /// The attempt failed; carries the number of events requeued for retry.
    Failed(usize),
}

impl SiemExporter {
    /// Send buffered events to the configured webhook.
    ///
    /// Drains both buffers, POSTs one signed batch, and — on any failure —
    /// puts the batch back so the next attempt retries it.
    pub async fn flush_webhook(&self) -> DeliveryOutcome {
        if !self.has_webhook() {
            return DeliveryOutcome::Idle;
        }

        let audit_events = self.drain_audit();
        let auth_events = self.drain_auth();
        let audit_len = audit_events.len();

        let mut batch = audit_events;
        batch.extend(auth_events);
        if batch.is_empty() {
            return DeliveryOutcome::Idle;
        }
        let count = batch.len();

        // An IP-literal host is never resolved, so only this check refuses it.
        if let Err(e) = self.policy.check_url(&self.config.webhook_url) {
            warn!(error = %e, events = count, "SIEM webhook URL fails the [egress] policy");
            return self.fail(batch, audit_len);
        }

        let (body, signature) = match self.build_webhook_payload(&batch) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(error = %e, events = count, "SIEM webhook payload build failed");
                return self.fail(batch, audit_len);
            }
        };

        let mut req = self
            .client
            .post(&self.config.webhook_url)
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(
                self.config.webhook_timeout_secs,
            ))
            .body(body);

        if !signature.is_empty() {
            req = req.header("X-NodeDB-Signature", &signature);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                self.record_delivered(count);
                info!(events = count, "SIEM webhook delivered");
                DeliveryOutcome::Delivered(count)
            }
            Ok(resp) => {
                warn!(status = %resp.status(), events = count, "SIEM webhook delivery failed");
                self.fail(batch, audit_len)
            }
            Err(e) => {
                warn!(error = %e, events = count, "SIEM webhook request failed");
                self.fail(batch, audit_len)
            }
        }
    }

    /// Count the failure and return the batch to its two source buffers.
    fn fail(&self, mut batch: Vec<AuditEntry>, audit_len: usize) -> DeliveryOutcome {
        let count = batch.len();
        self.record_delivery_failure();
        let auth_part = batch.split_off(audit_len.min(count));
        self.requeue_audit(batch);
        self.requeue_auth(auth_part);
        DeliveryOutcome::Failed(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;
    use crate::control::security::siem::config::SiemConfig;

    fn exporter(config: SiemConfig) -> SiemExporter {
        SiemExporter::strict_for_test(config)
    }

    fn entry(seq: u64) -> AuditEntry {
        AuditEntry {
            seq,
            timestamp_us: 0,
            event: AuditEvent::AuthSuccess,
            tenant_id: None,
            database_id: None,
            auth_user_id: "u1".into(),
            auth_user_name: "alice".into(),
            session_id: "s1".into(),
            source: "10.0.0.1".into(),
            detail: "test".into(),
            prev_hash: String::new(),
        }
    }

    #[tokio::test]
    async fn no_webhook_url_is_idle_and_keeps_the_buffer() {
        let exporter = exporter(SiemConfig {
            destinations: vec!["splunk".into()],
            ..Default::default()
        });
        exporter.push_audit(entry(1));
        assert_eq!(exporter.flush_webhook().await, DeliveryOutcome::Idle);
        assert_eq!(exporter.buffered_count(), 1);
    }

    #[tokio::test]
    async fn empty_buffers_are_idle() {
        let exporter = exporter(SiemConfig {
            destinations: vec!["webhook".into()],
            webhook_url: "http://127.0.0.1:1/ingest".into(),
            ..Default::default()
        });
        assert_eq!(exporter.flush_webhook().await, DeliveryOutcome::Idle);
    }

    /// No local HTTP sink is available in unit tests, so the success path is
    /// covered by payload-construction tests; here the reachable behaviour is
    /// the failure path. The strict egress policy refuses the IP-literal URL,
    /// and the refusal must requeue every event and count the failure rather
    /// than silently discarding the batch.
    #[tokio::test]
    async fn failed_delivery_requeues_both_buffers() {
        let exporter = exporter(SiemConfig {
            destinations: vec!["webhook".into()],
            webhook_url: "http://127.0.0.1:1/ingest".into(),
            webhook_timeout_secs: 2,
            ..Default::default()
        });
        exporter.push_audit(entry(1));
        exporter.push_audit(entry(2));
        exporter.push_auth(entry(3));

        assert_eq!(exporter.flush_webhook().await, DeliveryOutcome::Failed(3));
        assert_eq!(exporter.delivery_failures(), 1);
        assert_eq!(exporter.delivered_events(), 0);
        assert_eq!(exporter.buffered_count(), 3, "batch must be retryable");
        assert_eq!(exporter.dropped_audit_events(), 0);
        assert_eq!(exporter.dropped_auth_events(), 0);

        let audit: Vec<u64> = exporter.drain_audit().iter().map(|e| e.seq).collect();
        let auth: Vec<u64> = exporter.drain_auth().iter().map(|e| e.seq).collect();
        assert_eq!(audit, vec![1, 2], "audit events return to the audit buffer");
        assert_eq!(auth, vec![3], "auth events return to the auth buffer");
    }
}

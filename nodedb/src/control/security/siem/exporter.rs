// SPDX-License-Identifier: BUSL-1.1

//! Buffering and signed-payload construction for the SIEM export adapter.
//!
//! Every audit entry recorded through `SharedState::audit_record*` is offered
//! to this exporter. Recording sits on the request path, so the push side is
//! deliberately cheap: an unconfigured exporter is rejected by
//! [`SiemExporter::is_configured`] before the caller clones anything, and a
//! configured one takes a single uncontended write lock and pushes.
//!
//! Both buffers are hard-bounded by `buffer_size`. When a buffer is full the
//! oldest entry is evicted so the newest security event is never the one lost,
//! and the eviction is counted — an unexported audit event is a compliance
//! event, so the count is surfaced on `/metrics` rather than left implicit.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::config::SiemConfig;
use crate::control::security::audit::AuditEntry;
use crate::control::security::egress::{EgressPolicy, outbound_client};

/// SIEM export adapter: buffers events for CDC streaming and webhook delivery.
pub struct SiemExporter {
    pub(super) config: SiemConfig,
    /// Shared HTTP client — constructed once at startup and reused across
    /// every webhook flush so we don't rebuild the connection pool and
    /// TLS session cache per call. It is the egress client built on `policy`.
    pub(super) client: Arc<reqwest::Client>,
    /// The `[egress]` policy the webhook URL passes before every flush.
    pub(super) policy: Arc<EgressPolicy>,
    /// Buffered audit events for CDC consumers.
    audit_buffer: RwLock<VecDeque<AuditEntry>>,
    /// Buffered auth events for CDC consumers.
    auth_buffer: RwLock<VecDeque<AuditEntry>>,
    /// Audit events evicted without being exported (buffer full).
    dropped_audit: AtomicU64,
    /// Auth events evicted without being exported (buffer full).
    dropped_auth: AtomicU64,
    /// Events successfully delivered to the webhook.
    delivered: AtomicU64,
    /// Webhook delivery attempts that failed (events were requeued).
    delivery_failures: AtomicU64,
}

impl SiemExporter {
    /// An exporter with its own egress client built on `policy`.
    pub fn new(config: SiemConfig, policy: Arc<EgressPolicy>) -> Result<Self, reqwest::Error> {
        let client = Arc::new(outbound_client(Arc::clone(&policy))?);
        Ok(Self::with_client(config, client, policy))
    }

    /// Construct with an existing shared HTTP client, the egress client
    /// built on `policy`.
    pub fn with_client(
        config: SiemConfig,
        client: Arc<reqwest::Client>,
        policy: Arc<EgressPolicy>,
    ) -> Self {
        let cap = config.buffer_size;
        Self {
            config,
            client,
            policy,
            audit_buffer: RwLock::new(VecDeque::with_capacity(cap.min(10_000))),
            auth_buffer: RwLock::new(VecDeque::with_capacity(cap.min(10_000))),
            dropped_audit: AtomicU64::new(0),
            dropped_auth: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            delivery_failures: AtomicU64::new(0),
        }
    }

    /// An exporter under the strict egress policy, for tests.
    #[cfg(test)]
    pub(crate) fn strict_for_test(config: SiemConfig) -> Self {
        Self::new(config, Arc::new(EgressPolicy::strict())).expect("exporter")
    }

    /// Push an audit event to the export buffer.
    pub fn push_audit(&self, entry: AuditEntry) {
        self.push_bounded(&self.audit_buffer, &self.dropped_audit, entry);
    }

    /// Push an auth event to the export buffer.
    pub fn push_auth(&self, entry: AuditEntry) {
        self.push_bounded(&self.auth_buffer, &self.dropped_auth, entry);
    }

    /// Append to a bounded buffer, evicting (and counting) the oldest entry
    /// when the configured ceiling is reached.
    fn push_bounded(
        &self,
        buffer: &RwLock<VecDeque<AuditEntry>>,
        dropped: &AtomicU64,
        entry: AuditEntry,
    ) {
        let cap = self.config.buffer_size;
        if cap == 0 {
            // A zero ceiling means "buffer nothing"; count the entry as
            // dropped rather than growing past the operator's limit.
            dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut buf = buffer.write().unwrap_or_else(|p| p.into_inner());
        while buf.len() >= cap {
            buf.pop_front();
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        buf.push_back(entry);
    }

    /// Drain audit events for CDC consumption (Splunk, Datadog, etc.).
    pub fn drain_audit(&self) -> Vec<AuditEntry> {
        let mut buf = self.audit_buffer.write().unwrap_or_else(|p| p.into_inner());
        buf.drain(..).collect()
    }

    /// Drain auth events for CDC consumption.
    pub fn drain_auth(&self) -> Vec<AuditEntry> {
        let mut buf = self.auth_buffer.write().unwrap_or_else(|p| p.into_inner());
        buf.drain(..).collect()
    }

    /// Return drained audit events to the head of the buffer after a failed
    /// delivery, so a webhook outage retries instead of discarding evidence.
    pub(super) fn requeue_audit(&self, events: Vec<AuditEntry>) {
        self.requeue_front(&self.audit_buffer, &self.dropped_audit, events);
    }

    /// Return drained auth events to the head of the buffer after a failed
    /// delivery.
    pub(super) fn requeue_auth(&self, events: Vec<AuditEntry>) {
        self.requeue_front(&self.auth_buffer, &self.dropped_auth, events);
    }

    /// Re-insert `events` (in original order) ahead of whatever accumulated
    /// while the delivery was in flight, still respecting `buffer_size`.
    /// Overflow drops the oldest requeued entries and counts them.
    fn requeue_front(
        &self,
        buffer: &RwLock<VecDeque<AuditEntry>>,
        dropped: &AtomicU64,
        events: Vec<AuditEntry>,
    ) {
        if events.is_empty() {
            return;
        }
        let cap = self.config.buffer_size;
        if cap == 0 {
            dropped.fetch_add(events.len() as u64, Ordering::Relaxed);
            return;
        }
        let mut buf = buffer.write().unwrap_or_else(|p| p.into_inner());
        let room = cap.saturating_sub(buf.len());
        let skipped = events.len().saturating_sub(room);
        if skipped > 0 {
            dropped.fetch_add(skipped as u64, Ordering::Relaxed);
        }
        for entry in events.into_iter().skip(skipped).rev() {
            buf.push_front(entry);
        }
    }

    /// Build a webhook payload with HMAC signature.
    ///
    /// Returns `(json_body, hmac_signature_hex)`; the signature is empty when
    /// no HMAC secret is configured.
    pub fn build_webhook_payload(&self, events: &[AuditEntry]) -> crate::Result<(String, String)> {
        let payload = serde_json::json!({
            "source": "nodedb",
            "event_count": events.len(),
            "events": events,
        });
        let body = sonic_rs::to_string(&payload).map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("SIEM webhook payload serialization failed: {e}"),
        })?;

        let signature = if self.config.webhook_hmac_secret.is_empty() {
            String::new()
        } else {
            compute_hmac(&self.config.webhook_hmac_secret, &body)
        };

        Ok((body, signature))
    }

    /// Whether any export destinations are configured.
    pub fn is_configured(&self) -> bool {
        !self.config.destinations.is_empty() || !self.config.webhook_url.is_empty()
    }

    /// Whether a webhook destination URL is set. Destinations without a URL
    /// (e.g. a CDC-drained "splunk" entry) buffer but never POST.
    pub fn has_webhook(&self) -> bool {
        !self.config.webhook_url.is_empty()
    }

    /// Number of buffered events (audit + auth).
    pub fn buffered_count(&self) -> usize {
        let a = self
            .audit_buffer
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        let b = self
            .auth_buffer
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        a + b
    }

    /// Configured per-buffer ceiling.
    pub fn buffer_capacity(&self) -> usize {
        self.config.buffer_size
    }

    /// Interval between delivery attempts after a successful/idle flush.
    pub fn flush_interval_secs(&self) -> u64 {
        self.config.flush_interval_secs
    }

    /// Ceiling for the retry backoff after a failed delivery.
    pub fn max_backoff_secs(&self) -> u64 {
        self.config.max_backoff_secs
    }

    /// Audit events evicted without being exported.
    pub fn dropped_audit_events(&self) -> u64 {
        self.dropped_audit.load(Ordering::Relaxed)
    }

    /// Auth events evicted without being exported.
    pub fn dropped_auth_events(&self) -> u64 {
        self.dropped_auth.load(Ordering::Relaxed)
    }

    /// Events successfully delivered to the webhook.
    pub fn delivered_events(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Failed webhook delivery attempts.
    pub fn delivery_failures(&self) -> u64 {
        self.delivery_failures.load(Ordering::Relaxed)
    }

    pub(super) fn record_delivered(&self, count: usize) {
        self.delivered.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(super) fn record_delivery_failure(&self) {
        self.delivery_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// Compute HMAC-SHA256 signature for webhook payload.
pub(super) fn compute_hmac(secret: &str, message: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return String::new();
    };
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;

    fn test_entry(seq: u64) -> AuditEntry {
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

    fn webhook_config(buffer_size: usize) -> SiemConfig {
        SiemConfig {
            destinations: vec!["webhook".into()],
            webhook_url: "https://siem.invalid/ingest".into(),
            buffer_size,
            ..Default::default()
        }
    }

    #[test]
    fn buffer_and_drain() {
        let exporter = SiemExporter::strict_for_test(SiemConfig::default());
        exporter.push_audit(test_entry(1));
        exporter.push_audit(test_entry(2));
        exporter.push_auth(test_entry(3));

        assert_eq!(exporter.buffered_count(), 3);

        let audit = exporter.drain_audit();
        assert_eq!(audit.len(), 2);
        assert_eq!(exporter.buffered_count(), 1); // auth still buffered.
    }

    #[test]
    fn push_past_buffer_size_is_bounded_and_counted() {
        let exporter = SiemExporter::strict_for_test(webhook_config(3));
        for seq in 0..10 {
            exporter.push_audit(test_entry(seq));
        }
        assert_eq!(
            exporter.buffered_count(),
            3,
            "buffer must not grow past cap"
        );
        assert_eq!(exporter.dropped_audit_events(), 7);
        assert_eq!(exporter.dropped_auth_events(), 0);

        // Eviction is oldest-first: the newest events survive.
        let remaining: Vec<u64> = exporter.drain_audit().iter().map(|e| e.seq).collect();
        assert_eq!(remaining, vec![7, 8, 9]);
    }

    #[test]
    fn zero_capacity_buffers_nothing_and_counts_every_event() {
        let exporter = SiemExporter::strict_for_test(webhook_config(0));
        exporter.push_audit(test_entry(1));
        exporter.push_auth(test_entry(2));
        assert_eq!(exporter.buffered_count(), 0);
        assert_eq!(exporter.dropped_audit_events(), 1);
        assert_eq!(exporter.dropped_auth_events(), 1);
    }

    #[test]
    fn requeue_restores_order_and_respects_the_ceiling() {
        let exporter = SiemExporter::strict_for_test(webhook_config(3));
        exporter.push_audit(test_entry(10));
        // Two in-flight events come back ahead of the one that arrived after.
        exporter.requeue_audit(vec![test_entry(1), test_entry(2)]);
        let order: Vec<u64> = exporter.drain_audit().iter().map(|e| e.seq).collect();
        assert_eq!(order, vec![1, 2, 10]);
        assert_eq!(exporter.dropped_audit_events(), 0);

        // Requeueing more than fits drops the oldest requeued entries.
        exporter.push_audit(test_entry(20));
        exporter.push_audit(test_entry(21));
        exporter.requeue_audit(vec![test_entry(1), test_entry(2), test_entry(3)]);
        let order: Vec<u64> = exporter.drain_audit().iter().map(|e| e.seq).collect();
        assert_eq!(order, vec![3, 20, 21]);
        assert_eq!(exporter.dropped_audit_events(), 2);
    }

    #[test]
    fn webhook_payload_with_hmac() {
        let config = SiemConfig {
            webhook_hmac_secret: "test_secret".into(),
            ..Default::default()
        };
        let exporter = SiemExporter::strict_for_test(config);

        let (body, signature) = exporter
            .build_webhook_payload(&[test_entry(1)])
            .expect("payload builds");
        assert!(body.contains("nodedb"));
        assert!(!signature.is_empty());
        assert_eq!(signature.len(), 64); // SHA-256 hex = 64 chars.
        assert_eq!(signature, compute_hmac("test_secret", &body));
    }

    #[test]
    fn webhook_payload_carries_the_whole_drained_batch() {
        let exporter = SiemExporter::strict_for_test(webhook_config(10));
        let batch = vec![test_entry(1), test_entry(2), test_entry(3)];
        let (body, signature) = exporter
            .build_webhook_payload(&batch)
            .expect("payload builds");
        assert!(body.contains("\"event_count\":3"));
        assert!(signature.is_empty(), "no secret configured → no signature");
    }

    #[test]
    fn hmac_consistency() {
        let sig1 = compute_hmac("secret", "hello");
        let sig2 = compute_hmac("secret", "hello");
        assert_eq!(sig1, sig2);

        let sig3 = compute_hmac("secret", "world");
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn default_exporter_is_not_configured() {
        assert!(!SiemExporter::strict_for_test(SiemConfig::default()).is_configured());
        assert!(SiemExporter::strict_for_test(webhook_config(1)).is_configured());
    }
}

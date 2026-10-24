// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: outbound HTTP emitters (alert webhooks, SIEM
//! webhooks, OTEL exporter) must reuse one `reqwest::Client` rather than
//! constructing a fresh one per call.
//!
//! Every `reqwest::Client::builder().build()` allocates a connection pool,
//! DNS resolver, rustls config, and TLS session cache. Under burst, that
//! means a SYN flood and a CPU pegged by TLS handshakes — the pooled
//! connection is discarded right after use.
//!
//! The webhook-delivery task at `event/webhook/delivery.rs` already holds
//! one client per stream. The fix extends that pattern: a single
//! `Arc<reqwest::Client>` lives on `SharedState` and is handed to every
//! emitter. This file pins the public shape of that contract.

use std::sync::Arc;

use nodedb::control::state::SharedState;

/// The fix must expose a shared HTTP client on `SharedState`. The exact
/// accessor name is left to the fix author — this test gates on a public
/// `http_client()` returning `&Arc<reqwest::Client>` because that's the
/// minimum shape every emitter needs (clone into its own task, reuse pool).
#[test]
fn shared_state_exposes_shared_reqwest_client() {
    fn assert_shape(state: &SharedState) {
        let _client: &Arc<reqwest::Client> = state.http_client();
    }
    // The function body compiles only when the API exists; the test itself
    // does not need to construct a SharedState (which is heavy) — the
    // compile check is the regression guard.
    let _ = assert_shape;
}

/// Alert webhook dispatch must accept a shared client rather than building
/// a fresh one per call. Locks in the function shape.
#[test]
fn alert_webhook_accepts_shared_client() {
    use nodedb::event::alert::notify::notify_webhook_with_client;
    use nodedb::event::alert::types::AlertEvent;

    fn assert_shape(
        client: &reqwest::Client,
        policy: &nodedb::control::security::egress::EgressPolicy,
        url: &str,
        event: &AlertEvent,
    ) {
        // The fix renames `notify_webhook` → `notify_webhook_with_client` so
        // the shared client is threaded through; the per-request timeout is
        // explicit and tuned from `SchedulerTuning::webhook_timeout_secs`.
        // The URL is user-supplied, so the `[egress]` policy travels with it.
        let _fut = notify_webhook_with_client(
            client,
            policy,
            url,
            event,
            std::time::Duration::from_secs(5),
        );
    }
    let _ = assert_shape;
}

/// SIEM exporter must accept a shared client (held on `SiemExporter` at
/// construction, or passed to `flush_webhook`).
#[test]
fn siem_exporter_holds_shared_client() {
    use nodedb::control::security::siem::{SiemConfig, SiemExporter};

    fn assert_shape(
        client: Arc<reqwest::Client>,
        config: SiemConfig,
        policy: Arc<nodedb::control::security::egress::EgressPolicy>,
    ) {
        let _exporter = SiemExporter::with_client(config, client, policy);
    }
    let _ = assert_shape;
}

/// OTEL trace exporter must reuse the shared client rather than calling
/// `reqwest::Client::new()` inline on every span export.
#[test]
fn otel_exporter_reuses_shared_client() {
    use nodedb::control::otel::exporter::export_trace_with_client;

    fn assert_shape(client: &reqwest::Client) {
        let _fut = export_trace_with_client(
            client,
            std::time::Duration::from_secs(5),
            "https://otel.example/v1/traces",
            &[],
        );
    }
    let _ = assert_shape;
}

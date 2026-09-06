// SPDX-License-Identifier: BUSL-1.1

//! Observability configuration: PromQL, OTLP receiver, OTLP export.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Top-level observability configuration.
///
/// ```toml
/// [observability.promql]
/// enabled = true
///
/// [observability.otlp.receiver]
/// enabled = true
/// http_listen = "0.0.0.0:4318"
/// grpc_listen = "0.0.0.0:4317"
///
/// [observability.otlp.export]
/// enabled = false
/// endpoint = "http://localhost:4318"
/// metrics_interval_secs = 15
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// PromQL engine and `/obsv/api/v1/*` endpoints.
    #[serde(default)]
    pub promql: PromqlConfig,

    /// OpenTelemetry Protocol configuration.
    #[serde(default)]
    pub otlp: OtlpConfig,

    /// Master gate for the `/cluster/debug/*` HTTP endpoints. Disabled
    /// by default — operators must opt in per deployment because the
    /// endpoints expose raft internals, transport connection caches,
    /// descriptor leases, and the full metadata cache. Even when
    /// enabled the handlers still require a superuser identity. (J.5)
    #[serde(default)]
    pub debug_endpoints_enabled: bool,
}

/// PromQL engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromqlConfig {
    /// Enable PromQL endpoints (`/obsv/api/v1/*`).
    /// Requires the `promql` cargo feature at compile time.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for PromqlConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// OpenTelemetry Protocol configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtlpConfig {
    /// OTLP receiver (ingest from external services).
    #[serde(default)]
    pub receiver: OtlpReceiverConfig,

    /// OTLP export (push NodeDB's own telemetry to a collector).
    #[serde(default)]
    pub export: OtlpExportConfig,
}

/// OTLP receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpReceiverConfig {
    /// Enable OTLP ingest endpoints.
    /// Requires the `otel` cargo feature at compile time.
    #[serde(default)]
    pub enabled: bool,

    /// OTLP/HTTP listen address (default: 0.0.0.0:4318).
    #[serde(default = "default_otlp_http_listen")]
    pub http_listen: SocketAddr,

    /// OTLP/gRPC listen address (default: 0.0.0.0:4317).
    #[serde(default = "default_otlp_grpc_listen")]
    pub grpc_listen: SocketAddr,
}

impl Default for OtlpReceiverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            http_listen: default_otlp_http_listen(),
            grpc_listen: default_otlp_grpc_listen(),
        }
    }
}

/// OTLP export configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpExportConfig {
    /// Enable OTLP export of NodeDB's own metrics/traces.
    #[serde(default)]
    pub enabled: bool,

    /// OTLP collector endpoint (e.g., "http://localhost:4318").
    #[serde(default)]
    pub endpoint: String,

    /// Metrics push interval in seconds (default: 15).
    #[serde(default = "default_metrics_interval")]
    pub metrics_interval_secs: u64,
}

impl Default for OtlpExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            metrics_interval_secs: 15,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_otlp_http_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 4318))
}

/// Validate observability configuration.
///
/// All observability features (PromQL, OTLP) are always compiled into the
/// binary and toggled at runtime via `nodedb.toml`. This function is a
/// no-op validation pass retained for call-site compatibility.
pub fn validate_feature_availability(_config: &ObservabilityConfig) -> crate::Result<()> {
    Ok(())
}

fn default_otlp_grpc_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 4317))
}

fn default_metrics_interval() -> u64 {
    15
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = ObservabilityConfig::default();
        assert!(cfg.promql.enabled);
        assert!(!cfg.otlp.receiver.enabled);
        assert!(!cfg.otlp.export.enabled);
        assert_eq!(cfg.otlp.receiver.http_listen.port(), 4318);
        assert_eq!(cfg.otlp.receiver.grpc_listen.port(), 4317);
        assert_eq!(cfg.otlp.export.metrics_interval_secs, 15);
    }

    #[test]
    fn validate_default_config() {
        let cfg = ObservabilityConfig::default();
        assert!(validate_feature_availability(&cfg).is_ok());
    }

    #[test]
    fn validate_disabled_always_passes() {
        let mut cfg = ObservabilityConfig::default();
        cfg.promql.enabled = false;
        cfg.otlp.receiver.enabled = false;
        cfg.otlp.export.enabled = false;
        assert!(validate_feature_availability(&cfg).is_ok());
    }

    #[test]
    fn toml_roundtrip() {
        let cfg = ObservabilityConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).unwrap();
        let _parsed: ObservabilityConfig = toml::from_str(&toml_str).unwrap();
    }
}

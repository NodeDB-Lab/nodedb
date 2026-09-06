// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_PROMQL_ENABLED` / `NODEDB_OTLP_*` / `NODEDB_DEBUG_ENDPOINTS_ENABLED`
//! overrides.

use crate::config::server::ServerConfig;

use super::super::parse::{parse_bool_lenient, parse_socket_addr, parse_u64_positive};
use super::super::table::EnvRow;
use crate::config::server::domain::otlp_endpoint_has_host;

fn apply_promql_enabled(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.promql.enabled = parse_bool_lenient(raw)?;
    Ok(())
}

fn apply_otlp_receiver_enabled(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.otlp.receiver.enabled = parse_bool_lenient(raw)?;
    Ok(())
}

fn apply_otlp_http_listen(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.otlp.receiver.http_listen =
        parse_socket_addr(raw, "a socket address such as 0.0.0.0:4318")?;
    Ok(())
}

fn apply_otlp_grpc_listen(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.otlp.receiver.grpc_listen =
        parse_socket_addr(raw, "a socket address such as 0.0.0.0:4317")?;
    Ok(())
}

fn apply_otlp_export_enabled(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.otlp.export.enabled = parse_bool_lenient(raw)?;
    Ok(())
}

/// Must carry a scheme and a non-empty host: `http://collector` is the
/// shortest value that can actually be dialed.
fn apply_otlp_export_endpoint(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    if !otlp_endpoint_has_host(raw) {
        return Err("an http:// or https:// endpoint URL");
    }
    config.observability.otlp.export.endpoint = raw.to_string();
    Ok(())
}

fn apply_otlp_export_interval(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.otlp.export.metrics_interval_secs = parse_u64_positive(raw)?;
    Ok(())
}

fn apply_debug_endpoints_enabled(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.observability.debug_endpoints_enabled = parse_bool_lenient(raw)?;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_PROMQL_ENABLED",
        apply: apply_promql_enabled,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_RECEIVER_ENABLED",
        apply: apply_otlp_receiver_enabled,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_HTTP_LISTEN",
        apply: apply_otlp_http_listen,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_GRPC_LISTEN",
        apply: apply_otlp_grpc_listen,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_EXPORT_ENABLED",
        apply: apply_otlp_export_enabled,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_EXPORT_ENDPOINT",
        apply: apply_otlp_export_endpoint,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_OTLP_EXPORT_INTERVAL",
        apply: apply_otlp_export_interval,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_DEBUG_ENDPOINTS_ENABLED",
        apply: apply_debug_endpoints_enabled,
        redact: false,
    },
];

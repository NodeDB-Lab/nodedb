// SPDX-License-Identifier: BUSL-1.1

//! Domain constraints on config values, checked whatever set them.
//!
//! The environment gate rejects an out-of-domain override and names the
//! variable. A TOML file reaches the same fields without passing that gate, so
//! [`validate_domain`] re-checks every constrained field on the loaded config.
//! Both paths read the bounds below, so neither can drift from the other.

use super::ServerConfig;

/// Smallest WAL write buffer the writer accepts.
pub(super) const MIN_WAL_WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// Smallest scope expiry sweep interval.
///
/// `ScopeGrant::is_effective` already enforces expiry on every read, so a
/// shorter sweep costs more than the resolution it buys.
pub(super) const MIN_SCOPE_EXPIRY_SECS: u64 = 10;

/// Rejects an endpoint that carries no `http://` or `https://` host.
pub(super) fn otlp_endpoint_has_host(raw: &str) -> bool {
    raw.strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
        .is_some_and(|host| !host.is_empty())
}

fn reject(field: &str, value: impl std::fmt::Display, expected: &str) -> crate::Error {
    crate::Error::Config {
        detail: format!("invalid value '{value}' for {field}: expected {expected}"),
    }
}

fn positive_u64(value: u64, field: &str) -> crate::Result<()> {
    if value == 0 {
        return Err(reject(field, value, "a positive integer"));
    }
    Ok(())
}

fn positive_usize(value: usize, field: &str) -> crate::Result<()> {
    if value == 0 {
        return Err(reject(field, value, "a positive integer"));
    }
    Ok(())
}

/// Checks every field the environment gate constrains, on the loaded config.
///
/// A value set in TOML reaches the same field the gate guards. Skipping this
/// leaves the bound enforced on one of the two paths.
pub(super) fn validate_domain(config: &ServerConfig) -> crate::Result<()> {
    positive_usize(config.server.data_plane_cores, "server.data_plane_cores")?;

    if config.tuning.wal.write_buffer_size < MIN_WAL_WRITE_BUFFER_BYTES {
        return Err(reject(
            "tuning.wal.write_buffer_size",
            config.tuning.wal.write_buffer_size,
            "a size of at least 64KiB",
        ));
    }

    positive_u64(config.checkpoint.interval_secs, "checkpoint.interval_secs")?;
    positive_u64(
        config.checkpoint.wal_segment_target_mb,
        "checkpoint.wal_segment_target_mb",
    )?;

    let ts = &config.tuning.timeseries;
    positive_usize(
        ts.memtable_budget_bytes,
        "tuning.timeseries.memtable_budget_bytes",
    )?;
    positive_usize(
        ts.memtable_hard_limit_bytes,
        "tuning.timeseries.memtable_hard_limit_bytes",
    )?;
    positive_u64(
        u64::from(ts.max_tag_cardinality),
        "tuning.timeseries.max_tag_cardinality",
    )?;

    let m = &config.tuning.maintenance;
    positive_u64(
        m.clone_sweep_interval_ms,
        "tuning.maintenance.clone_sweep_interval_ms",
    )?;
    positive_u64(
        m.constraint_reconcile_interval_ms,
        "tuning.maintenance.constraint_reconcile_interval_ms",
    )?;
    if m.scope_expiry_interval_secs < MIN_SCOPE_EXPIRY_SECS {
        return Err(reject(
            "tuning.maintenance.scope_expiry_interval_secs",
            m.scope_expiry_interval_secs,
            "an interval of at least 10 seconds",
        ));
    }

    if let Some(cluster) = config.cluster.as_ref() {
        positive_u64(
            u64::from(cluster.join_retry_max_attempts),
            "cluster.join_retry_max_attempts",
        )?;
        positive_u64(
            cluster.join_retry_max_backoff_secs,
            "cluster.join_retry_max_backoff_secs",
        )?;
    }

    let export = &config.observability.otlp.export;
    positive_u64(
        export.metrics_interval_secs,
        "observability.otlp.export.metrics_interval_secs",
    )?;
    if export.enabled && !otlp_endpoint_has_host(&export.endpoint) {
        return Err(reject(
            "observability.otlp.export.endpoint",
            &export.endpoint,
            "an http:// or https:// endpoint URL",
        ));
    }

    Ok(())
}

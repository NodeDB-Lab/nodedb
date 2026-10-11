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

/// Smallest Calvin verdict stall warning interval. The scheduler's idle sweep
/// ticks at a quarter of it, and a zero-length tick panics the timer.
pub(super) const MIN_CALVIN_VERDICT_STALL_WARN_MS: u64 = 4;

/// Smallest redo entry size. A chunk entry carries its stream header beside
/// its bytes, so a smaller entry leaves too little room for the bytes.
pub(super) const MIN_REDO_ENTRY_BYTES: usize = 64 * 1024;

/// Room a redo entry leaves in one RPC frame for the frame header, the
/// AppendEntries envelope, and the entry's own routing fields.
pub(super) const REDO_ENTRY_ENVELOPE_BYTES: usize = 1024 * 1024;

/// Largest redo entry size: one entry and its envelope fit one RPC frame.
pub(super) const MAX_REDO_ENTRY_BYTES: usize =
    nodedb_cluster::rpc_codec::MAX_RPC_PAYLOAD_SIZE as usize - REDO_ENTRY_ENVELOPE_BYTES;

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

pub(super) fn positive_u64(value: u64, field: &str) -> crate::Result<()> {
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
    positive_u64(
        config.checkpoint.wal_archive_interval_secs,
        "checkpoint.wal_archive_interval_secs",
    )?;
    super::pitr::validate_pitr(config)?;
    super::backup::validate_backup(config)?;

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

    validate_calvin(config)?;

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

/// Checks `[tuning.calvin]`. A zero channel capacity panics the channel
/// constructor, and a zero bound stalls every scheduler.
fn validate_calvin(config: &ServerConfig) -> crate::Result<()> {
    let calvin = &config.tuning.calvin;
    positive_usize(calvin.channel_capacity, "tuning.calvin.channel_capacity")?;
    positive_u64(
        u64::from(calvin.txn_deadline_multiplier),
        "tuning.calvin.txn_deadline_multiplier",
    )?;
    positive_u64(
        calvin.dependent_read_passive_timeout_ms,
        "tuning.calvin.dependent_read_passive_timeout_ms",
    )?;
    if calvin.verdict_stall_warn_ms < MIN_CALVIN_VERDICT_STALL_WARN_MS {
        return Err(reject(
            "tuning.calvin.verdict_stall_warn_ms",
            calvin.verdict_stall_warn_ms,
            "an interval of at least 4 milliseconds",
        ));
    }
    positive_usize(
        calvin.max_inflight_backlog,
        "tuning.calvin.max_inflight_backlog",
    )?;
    positive_u64(calvin.catch_up_window, "tuning.calvin.catch_up_window")?;
    positive_u64(
        u64::from(calvin.restage_attempts),
        "tuning.calvin.restage_attempts",
    )?;
    positive_u64(
        calvin.restage_backoff_ms,
        "tuning.calvin.restage_backoff_ms",
    )?;
    validate_redo_sizes(calvin)
}

/// Checks the redo entry size against one RPC frame, and the open-stream
/// cap against one entry.
fn validate_redo_sizes(calvin: &nodedb_types::config::tuning::CalvinTuning) -> crate::Result<()> {
    let entry = calvin.max_redo_entry_bytes;
    if !(MIN_REDO_ENTRY_BYTES..=MAX_REDO_ENTRY_BYTES).contains(&entry) {
        return Err(reject(
            "tuning.calvin.max_redo_entry_bytes",
            entry,
            &format!(
                "a size of at least {MIN_REDO_ENTRY_BYTES} bytes and at most \
                 {MAX_REDO_ENTRY_BYTES} bytes, the RPC frame limit less its envelope"
            ),
        ));
    }
    if calvin.max_open_redo_bytes < entry as u64 {
        return Err(reject(
            "tuning.calvin.max_open_redo_bytes",
            calvin.max_open_redo_bytes,
            "a size of at least tuning.calvin.max_redo_entry_bytes",
        ));
    }
    Ok(())
}

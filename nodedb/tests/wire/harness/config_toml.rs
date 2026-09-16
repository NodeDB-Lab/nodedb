// SPDX-License-Identifier: BUSL-1.1

//! Writes the TOML config file the spawned server reads via `NODEDB_CONFIG`.
//!
//! pgwire authentication mode has no environment-variable override — it is
//! TOML only (`[auth] mode = "..."`). `ServerConfig` derives `[auth]` from
//! `#[serde(default)]` only when the `[auth]` table is absent entirely; once
//! the table is written, every one of its fields that lacks its own
//! `#[serde(default = ...)]` must be present or parsing fails. This writes
//! the full required set every time, never a bare `mode` line.

use std::path::{Path, PathBuf};

/// Fixed 32-byte backup KEK written to every spawned test server's key
/// file. Test cases that hand-craft an encrypted envelope (rather than
/// going through `BACKUP TENANT`) must encrypt with this exact key or the
/// server rejects it as `WrongBackupKek` before reaching the check under
/// test.
pub(crate) const TEST_BACKUP_KEK: [u8; 32] = [0x42u8; 32];

/// Which pgwire authentication mode the spawned server boots into.
#[derive(Clone, Copy)]
pub(super) enum AuthMode {
    /// No authentication, lockout disabled — `TestServer::start`'s default.
    Trust,
    /// SCRAM-SHA-256 with a lockout policy of 5 failures / 300s, matching
    /// `TestServer::start_password`.
    Password,
}

/// `[tuning]` overrides a test asks the spawned server to boot with.
///
/// Each field is `None` for the shipped default. One struct keeps the spawn
/// path from growing a positional argument per knob.
#[derive(Clone, Copy, Default)]
pub(super) struct TuningOverrides {
    /// Overrides `[tuning.query] columnar_flush_threshold` so a test can
    /// observe segment flushes without inserting 65k rows.
    pub(super) columnar_flush_threshold: Option<usize>,
    /// Overrides `[tuning.maintenance] auto_analyze_min_mutations` so a test
    /// can trip auto-ANALYZE without issuing 1000 writes.
    pub(super) auto_analyze_min_mutations: Option<u64>,
    /// Overrides `[tuning.query] stream_chunk_size` so a test can drive the
    /// chunked-streaming scan path without seeding 1000 rows.
    pub(super) stream_chunk_size: Option<usize>,
    /// Overrides `[tuning.timeseries] memtable_budget_bytes` so a test can
    /// observe timeseries partition flushes on a handful of rows.
    pub(super) timeseries_memtable_budget_bytes: Option<usize>,
}

impl TuningOverrides {
    /// Boot with every tuning knob at its shipped default.
    pub(super) fn none() -> Self {
        Self::default()
    }

    /// Boot with a lowered columnar flush threshold.
    pub(super) fn columnar_flush(threshold: usize) -> Self {
        Self {
            columnar_flush_threshold: Some(threshold),
            ..Self::default()
        }
    }

    /// Boot with a lowered auto-ANALYZE mutation floor.
    pub(super) fn auto_analyze(min_mutations: u64) -> Self {
        Self {
            auto_analyze_min_mutations: Some(min_mutations),
            ..Self::default()
        }
    }

    /// Boot with a lowered scan streaming chunk size.
    pub(super) fn stream_chunk(rows: usize) -> Self {
        Self {
            stream_chunk_size: Some(rows),
            ..Self::default()
        }
    }

    /// Boot with a lowered timeseries memtable budget.
    pub(super) fn timeseries_memtable_budget(bytes: usize) -> Self {
        Self {
            timeseries_memtable_budget_bytes: Some(bytes),
            ..Self::default()
        }
    }
}

/// Write `nodedb.toml` into `dir` and return its path.
pub(super) fn write_config(dir: &Path, auth_mode: AuthMode, tuning: TuningOverrides) -> PathBuf {
    let (mode, max_failed_logins, lockout_duration_secs) = match auth_mode {
        AuthMode::Trust => ("trust", 0u32, 0u64),
        AuthMode::Password => ("password", 5u32, 300u64),
    };
    let mut toml = format!(
        "[auth]\n\
         mode = \"{mode}\"\n\
         superuser_name = \"nodedb\"\n\
         min_password_length = 8\n\
         max_failed_logins = {max_failed_logins}\n\
         lockout_duration_secs = {lockout_duration_secs}\n\
         idle_timeout_secs = 0\n\
         max_connections_per_user = 0\n\
         password_expiry_days = 0\n\
         audit_retention_days = 0\n"
    );
    // A test's own client connections are still open at shutdown. The
    // production drain outlives the harness's 20s SIGTERM patience, so the
    // server would be force-killed instead of exiting gracefully.
    toml.push_str("\n[tuning.network]\ndrain_timeout_secs = 2\n");
    if let Some(threshold) = tuning.columnar_flush_threshold {
        toml.push_str(&format!(
            "\n[tuning.query]\ncolumnar_flush_threshold = {threshold}\n"
        ));
    }
    if let Some(min_mutations) = tuning.auto_analyze_min_mutations {
        toml.push_str(&format!(
            "\n[tuning.maintenance]\nauto_analyze_min_mutations = {min_mutations}\n"
        ));
    }
    if let Some(rows) = tuning.stream_chunk_size {
        toml.push_str(&format!("\n[tuning.query]\nstream_chunk_size = {rows}\n"));
    }
    if let Some(bytes) = tuning.timeseries_memtable_budget_bytes {
        toml.push_str(&format!(
            "\n[tuning.timeseries]\nmemtable_budget_bytes = {bytes}\n"
        ));
    }
    toml.push_str(&format!(
        "\n[backup_encryption]\nkey_path = {}\n",
        toml_quote(&write_backup_kek(dir))
    ));
    let path = dir.join("nodedb.toml");
    std::fs::write(&path, toml).expect("write test server config file");
    path
}

/// Write the 32-byte key that wraps each backup's data key.
///
/// The server refuses plaintext backup envelopes, so without this every backup
/// test fails on config before reaching the behaviour under test. Nothing
/// outside this temp dir reads these backups.
fn write_backup_kek(dir: &Path) -> PathBuf {
    let path = dir.join("backup.key");
    std::fs::write(&path, TEST_BACKUP_KEK).expect("write backup key file");
    path
}

/// Render a path as a TOML string. Temp dir names come from the OS, so do not
/// assume they are free of backslashes or quotes.
fn toml_quote(path: &Path) -> String {
    let raw = path.display().to_string();
    let escaped = raw.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

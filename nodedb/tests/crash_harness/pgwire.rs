// SPDX-License-Identifier: BUSL-1.1

//! `CrashHarness` query helpers over pgwire.
//!
//! Split out of `mod.rs` (wiring + process lifecycle only) because this is a
//! distinct concern: running SQL against the harness and shaping the results,
//! as opposed to spawning/killing the child process itself.

use std::time::{Duration, Instant};

use super::CrashHarness;

/// Bounded retry budget for the `Error::RetryableSchemaChanged` condition
/// (rendered over pgwire as `XX000: schema changed during execution
/// (<descriptor>); please retry`).
///
/// The server already retries this condition server-side for ~750ms
/// (`retry_on_schema_change`, `control/server/shared/retry.rs`, 5 attempts,
/// 50/100/200/400ms backoff) before giving up and surfacing it to the
/// client. By the server's own contract, a client that still observes this
/// error is expected to retry the statement — it is not a distinct failure
/// mode, it is the same descriptor-lease-drain race the server's retry loop
/// exists to absorb, just unlucky enough to still be running when the
/// server's budget ran out. A harness that panics on it instead of retrying
/// is a worse client than any real pgwire driver would be. This budget is
/// separate from and on top of the server's own, and is deliberately short:
/// it exists to cover the rare case where the drain outlives the server's
/// window, not to paper over a drain that is genuinely stuck (that still
/// panics loudly once this budget is exhausted).
const SCHEMA_CHANGE_RETRY_ATTEMPTS: usize = 5;
const SCHEMA_CHANGE_RETRY_BACKOFF: Duration = Duration::from_millis(150);

/// Substring of `Error::RetryableSchemaChanged`'s Display text
/// (`#[error("schema changed during execution ({descriptor}); please retry")]`
/// in `nodedb/src/error/types.rs`). The message is the durable signal: a code
/// alone would blanket-retry unrelated internal errors, because the class this
/// condition carries depends on the mapper in front of it.
/// The server was still reporting `RetryableSchemaChanged` when the
/// client-side retry budget ran out.
///
/// Deliberately carries no payload: it exists so a caller with its own longer
/// deadline can tell "the descriptor drain has not settled yet" apart from a
/// real error, and every real error still panics at the call site with the full
/// server log and faultbox report attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryableSchemaChange;

fn is_retryable_schema_change(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .is_some_and(|db| db.message().contains("schema changed during execution"))
}

/// Substring of the `calvin-submit` internal error text
/// (`control/planner/calvin/submit.rs`) surfaced verbatim over pgwire as a
/// generic `XX000` `DbError` — the same text ILP logs and drops the
/// connection on (`control/server/ilp_listener.rs`). See
/// [`CrashHarness::wait_for_calvin_ready`] for why this needs its own probe
/// instead of relying on `simple_query_ready`'s existing retry of the same
/// condition.
fn is_no_sequencer_leader_error(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .is_some_and(|db| db.message().contains("no sequencer leader elected yet"))
}

impl CrashHarness {
    /// Open a fresh pgwire connection, run one statement, and return the
    /// resulting messages. Panics on connect/exec error.
    ///
    /// Retries two distinct transient conditions, each bounded independently
    /// so neither can mask the other:
    ///
    /// - "no sequencer leader elected yet": `/healthz` intentionally reports
    ///   ready before the Calvin sequencer group has elected a leader (the
    ///   sequencer is deliberately not a data group in the readiness gate),
    ///   so a cross-shard write issued in the first moments of uptime can
    ///   race the election and get a clean, retryable error. On a loaded
    ///   machine that window is wide enough to lose. A real client retries;
    ///   so does the harness, bounded, before writing.
    /// - `RetryableSchemaChanged` (see [`is_retryable_schema_change`]).
    async fn simple_query_ready(&self, sql: &str) -> Vec<tokio_postgres::SimpleQueryMessage> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut schema_change_attempts = 0usize;
        loop {
            let (client, connection) =
                tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                    .await
                    .expect("connect for exec");
            let conn_handle = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = client.simple_query(sql).await;
            drop(client);
            let _ = conn_handle.await;
            match result {
                Ok(messages) => return messages,
                Err(e)
                    if Instant::now() < deadline
                        && e.as_db_error().is_some_and(|db| {
                            db.message().contains("no sequencer leader elected yet")
                        }) =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                // Mirrors real client-driver behavior for a condition the
                // server itself documents as retryable — see the constant
                // doc comment above. Bounded separately from the
                // sequencer-election retry above: this is a short-lived
                // schema-change race, not a startup race, and should not
                // inherit that loop's 20s budget.
                Err(e)
                    if is_retryable_schema_change(&e)
                        && schema_change_attempts < SCHEMA_CHANGE_RETRY_ATTEMPTS =>
                {
                    schema_change_attempts += 1;
                    tokio::time::sleep(SCHEMA_CHANGE_RETRY_BACKOFF).await;
                }
                // `tokio_postgres::Error` renders as a bare "db error"; the
                // server's message only lives on the DbError payload. The
                // server log tail comes along because the interesting failures
                // here are server-side (stalled apply, failed recovery) and the
                // harness's tempdir is gone by the time anyone reads the panic
                // (unless `NODEDB_TEST_KEEP_DATA_DIR` says otherwise).
                Err(e) if is_retryable_schema_change(&e) => {
                    let tail = super::diagnostics::log_tail_section(&self.server_log());
                    let reports = super::diagnostics::faultbox_report_section(self.data_dir());
                    panic!(
                        "exec: retryable schema change never cleared within \
                         {SCHEMA_CHANGE_RETRY_ATTEMPTS} attempts: {e}{}{}\n{reports}{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default(),
                        self.keep_data_dir_note(),
                    )
                }
                Err(e) => {
                    let tail = super::diagnostics::log_tail_section(&self.server_log());
                    let reports = super::diagnostics::faultbox_report_section(self.data_dir());
                    panic!(
                        "exec: {e}{}{}\n{reports}{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default(),
                        self.keep_data_dir_note(),
                    )
                }
            }
        }
    }

    /// Block until the Calvin sequencer group has elected a leader and can
    /// actually serve a Calvin-routed write — not merely until `/healthz`
    /// reports ready.
    ///
    /// `/healthz` is a one-shot boot-phase latch (`control/startup/health.rs`)
    /// that flips to OK at `GatewayEnable`; the Calvin sequencer is
    /// deliberately not a data group in that readiness gate, so a write can
    /// still hit `calvin-submit: no sequencer leader elected yet; cannot
    /// submit cross-shard transaction` after `/healthz` is already green.
    /// pgwire callers never see that race because `simple_query_ready`
    /// above already retries this exact condition — but ILP has no such
    /// retry (`handle_ilp_connection` in `control/server/ilp_listener.rs`
    /// logs the error and drops the connection), so an ILP line sent into
    /// that window is silently dropped with no client-visible signal at
    /// all. A caller that is about to drive ILP must wait for this
    /// separately, before sending its first line.
    ///
    /// Proves readiness the same way a real client would: by sending an
    /// actual Calvin-routed write and observing whether it succeeds, not by
    /// peeking at server-internal state. The probe write lands in
    /// `__crash_harness_calvin_probe`, a throwaway collection scoped to
    /// this call and never referenced by any caller's own assertions, so it
    /// cannot perturb a row count a test checks elsewhere.
    pub async fn wait_for_calvin_ready(&self, timeout: Duration) {
        self.exec(
            "CREATE COLLECTION __crash_harness_calvin_probe \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await;
        let deadline = Instant::now() + timeout;
        loop {
            let (client, connection) =
                tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                    .await
                    .expect("connect for calvin readiness probe");
            let conn_handle = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = client
                .simple_query(
                    "INSERT INTO __crash_harness_calvin_probe (id, ts, v) \
                     VALUES ('probe', 0, 0.0)",
                )
                .await;
            drop(client);
            let _ = conn_handle.await;
            match result {
                Ok(_) => return,
                Err(e) if is_no_sequencer_leader_error(&e) && Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) if is_no_sequencer_leader_error(&e) => {
                    let tail = super::diagnostics::log_tail_section(&self.server_log());
                    let reports = super::diagnostics::faultbox_report_section(self.data_dir());
                    panic!(
                        "the Calvin sequencer never elected a leader within {timeout:?} — a \
                         write routed through it still failed with \"no sequencer leader \
                         elected yet\" this long after boot: {e}{}{}\n{reports}{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default(),
                        self.keep_data_dir_note(),
                    )
                }
                Err(e) => {
                    let tail = super::diagnostics::log_tail_section(&self.server_log());
                    let reports = super::diagnostics::faultbox_report_section(self.data_dir());
                    panic!(
                        "calvin readiness probe failed with an error other than the \
                         sequencer-election race it exists to wait out: {e}{}{}\n{reports}{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default(),
                        self.keep_data_dir_note(),
                    )
                }
            }
        }
    }

    /// Open a fresh pgwire connection, run one statement, and drop the
    /// connection. Panics on connect/exec error.
    pub async fn exec(&self, sql: &str) {
        let _ = self.simple_query_ready(sql).await;
    }

    /// Run a query and return column `col` from every returned row, as text
    /// (via `simple_query`, so the value survives regardless of its type OID).
    pub async fn query_col(&self, sql: &str, col: &str) -> Vec<String> {
        let messages = self.simple_query_ready(sql).await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(col).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }

    /// Run a query and return column `idx` (0-based) from every returned row.
    ///
    /// Positional rather than by-name: a bare `COUNT(*)` — and even
    /// `COUNT(*) AS n` — does not surface a usable column name through the
    /// pgwire row description, so aggregates must be read by index.
    pub async fn query_col_idx(&self, sql: &str, idx: usize) -> Vec<String> {
        let messages = self.simple_query_ready(sql).await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(idx).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }

    /// Open a single pgwire connection that a caller can reuse across many
    /// queries. Panics on connect error.
    ///
    /// `exec` / `query_col` / `query_col_idx` each open a fresh connection
    /// per call, which is fine for a one-shot statement but turns a tight
    /// poll loop (e.g. waiting for a row to become visible) into a login
    /// flood: the server's login rate limiter sees the burst of new
    /// connections and starts refusing them (`E53300: too many login
    /// attempts`). `Session` connects once so a poll loop reuses the same
    /// connection for every attempt instead of paying the login cost each
    /// time.
    pub async fn connect(&self) -> Session<'_> {
        let (client, connection) =
            tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                .await
                .expect("connect for session");
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Session {
            harness: self,
            client,
            conn_handle,
        }
    }
}

/// A single pgwire connection held open across multiple queries. See
/// [`CrashHarness::connect`] for why this exists instead of always opening a
/// fresh connection per query.
pub struct Session<'h> {
    harness: &'h CrashHarness,
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
}

impl Session<'_> {
    /// Run a query and return column `idx` (0-based) from every returned
    /// row, as text — mirrors `CrashHarness::query_col_idx`, but over this
    /// session's already-open connection instead of a fresh one.
    ///
    /// Retries `RetryableSchemaChanged` the same way `simple_query_ready`
    /// does (see [`is_retryable_schema_change`] and its budget constants) —
    /// this is the helper the tests' tight polling loops actually use, so
    /// it needs the same client-retry behavior, not just the one-shot path.
    ///
    /// Panics once that budget is exhausted. A caller that runs this inside
    /// its own bounded poll should use [`Self::try_query_col_idx`] instead:
    /// this helper's ~750ms budget is far shorter than a typical poll
    /// deadline, so panicking here would abort a poll that still had seconds
    /// of budget left for exactly the condition the server told it to retry.
    pub async fn query_col_idx(&self, sql: &str, idx: usize) -> Vec<String> {
        match self.try_query_col_idx(sql, idx).await {
            Ok(rows) => rows,
            Err(RetryableSchemaChange) => {
                let tail = super::diagnostics::log_tail_section(&self.harness.server_log());
                let reports = super::diagnostics::faultbox_report_section(self.harness.data_dir());
                panic!(
                    "query on session: retryable schema change never cleared within \
                     {SCHEMA_CHANGE_RETRY_ATTEMPTS} attempts{}\n{reports}{tail}",
                    self.harness.keep_data_dir_note(),
                )
            }
        }
    }

    /// [`Self::query_col_idx`] that reports an unresolved retryable schema
    /// change to the caller instead of panicking on it.
    ///
    /// The server's own contract says a client observing this condition
    /// retries the statement — it is the descriptor-lease-drain race, not a
    /// distinct failure. A poll loop that owns a longer deadline is the right
    /// place to absorb it, so this returns [`RetryableSchemaChange`] and lets
    /// the caller decide. Every other error still panics: those are real.
    pub async fn try_query_col_idx(
        &self,
        sql: &str,
        idx: usize,
    ) -> Result<Vec<String>, RetryableSchemaChange> {
        let mut schema_change_attempts = 0usize;
        loop {
            match self.client.simple_query(sql).await {
                Ok(messages) => {
                    return Ok(messages
                        .iter()
                        .filter_map(|m| match m {
                            tokio_postgres::SimpleQueryMessage::Row(row) => {
                                Some(row.get(idx).unwrap_or_default().to_string())
                            }
                            _ => None,
                        })
                        .collect());
                }
                Err(e)
                    if is_retryable_schema_change(&e)
                        && schema_change_attempts < SCHEMA_CHANGE_RETRY_ATTEMPTS =>
                {
                    schema_change_attempts += 1;
                    tokio::time::sleep(SCHEMA_CHANGE_RETRY_BACKOFF).await;
                }
                // Hand the still-unresolved drain back to the caller. Callers
                // polling on their own deadline retry; `query_col_idx` panics.
                Err(e) if is_retryable_schema_change(&e) => return Err(RetryableSchemaChange),
                // Same rationale as `simple_query_ready`'s error branches: the
                // interesting failures here are server-side, and the harness's
                // tempdir is gone by the time anyone reads the panic (unless
                // `NODEDB_TEST_KEEP_DATA_DIR` says otherwise).
                Err(e) => {
                    let tail = super::diagnostics::log_tail_section(&self.harness.server_log());
                    let reports =
                        super::diagnostics::faultbox_report_section(self.harness.data_dir());
                    panic!(
                        "query on session: {e}{}{}\n{reports}{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default(),
                        self.harness.keep_data_dir_note(),
                    )
                }
            }
        }
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        // The connection driver task holds no resources the OS needs back
        // synchronously, but abort it anyway so a dropped session doesn't
        // leave an orphaned task polling a socket nobody reads from again.
        self.conn_handle.abort();
    }
}

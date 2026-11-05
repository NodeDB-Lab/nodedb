// SPDX-License-Identifier: BUSL-1.1

//! One ILP TCP connection: the Hello/Auth prelude, admission, and the
//! adaptive batch-coalescing ingest loop. The accept loop lives in
//! `ilp_listener`.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::io::BufReader;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{debug, warn};

use crate::config::auth::AuthMode;
use crate::control::security::sessions::{SessionBindRequest, SessionBinding, bind_session};
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::ilp_auth::AuthenticatedIlpContext;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::ilp_batch::{IlpRateEstimator, flush_ilp_batch};
use super::ilp_drop::{IlpDropCause, terminate_with_buffered_flush};
use super::ilp_line_read::read_bounded_ilp_line;

/// Maximum byte length of a single ILP line. Lines exceeding this are
/// rejected and the connection is dropped to prevent memory exhaustion.
const MAX_ILP_LINE_BYTES: usize = 10 * 1024 * 1024; // 10 MiB
/// Per-connection aggregate cap. A batch must not turn many individually
/// valid lines into an unbounded allocation before its timer/line flush.
const MAX_ILP_BATCH_BYTES: usize = MAX_ILP_LINE_BYTES;

/// Handle a single ILP TCP connection.
///
/// The connection future is boxed, once per connection. Every ingest path
/// nests inside it, and unboxed it can overflow the compiler's layout depth
/// limit in the listener's connection task.
pub(super) fn handle_ilp_connection<'a>(
    stream: ConnStream,
    peer: SocketAddr,
    state: &'a Arc<SharedState>,
    auth_mode: &'a AuthMode,
) -> BoxFuture<'a, crate::Result<()>> {
    Box::pin(serve_ilp_connection(stream, peer, state, auth_mode))
}

/// Serve one ILP connection with adaptive batch coalescing.
///
/// Batch size adapts to ingest rate:
/// - High rate (>100K lines/s): batch up to 10K lines or 10ms window
/// - Medium rate (1K-100K/s): batch up to 1K lines or 50ms window
/// - Low rate (<1K/s): batch per 100 lines or 100ms window
///
/// Larger batches amortize per-batch overhead (WAL append, memtable lock,
/// partition lookup).
async fn serve_ilp_connection(
    mut stream: ConnStream,
    peer: SocketAddr,
    state: &Arc<SharedState>,
    auth_mode: &AuthMode,
) -> crate::Result<()> {
    // Captured before the Hello/Auth prelude borrows the stream and the
    // ingest loop moves it into a `BufReader`, after which the TLS session is
    // no longer reachable.
    let transport = stream.transport_security();

    // The native Hello/Auth prelude must finish before line parsing, tenant
    // accounting, or any ingest side effect. Authentication failures consume
    // no ILP bytes and the dropped stream cannot enter the ingest loop.
    let mut authenticated_context = crate::control::server::ilp_auth::authenticate_ilp_connection(
        &mut stream,
        state,
        auth_mode,
        &peer.to_string(),
    )
    .await
    .map_err(|_| crate::Error::BadRequest {
        detail: "ILP authentication failed".into(),
    })?;
    // The TLS policy is evaluated before any ingest capacity is acquired: the
    // identity (and with it the superuser flag the cleartext carve-out needs)
    // exists only now, and a refused connection must not hold a slot.
    if crate::control::server::session_auth::check_transport_security(
        state,
        authenticated_context.identity(),
        transport,
        authenticated_context.peer_addr(),
    )
    .is_err()
    {
        crate::control::server::ilp_auth::write_ilp_auth_failure(
            &mut stream,
            &authenticated_context,
        )
        .await;
        return Err(crate::Error::BadRequest {
            detail: "ILP authentication failed".into(),
        });
    }

    let _admission = match IlpConnectionAdmission::acquire(state, &authenticated_context) {
        Ok(admission) => admission,
        Err(_) => {
            crate::control::server::ilp_auth::write_ilp_auth_failure(
                &mut stream,
                &authenticated_context,
            )
            .await;
            return Err(crate::Error::BadRequest {
                detail: "ILP authentication failed".into(),
            });
        }
    };
    let mut binding = match bind_session(
        state,
        SessionBindRequest::new(
            authenticated_context.identity(),
            "ilp",
            authenticated_context.peer_addr(),
        )
        .in_database(authenticated_context.database_id()),
    ) {
        Ok(binding) => binding,
        Err(_) => {
            crate::control::server::ilp_auth::write_ilp_auth_failure(
                &mut stream,
                &authenticated_context,
            )
            .await;
            return Err(crate::Error::BadRequest {
                detail: "ILP authentication failed".into(),
            });
        }
    };
    crate::control::server::ilp_auth::write_ilp_auth_success(&mut stream, &authenticated_context)
        .await
        .map_err(|_| crate::Error::BadRequest {
            detail: "ILP authentication failed".into(),
        })?;

    debug!(%peer, "authenticated ILP connection accepted");

    let mut reader = BufReader::new(stream);
    let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut batch = String::new();
    let mut line_count = 0u64;
    let mut total_ingested = 0u64;

    // Adaptive batch coalescing state.
    let mut rate_estimator = IlpRateEstimator::new();
    let mut batch_target = 1000u64;
    let mut window = tokio::time::interval(std::time::Duration::from_millis(50));
    window.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // A killed session's buffered lines are refused, not flushed: their
            // author no longer has the right to write them.
            ended = binding.ended() => {
                report_killed_session_lines(peer, &authenticated_context, &ended, line_count);
                return Err(ended);
            }
            // Read next line with an enforced byte-length cap.
            result = read_bounded_ilp_line(&mut reader, &mut line_buf, MAX_ILP_LINE_BYTES) => {
                match result {
                    Ok(false) => break, // Connection closed (EOF).
                    Ok(true) => {
                        // Strip trailing newline / CRLF.
                        let line_bytes = line_buf
                            .strip_suffix(b"\r\n")
                            .or_else(|| line_buf.strip_suffix(b"\n"))
                            .unwrap_or(&line_buf);

                        let line = match std::str::from_utf8(line_bytes) {
                            Ok(s) => s,
                            // The framing is broken and cannot be resynchronized, so the
                            // connection still ends here — but the lines already accepted
                            // into `batch` are dispatched first and the termination is
                            // recorded, because ILP acks nothing and the client will
                            // otherwise have no way to learn they were discarded.
                            Err(_) => {
                                return Err(terminate_with_buffered_flush(
                                    state,
                                    &authenticated_context,
                                    peer,
                                    IlpDropCause::InvalidUtf8,
                                    &batch,
                                    line_count,
                                )
                                .await);
                            }
                        };

                        if line.trim().is_empty() || line.trim_start().starts_with('#') {
                            line_buf.clear();
                            continue;
                        }

                        let separator_bytes = if batch.is_empty() { 0 } else { 1 };
                        if batch
                            .len()
                            .saturating_add(separator_bytes)
                            .saturating_add(line.len())
                            > MAX_ILP_BATCH_BYTES
                        {
                            let flushed = line_count;
                            prepare_flush(
                                &mut binding,
                                state,
                                &mut authenticated_context,
                                peer,
                                &batch,
                                line_count,
                            )?;
                            total_ingested +=
                                flush_ilp_batch(state, &authenticated_context, &batch).await?;
                            batch.clear();
                            line_count = 0;

                            rate_estimator.record(flushed);
                            let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                            batch_target = new_target;
                            window = tokio::time::interval(
                                std::time::Duration::from_millis(new_window_ms),
                            );
                            window.set_missed_tick_behavior(
                                tokio::time::MissedTickBehavior::Delay,
                            );
                        }

                        if !batch.is_empty() {
                            batch.push('\n');
                        }
                        batch.push_str(line);
                        line_count += 1;
                        line_buf.clear();

                        // Flush when batch reaches adaptive target.
                        if line_count >= batch_target {
                            let flushed = line_count;
                            prepare_flush(
                                &mut binding,
                                state,
                                &mut authenticated_context,
                                peer,
                                &batch,
                                line_count,
                            )?;
                            total_ingested +=
                                flush_ilp_batch(state, &authenticated_context, &batch).await?;
                            batch.clear();
                            line_count = 0;

                            // Update rate estimator and recalculate batch target.
                            rate_estimator.record(flushed);
                            let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                            batch_target = new_target;
                            window = tokio::time::interval(
                                std::time::Duration::from_millis(new_window_ms),
                            );
                            window.set_missed_tick_behavior(
                                tokio::time::MissedTickBehavior::Delay,
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            %peer,
                            error = %error,
                            limit = MAX_ILP_LINE_BYTES,
                            "ILP line read failed — rejecting connection"
                        );
                        // Same contract as the decode failure above: the connection
                        // ends, but not before the accepted lines are dispatched and
                        // the loss surface is recorded.
                        return Err(terminate_with_buffered_flush(
                            state,
                            &authenticated_context,
                            peer,
                            IlpDropCause::LineReadFailed,
                            &batch,
                            line_count,
                        )
                        .await);
                    }
                }
            }
            // Timer-based flush (for low-rate connections).
            _ = window.tick() => {
                if !batch.is_empty() {
                    let flushed = line_count;
                    prepare_flush(
                        &mut binding,
                        state,
                        &mut authenticated_context,
                        peer,
                        &batch,
                        line_count,
                    )?;
                    total_ingested +=
                        flush_ilp_batch(state, &authenticated_context, &batch).await?;
                    batch.clear();
                    line_count = 0;

                    rate_estimator.record(flushed);
                    let (new_target, new_window_ms) = rate_estimator.suggest_batch_params();
                    batch_target = new_target;
                    window = tokio::time::interval(
                        std::time::Duration::from_millis(new_window_ms),
                    );
                    window.set_missed_tick_behavior(
                        tokio::time::MissedTickBehavior::Delay,
                    );
                }
            }
        }
    }

    // Flush remaining.
    if !batch.is_empty() {
        prepare_flush(
            &mut binding,
            state,
            &mut authenticated_context,
            peer,
            &batch,
            line_count,
        )?;
        total_ingested += flush_ilp_batch(state, &authenticated_context, &batch).await?;
    }

    debug!(
        %peer,
        total_ingested,
        database_id = ?authenticated_context.database_id(),
        "ILP connection closed"
    );
    Ok(())
}

/// Check the kill and bring the identity up to date ahead of a batch flush, and
/// mark the activity. ILP works per batch, so this keeps the per-line path clear
/// of the registry lock.
fn prepare_flush(
    binding: &mut SessionBinding,
    state: &SharedState,
    context: &mut AuthenticatedIlpContext,
    peer: SocketAddr,
    batch: &str,
    buffered_lines: u64,
) -> crate::Result<()> {
    if let Err(ended) = binding.refresh(state, context.identity_mut()) {
        report_killed_session_lines(peer, context, &ended, buffered_lines);
        return Err(ended);
    }
    binding.touch(batch.len() as u64, 0);
    Ok(())
}

/// ILP acks nothing, so the client cannot learn that lines were refused. The
/// report is the one record of them.
fn report_killed_session_lines(
    peer: SocketAddr,
    context: &AuthenticatedIlpContext,
    ended: &crate::Error,
    buffered_lines: u64,
) {
    warn!(%peer, buffered_lines, error = %ended, "ILP session killed; buffered lines refused");
    if buffered_lines > 0 {
        crate::diag::ilp_killed_session_lines_refused(
            &peer.to_string(),
            context.database_id().as_u64(),
            buffered_lines,
        );
    }
}

/// Connection-scoped tenant accounting and quota permits.
///
/// The permit fields release configured database/tenant limits on every return
/// path, while `Drop` balances the legacy tenant activity accounting.
struct IlpConnectionAdmission<'a> {
    state: &'a SharedState,
    tenant_id: TenantId,
    _database_permit: Option<OwnedSemaphorePermit>,
    _tenant_permit: Option<OwnedSemaphorePermit>,
}

impl<'a> IlpConnectionAdmission<'a> {
    fn acquire(state: &'a SharedState, context: &AuthenticatedIlpContext) -> crate::Result<Self> {
        let tenant_id = context.identity().tenant_id;
        let database_id = context.database_id();

        let database_permit = state
            .admission_registry
            .try_acquire_database(database_id)
            .map_err(|_| crate::Error::BadRequest {
                detail: "ILP admission denied".into(),
            })?;
        let tenant_permit = state
            .admission_registry
            .try_acquire_tenant(database_id, tenant_id)
            .map_err(|_| crate::Error::BadRequest {
                detail: "ILP admission denied".into(),
            })?;

        start_tenant_connection(state, tenant_id)?;
        Ok(Self {
            state,
            tenant_id,
            _database_permit: database_permit,
            _tenant_permit: tenant_permit,
        })
    }
}

impl Drop for IlpConnectionAdmission<'_> {
    fn drop(&mut self) {
        self.state.tenant_connection_end(self.tenant_id);
    }
}

/// Atomically check and account for the legacy per-tenant connection cap.
///
/// The database/tenant semaphore permits above cover configured catalog
/// quotas; this lock also protects the legacy tenant-isolation counter from a
/// check-then-increment race when it has its own max-connections setting.
fn start_tenant_connection(state: &SharedState, tenant_id: TenantId) -> crate::Result<()> {
    let mut tenants = state
        .tenants
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !matches!(
        tenants.check_connection(tenant_id),
        crate::control::security::tenant::QuotaCheck::Allowed
    ) {
        return Err(crate::Error::BadRequest {
            detail: "ILP admission denied".into(),
        });
    }
    tenants.connection_start(tenant_id);
    Ok(())
}

// SPDX-License-Identifier: BUSL-1.1

//! pgwire COPY-protocol bridge for the backup/restore wire surface and for
//! `COPY <array> FROM STDIN`, whose load lives in `array_copy`.
//!
//! - `intent_to_response()` runs in `SimpleQueryHandler::do_query`
//!   when `backup::detect()` recognises a wire COPY shape. It either
//!   returns a `Response::CopyOut` whose stream pulls bytes from
//!   `backup_tenant`, or registers per-connection state and returns
//!   `Response::CopyIn` so the client begins streaming bytes.
//! - `CopyHandler::on_copy_data` streams an array COPY's bytes into its
//!   load, or accumulates a restore's bytes (size-capped).
//! - `CopyHandler::on_copy_done` validates the envelope and dispatches
//!   `restore::restore_tenant`.

use std::fmt::Debug;
use std::sync::Arc;

use nodedb_types::error::sqlstate as ss;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, SinkExt};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};

use crate::control::backup;
use crate::control::backup::CopyIntent;
use crate::control::backup::state::AppendError;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::backup_metering::{
    admit_backup_restore_quota, meter_backup_restore,
};
use crate::control::server::shared::session::{ConnectionId, SessionId, SessionStore};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::array_copy::{ArrayCopyConn, ArrayCopyState, feed_array_copy, finish_array_copy};
use super::core::NodeDbPgHandler;

/// Hard cap on accumulated COPY IN bytes for one restore. 16 GiB matches
/// the envelope's default total cap; any larger payload is rejected
/// before it can drive unbounded server allocation.
const COPY_IN_CAP: u64 = 16 * 1024 * 1024 * 1024;

impl NodeDbPgHandler {
    /// Translate a recognised `CopyIntent` into a pgwire COPY response.
    pub(super) async fn intent_to_response(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        intent: CopyIntent,
    ) -> PgWireResult<Response> {
        // Blacklist + account status, no rate limit: backup/restore is
        // admin-scoped bulk data movement, not the per-query traffic the
        // rate-limiter's cost table models, so charging it against a query
        // rate limit will throttle a legitimate restore. A blacklisted or
        // suspended/banned account must not be able to run backup or
        // restore, though — `check_blacklist_and_status` runs that half of
        // `check_request_admission`'s gate (plus the internal-service
        // exemption every other transport gets) using this connection's
        // real peer address.
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
                        "connection session metadata is unavailable".to_owned(),
                    )))
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let database_id = identity
            .default_database
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let peer_addr = peer_addr.to_string();
        let request = crate::control::security::request_scope::ClientRequestScope::for_database(
            identity,
            self.state.auth_stores(),
            database_id,
            &peer_addr,
        )
        .map_err(typed)?;
        crate::control::server::session_auth::check_blacklist_and_status(&self.state, &request)
            .map_err(|e| {
                let (severity, code, message) =
                    crate::control::server::pgwire::types::error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

        // Backup and restore both operate on a whole tenant — authorize
        // against the tenant-scoped `Backup` permission. Superuser bypasses
        // the grant check; everyone else needs `GRANT BACKUP ON TENANT`.
        let tenant_id = match &intent {
            CopyIntent::BackupTenant { tenant_id } => *tenant_id,
            CopyIntent::RestoreTenant { tenant_id, .. } => *tenant_id,
        };
        if !identity.is_superuser {
            // A tenant is backed up or restored by its own tenant only.
            if TenantId::new(tenant_id) != identity.tenant_id {
                return Err(sqlstate(
                    ss::INSUFFICIENT_PRIVILEGE,
                    "permission denied: backup and restore of another tenant require superuser",
                ));
            }
            let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
            let allowed = self.state.permissions.check_tenant(
                identity,
                Permission::Backup,
                TenantId::new(tenant_id),
                &self.state.roles,
                &emitter,
            );
            if !allowed {
                return Err(sqlstate(
                    ss::INSUFFICIENT_PRIVILEGE,
                    "permission denied: BACKUP permission on the tenant required",
                ));
            }
        }

        match intent {
            CopyIntent::BackupTenant { tenant_id } => {
                // A spent hard quota refuses the backup before it reads a
                // byte. The charge below is on the success path and so can
                // never be where a cap blocks anything.
                admit_backup_restore_quota(&self.state, request.scope(), tenant_id)
                    .map_err(typed)?;
                let bytes = backup::backup_tenant(&self.state, tenant_id)
                    .await
                    .map_err(typed)?;
                // Metered here, on the success path, before the response is
                // built below — there is no `PhysicalPlan` for a whole-tenant
                // backup, so the collection dimension is a synthetic
                // `tenant:<id>` marker rather than a real collection name.
                // `rows: None` — the backup produces a byte blob, not a row
                // count; `meter_dispatch` charges one unit for `None`.
                meter_backup_restore(&self.state, request.scope(), tenant_id, None);
                let copy_data = Ok(CopyData::new(bytes));
                let stream = stream::once(async move { copy_data });
                Ok(Response::CopyOut(CopyResponse::new(0, 0, stream)))
            }
            CopyIntent::RestoreTenant {
                tenant_id,
                dry_run,
                force,
            } => {
                let connection_id = match session_id {
                    SessionId::Connection(connection_id) => connection_id,
                    SessionId::LegacySocket(_) => {
                        return Err(sqlstate(
                            ss::INTERNAL_ERROR,
                            "COPY restore requires a typed connection",
                        ));
                    }
                };
                self.restore_state.begin(
                    connection_id.get(),
                    backup::RestorePending::new(
                        tenant_id,
                        dry_run,
                        force,
                        COPY_IN_CAP,
                        identity.clone(),
                    ),
                );
                // Empty out-stream — server tells client "send me bytes".
                let empty = stream::empty();
                Ok(Response::CopyIn(CopyResponse::new(0, 0, empty)))
            }
        }
    }
}

/// CopyHandler shared by the factory's per-connection handler. Holds
/// only the `Arc<RestoreState>` it needs — the rest of the SharedState
/// is reachable via the cloned handle for the dispatch on `on_copy_done`.
pub struct NodeDbCopyHandler {
    pub state: Arc<SharedState>,
    pub restore_state: Arc<backup::RestoreState>,
    /// Open `COPY <array> FROM STDIN` loads, keyed by connection.
    pub array_copy: Arc<ArrayCopyState>,
    /// The session store, for the transaction an array load runs in.
    pub sessions: Arc<SessionStore>,
    pub connection_id: ConnectionId,
}

#[async_trait]
impl CopyHandler for NodeDbCopyHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = self.connection_id.get();
        let array_conn = ArrayCopyConn {
            state: &self.state,
            sessions: &self.sessions,
            session_id: SessionId::Connection(self.connection_id),
            copies: &self.array_copy,
            conn_id: id,
        };
        // Boxed: an array load dispatches whole statements, and inlining that
        // future here nests past the layout limit.
        if Box::pin(feed_array_copy(&array_conn, &copy_data.data)).await? {
            return Ok(());
        }
        match self.restore_state.append(id, &copy_data.data) {
            Ok(()) => Ok(()),
            Err(e @ AppendError::NotPending) => {
                Err(sqlstate(ss::FEATURE_NOT_SUPPORTED, &e.to_string()))
            }
            Err(e @ AppendError::OverCap { .. }) => {
                self.restore_state.cancel(id);
                Err(sqlstate(ss::PROGRAM_LIMIT_EXCEEDED, &e.to_string()))
            }
        }
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, _fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if self.array_copy.take(self.connection_id.get()).is_some() {
            return sqlstate(ss::QUERY_CANCELED.0, "COPY aborted by the client");
        }
        cancel_restore(&self.restore_state, self.connection_id);
        sqlstate(ss::QUERY_CANCELED.0, "COPY restore aborted")
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = self.connection_id.get();
        if let Some(pending) = self.array_copy.take(id) {
            let session_id = SessionId::Connection(self.connection_id);
            // Boxed: an array load dispatches whole statements, and inlining
            // that future here nests past the layout limit.
            let rows = Box::pin(finish_array_copy(
                &self.state,
                &self.sessions,
                session_id,
                pending,
            ))
            .await?;
            return complete_copy_in(client, Tag::new("COPY").with_rows(rows as usize)).await;
        }
        let pending = self.restore_state.take(id).ok_or_else(|| {
            sqlstate(
                ss::FEATURE_NOT_SUPPORTED,
                "no restore pending on this connection",
            )
        })?;
        // Scope resolved here rather than only below, so the quota gate can
        // refuse the restore before it writes anything — the charge further
        // down is on the success path and so can never refuse anything.
        let database_id = pending
            .identity
            .default_database
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let scope = RequestAuthScope::for_database(
            &pending.identity,
            self.state.auth_stores(),
            database_id,
        )
        .map_err(typed)?;
        admit_backup_restore_quota(&self.state, &scope, pending.tenant_id).map_err(typed)?;

        let stats = backup::restore_tenant(
            &self.state,
            pending.tenant_id,
            &pending.bytes,
            pending.dry_run,
            pending.force,
        )
        .await
        .map_err(typed)?;
        // pgwire does not auto-send CommandComplete after `on_copy_done`
        // returns Ok — the trait contract leaves message construction to
        // the handler. Send a `RESTORE TENANT N <op-count>` tag so the
        // client's COPY IN sink can complete.
        let rows =
            stats.documents + stats.kv_tables + stats.vectors + stats.timeseries + stats.edges;
        // Metered on the success path, using the same `rows` count the
        // command tag below reports, and the same scope the quota gate above
        // resolved — `pending.identity` is the caller who opened this COPY IN
        // back in `intent_to_response`, carried through `RestorePending`
        // since this per-connection `CopyHandler` has no identity of its own.
        meter_backup_restore(&self.state, &scope, pending.tenant_id, Some(rows as u64));
        complete_copy_in(client, Tag::new("RESTORE TENANT").with_rows(rows)).await
    }
}

/// Send the COPY's CommandComplete `tag` and leave the COPY-in state.
async fn complete_copy_in<C>(client: &mut C, tag: Tag) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await
        .map_err(|e| {
            sqlstate(
                ss::INTERNAL_ERROR,
                &format!("CommandComplete send failed: {e:?}"),
            )
        })?;
    // Leave the COPY-in-progress state so the next Sync from the
    // client gets dispatched normally. pgwire's `process_message`
    // only routes Sync via the `AwaitingSync` arm.
    client.set_state(PgWireConnectionState::AwaitingSync);
    Ok(())
}

fn cancel_restore(state: &backup::RestoreState, connection_id: ConnectionId) {
    state.cancel(connection_id.get());
}

fn sqlstate(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

/// Render a backup or restore error with its own SQLSTATE: a spent quota,
/// a tenant mismatch or a wrong key keeps its class. The restore
/// orchestrator already scrubs envelope errors before they reach here.
fn typed(e: crate::Error) -> PgWireError {
    super::super::types::error_to_pg(&e)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bridge::dispatch::Dispatcher;
    use crate::config::auth::AuthMode;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::wal::WalManager;

    fn test_handler() -> (NodeDbPgHandler, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (NodeDbPgHandler::new(state, AuthMode::Trust), dir)
    }

    fn identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "copy-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    /// `intent_to_response` runs the blacklist and account-status check, so a
    /// blacklisted client cannot run backup/restore.
    #[tokio::test]
    async fn intent_to_response_rejects_blacklisted_identity() {
        let (handler, _dir) = test_handler();
        let identity = identity(101);
        handler
            .state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let result = handler
            .intent_to_response(
                &identity,
                handler.session_id,
                CopyIntent::BackupTenant { tenant_id: 1 },
            )
            .await;

        assert!(
            result.is_err(),
            "a blacklisted identity must be rejected before backup/restore runs"
        );
    }

    /// A suspended JIT-provisioned account must not be able to run
    /// backup/restore, even though this door skips the rate limiter.
    #[tokio::test]
    async fn intent_to_response_rejects_suspended_account() {
        let (handler, _dir) = test_handler();
        let identity = identity(102);
        handler
            .state
            .auth_users
            .upsert(crate::control::security::jit::auth_user::AuthUserRecord {
                id: identity.user_id.to_string(),
                username: identity.username.clone(),
                email: String::new(),
                tenant_id: identity.tenant_id.as_u64(),
                provider: "test".into(),
                first_seen: 0,
                last_seen: 0,
                is_active: false,
                status: crate::control::security::auth_context::AuthStatus::Suspended,
                is_external: true,
                synced_claims: std::collections::HashMap::new(),
                escalation_suspensions: 0,
            })
            .expect("register suspended auth user");

        let result = handler
            .intent_to_response(
                &identity,
                handler.session_id,
                CopyIntent::BackupTenant { tenant_id: 1 },
            )
            .await;

        assert!(
            result.is_err(),
            "a suspended account must be rejected before backup/restore runs"
        );
    }

    #[test]
    fn copy_fail_cancels_only_the_exact_pending_restore() {
        let state = backup::RestoreState::new();
        let first = ConnectionId::new(1).unwrap_or_else(|_| unreachable!());
        let second = ConnectionId::new(2).unwrap_or_else(|_| unreachable!());
        state.begin(
            first.get(),
            backup::RestorePending::new(1, false, false, 16, identity(1)),
        );
        state.begin(
            second.get(),
            backup::RestorePending::new(1, false, false, 16, identity(2)),
        );

        cancel_restore(&state, first);

        assert_eq!(
            state.append(first.get(), b"x"),
            Err(AppendError::NotPending)
        );
        assert!(state.append(second.get(), b"x").is_ok());
    }

    /// `meter_backup_restore` is called unconditionally from both the backup
    /// success arm of `intent_to_response` and the restore success tail of
    /// `on_copy_done` — never from an error path in either (see the source
    /// above: it runs after `?` has already propagated any failure) — so
    /// covering it directly here exercises the same enabled/disabled and
    /// row-count behavior every call site relies on.
    #[test]
    fn metering_disabled_by_default_records_nothing() {
        let (handler, _dir) = test_handler();
        assert!(
            !handler.state.metering_config.enabled,
            "default is disabled"
        );
        let user = identity(1);
        let scope = RequestAuthScope::for_database(
            &user,
            handler.state.auth_stores(),
            crate::types::DatabaseId::DEFAULT,
        )
        .expect("resolve request scope");

        meter_backup_restore(&handler.state, &scope, 7, Some(42));

        assert_eq!(handler.state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn enabled_backup_restore_records_one_event_for_the_tenant() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct shared state");
        // Sole owner at this point — construction below is the only clone.
        std::sync::Arc::get_mut(&mut state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
        let handler = NodeDbPgHandler::new(state, AuthMode::Trust);
        let user = identity(1);
        let scope = RequestAuthScope::for_database(
            &user,
            handler.state.auth_stores(),
            crate::types::DatabaseId::DEFAULT,
        )
        .expect("resolve request scope");

        meter_backup_restore(&handler.state, &scope, 7, Some(42));

        let events = handler.state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "tenant:7");
        assert_eq!(events[0].engine, "meta");
        assert_eq!(events[0].tokens, 42);
    }
}

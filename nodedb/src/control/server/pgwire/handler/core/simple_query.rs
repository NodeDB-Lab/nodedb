// SPDX-License-Identifier: BUSL-1.1

use std::fmt::Debug;

use async_trait::async_trait;
use futures::SinkExt;
use futures::sink::Sink;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use super::NodeDbPgHandler;
use crate::control::server::pgwire::handler::in_flight::InFlightGuard;
use crate::control::server::pgwire::types::wire_type::TEXT_RESULTS;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::txn_control::{TxnControl, classify as classify_txn_control};

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = self.session_id;

        // Keep this statement ineligible for idle teardown until every exit
        // path completes and the guard stamps the session's last activity.
        let _in_flight = InFlightGuard::new(&self.sessions, session_id);

        // Pin this statement's deadline before any work is planned or
        // dispatched. Every Control -> Data envelope the statement fans out
        // into reads it, so the whole statement is bounded by one budget
        // instead of each hop restarting the clock. The guard restores the
        // previous value on every exit path.
        let _statement_scope = crate::control::server::shared::session::deadline::enter(
            self.sessions.statement_timeout(session_id),
            self.state.tuning.network.default_deadline_secs,
        );

        let identity = self.resolve_identity(client, &session_id)?;
        self.authorize_session_database(&identity, session_id)?;

        // Emit db.id / db.name trace fields at session bind so that any
        // downstream spans inherit the database context.
        let current_db = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let db_name: String = self
            .state
            .credentials
            .catalog()
            .get_database(current_db)
            .ok()
            .flatten()
            .map(|d| d.name.clone())
            .unwrap_or_else(|| "default".to_string());
        tracing::debug!(
            db.id = current_db.as_u64(),
            db.name = %db_name,
            user = %identity.username,
            "session query dispatch",
        );

        // Warn on BEGIN inside a block and on COMMIT outside one, as
        // PostgreSQL does.
        let control = classify_txn_control(query);
        if matches!(control, Some(TxnControl::Begin(_)))
            && self.sessions.transaction_state(session_id) == TransactionState::InBlock
        {
            let notice = super::super::super::types::notice_warning(
                "there is already a transaction in progress",
            );
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        if control == Some(TxnControl::Commit)
            && self.sessions.transaction_state(session_id) == TransactionState::Idle
        {
            let notice =
                super::super::super::types::notice_warning("there is no transaction in progress");
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        // J.4: install the DDL audit context for this statement. Any
        // `propose_catalog_entry` call reached from `execute_sql`
        // picks up the identity + raw SQL so the applier can emit a
        // full audit record on every replica. The guard auto-clears
        // on scope exit.
        let _audit_scope = crate::control::server::shared::session::audit_context::AuditScope::new(
            crate::control::server::shared::session::audit_context::AuditCtx {
                auth_user_id: identity.user_id.to_string(),
                auth_user_name: identity.username.clone(),
                sql_text: query.to_string(),
            },
        );

        let result = self
            .execute_sql(&identity, session_id, query, &TEXT_RESULTS)
            .await;

        // Drain queued NOTICE messages emitted by response shapers (e.g.
        // `truncated_before_horizon` on array slices) and raised below them
        // (`session::statement_notice`), and send them before
        // the query result so the client associates the warning with the
        // current statement.
        for message in self
            .sessions
            .drain_notices(session_id)
            .into_iter()
            .chain(crate::control::server::shared::session::statement_notice::take())
        {
            let notice = super::super::super::types::notice_warning(&message);
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }
        // Cross-shard reads the query made join the transaction's
        // read-set.
        crate::control::server::shared::session::pending_shard_reads::record_pending(
            &self.sessions,
            session_id,
        );

        // Drain pending LIVE SELECT notifications and send as pgwire
        // async NotificationResponse messages. This is the standard
        // PostgreSQL notification delivery model: notifications are
        // delivered between queries.
        if self.sessions.has_live_subscriptions(session_id) {
            let notifications = self.sessions.drain_live_notifications(session_id);
            for (channel, payload) in notifications {
                let notification = pgwire::messages::response::NotificationResponse::new(
                    0, // backend PID (not meaningful for NodeDB)
                    channel, payload,
                );
                let _ = client
                    .send(PgWireBackendMessage::NotificationResponse(notification))
                    .await;
            }
        }

        // Drain pending LISTEN/NOTIFY notifications and deliver as pgwire
        // NotificationResponse messages (between queries, per PG semantics).
        if self.sessions.has_listen_subscriptions(session_id) {
            let notifications = self.sessions.drain_listen_notifications(session_id);
            for n in notifications {
                let notification = pgwire::messages::response::NotificationResponse::new(
                    n.pid, n.channel, n.payload,
                );
                let _ = client
                    .send(PgWireBackendMessage::NotificationResponse(notification))
                    .await;
            }
        }

        result
    }
}

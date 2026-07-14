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
use crate::control::server::shared::session::TransactionState;

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        self.sessions.ensure_session(addr);

        // Keep this statement ineligible for idle teardown until every exit
        // path completes and the guard stamps the session's last activity.
        let _in_flight = InFlightGuard::new(&self.sessions, addr);

        let identity = self.resolve_identity(client, &addr)?;
        self.authorize_session_database(&identity, &addr)?;

        // Emit db.id / db.name trace fields at session bind so that any
        // downstream spans inherit the database context.
        let current_db = self
            .sessions
            .get_current_database(&addr)
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

        // Send notice if BEGIN is called (advisory transactions).
        let upper = query.trim().to_uppercase();
        if (upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION")
            && self.sessions.transaction_state(&addr) == TransactionState::InBlock
        {
            let notice = super::super::super::types::notice_warning(
                "there is already a transaction in progress",
            );
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        if (upper == "COMMIT" || upper == "END")
            && self.sessions.transaction_state(&addr) == TransactionState::Idle
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

        let result = self.execute_sql(&identity, &addr, query).await;

        // Drain queued NOTICE messages emitted by response shapers (e.g.
        // `truncated_before_horizon` on array slices) and send them before
        // the query result so the client associates the warning with the
        // current statement.
        for message in self.sessions.drain_notices(&addr) {
            let notice = super::super::super::types::notice_warning(&message);
            let _ = client
                .send(PgWireBackendMessage::NoticeResponse(notice))
                .await;
        }

        // Drain pending LIVE SELECT notifications and send as pgwire
        // async NotificationResponse messages. This is the standard
        // PostgreSQL notification delivery model: notifications are
        // delivered between queries.
        if self.sessions.has_live_subscriptions(&addr) {
            let notifications = self.sessions.drain_live_notifications(&addr);
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
        if self.sessions.has_listen_subscriptions(&addr) {
            let notifications = self.sessions.drain_listen_notifications(&addr);
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

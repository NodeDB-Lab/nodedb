// SPDX-License-Identifier: BUSL-1.1

//! BEGIN and ROLLBACK adapters — thin pgwire shims over the protocol-neutral
//! lifecycle orchestrator (`control/server/shared/session/lifecycle.rs`). The
//! staging-overlay release, DDL-buffer, and GAP_FREE rollback logic all live in
//! the neutral core now; these functions only shape the tag / error.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{TransactionState, lifecycle};

use super::super::core::NodeDbPgHandler;
use super::commit::PgwireTxnDp;

impl NodeDbPgHandler {
    /// Handle BEGIN / START TRANSACTION.
    pub(in crate::control::server::pgwire::handler) fn handle_begin(
        &self,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        match lifecycle::run_begin(&self.sessions, addr, &self.state) {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("BEGIN"))]),
            Err(e) => {
                let message = match &e {
                    crate::Error::BadRequest { detail } => detail.clone(),
                    other => other.to_string(),
                };
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25P02".to_owned(),
                    message,
                ))))
            }
        }
    }

    /// Handle ROLLBACK / ABORT.
    pub(in crate::control::server::pgwire::handler) async fn handle_rollback(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let dp = PgwireTxnDp { handler: self };
        lifecycle::run_rollback(&self.sessions, addr, identity, &self.state, &dp).await;
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }

    /// Reclaim an abandoned transaction's Data-Plane staging overlays when a
    /// pgwire connection ends without COMMIT/ROLLBACK.
    ///
    /// A no-op when the connection had no open transaction (the common case).
    /// Otherwise drives the same neutral `run_rollback` path as an explicit
    /// ROLLBACK, using the identity stashed by `resolve_identity` on the last
    /// query — without it the overlays (keyed by `txn_id` per staged vShard)
    /// would leak for the process lifetime.
    pub(in crate::control::server::pgwire) async fn reclaim_open_txn(
        &self,
        addr: &std::net::SocketAddr,
    ) {
        if self.sessions.transaction_state(addr) == TransactionState::Idle {
            return;
        }
        let Some(identity) = self.sessions.identity(addr) else {
            return;
        };
        let dp = PgwireTxnDp { handler: self };
        lifecycle::run_rollback(&self.sessions, addr, &identity, &self.state, &dp).await;
    }
}

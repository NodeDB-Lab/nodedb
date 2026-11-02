// SPDX-License-Identifier: BUSL-1.1

//! BEGIN and ROLLBACK adapters — thin pgwire shims over the protocol-neutral
//! lifecycle orchestrator (`control/server/shared/session/lifecycle.rs`). The
//! staging-overlay release, DDL-buffer, and GAP_FREE rollback logic all live in
//! the neutral core. These functions check BEGIN's transaction modes and
//! shape the tag / error.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{SessionId, TransactionState, lifecycle};

use super::super::super::types::sqlstate_error;
use super::super::core::NodeDbPgHandler;
use super::commit::PgwireTxnDp;
use crate::control::server::shared::txn_control::TxnModes;

impl NodeDbPgHandler {
    /// Handle BEGIN / START TRANSACTION. A refused isolation level or
    /// `DEFERRABLE` fails with SQLSTATE 0A000 before the block opens. An
    /// access mode is stored the way `SET TRANSACTION` stores it.
    pub(in crate::control::server::pgwire::handler) fn handle_begin(
        &self,
        session_id: SessionId,
        modes: &TxnModes,
    ) -> PgWireResult<Vec<Response>> {
        if let Some(message) = modes.refusal("BEGIN") {
            return Err(sqlstate_error(
                nodedb_types::error::sqlstate::FEATURE_NOT_SUPPORTED,
                &message,
            ));
        }
        match lifecycle::run_begin(&self.sessions, session_id) {
            Ok(()) => {
                if let Some(access) = modes.access {
                    self.sessions.set_parameter(
                        session_id,
                        "transaction_access_mode".into(),
                        access.parameter_value().into(),
                    );
                }
                Ok(vec![Response::Execution(Tag::new("BEGIN"))])
            }
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
        session_id: SessionId,
    ) -> PgWireResult<Vec<Response>> {
        let dp = PgwireTxnDp { handler: self };
        lifecycle::run_rollback(&self.sessions, session_id, identity, &self.state, &dp).await;
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
    pub(in crate::control::server::pgwire) async fn reclaim_open_txn(&self, session_id: SessionId) {
        if self.sessions.transaction_state(session_id) == TransactionState::Idle {
            return;
        }
        let Some(identity) = self.sessions.identity(session_id) else {
            return;
        };
        let dp = PgwireTxnDp { handler: self };
        lifecycle::run_rollback(&self.sessions, session_id, &identity, &self.state, &dp).await;
    }
}

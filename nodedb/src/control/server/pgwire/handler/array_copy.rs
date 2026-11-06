// SPDX-License-Identifier: BUSL-1.1

//! pgwire transport for `COPY <array> FROM STDIN`.
//!
//! The statement opens a COPY-in stream and an array load for the
//! connection. Each `CopyData` message streams through the shared array
//! bulk-load core, which checks its rows and dispatches each chunk they
//! fill. `CopyDone` loads the last record and the pending chunks. A
//! connection holds one incomplete record and a few chunks, whatever the
//! input size.

use std::collections::HashMap;
use std::sync::Mutex;

use futures::stream;
use nodedb_sql::ddl_ast::statement::{MiscStmt, NodedbStatement};
use pgwire::api::results::{CopyResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::mirror::source::sync::await_database_sync;
use crate::control::planner::context::{BulkLoadRoute, QueryContext};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::DdlError;
use crate::control::server::shared::ddl::neutral::collection::copy_from::{
    ArrayLoadCtx, ArrayLoader, LoadInput, TextFormat,
};
use crate::control::server::shared::session::{
    DmlTxnCtx, SessionId, SessionStore, TransactionState,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::core::NodeDbPgHandler;

/// One connection's open `COPY <array> FROM STDIN`.
pub struct ArrayCopyPending {
    identity: AuthenticatedIdentity,
    database_id: DatabaseId,
    loader: ArrayLoader,
}

/// Connection-keyed map of open array COPY loads.
#[derive(Default)]
pub struct ArrayCopyState {
    inner: Mutex<HashMap<u64, ArrayCopyPending>>,
}

impl ArrayCopyState {
    pub fn new() -> Self {
        Self::default()
    }

    fn put(&self, conn_id: u64, pending: ArrayCopyPending) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.insert(conn_id, pending);
    }

    pub fn take(&self, conn_id: u64) -> Option<ArrayCopyPending> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.remove(&conn_id)
    }

    pub fn cancel(&self, conn_id: u64) {
        let _ = self.take(conn_id);
    }
}

impl NodeDbPgHandler {
    /// The COPY-stream response for `sql` when it is a streaming COPY shape:
    /// backup, restore, or a load into an array. `None` for any other
    /// statement, which runs on the regular path.
    pub(super) async fn copy_stream_response(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> Option<PgWireResult<Response>> {
        if let Some(intent) = crate::control::backup::detect(sql) {
            return Some(self.intent_to_response(identity, session_id, intent).await);
        }
        self.array_copy_in(identity, session_id, sql).await
    }

    /// Open `COPY <array> FROM STDIN`. `None` when `sql` is not one, or
    /// names no array: that statement runs on the regular path, which
    /// answers a non-array target as before.
    async fn array_copy_in(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> Option<PgWireResult<Response>> {
        if !sql.trim_start().get(..5)?.eq_ignore_ascii_case("COPY ") {
            return None;
        }
        let Some(Ok(NodedbStatement::Misc(MiscStmt::CopyFromStdin {
            collection,
            format,
            delimiter,
            header,
        }))) = nodedb_sql::ddl_ast::parse(sql)
        else {
            return None;
        };
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(DatabaseId::DEFAULT);
        let route = QueryContext::for_state(&self.state).bulk_load_route(
            &collection,
            identity.tenant_id,
            database_id,
        );
        let Ok(BulkLoadRoute::ArrayCells(view)) = route else {
            return None;
        };
        let open = ArrayCopyOpen {
            database_id,
            array: &collection,
            columns: view.dims.len() + view.attrs.len(),
            format: TextFormat {
                format,
                delimiter,
                header,
            },
        };
        Some(self.open_array_copy(identity, session_id, open).await)
    }

    async fn open_array_copy(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        open: ArrayCopyOpen<'_>,
    ) -> PgWireResult<Response> {
        let ArrayCopyOpen {
            database_id,
            array,
            columns,
            format,
        } = open;
        self.admit_statement(identity, session_id, database_id)
            .await?;
        self.state
            .check_tenant_quota(identity.tenant_id)
            .map_err(|e| super::super::types::error_to_pg(&e))?;
        let SessionId::Connection(connection_id) = session_id else {
            return Err(user_error(
                nodedb_types::error::sqlstate::INTERNAL_ERROR,
                "COPY FROM STDIN requires a typed connection".to_owned(),
            ));
        };
        let txn_ctx = DmlTxnCtx {
            sessions: &self.sessions,
            session_id,
        };
        let ctx = ArrayLoadCtx {
            state: &self.state,
            identity,
            database_id,
            txn_ctx: &txn_ctx,
        };
        let loader = ArrayLoader::open(&ctx, array, LoadInput::Text(format))
            .map_err(|e| refused(&self.sessions, session_id, e))?;
        self.array_copy.put(
            connection_id.get(),
            ArrayCopyPending {
                identity: identity.clone(),
                database_id,
                loader,
            },
        );
        // Text format, one column per dim and attr. The client starts
        // sending rows.
        Ok(Response::CopyIn(CopyResponse::new(
            0,
            columns,
            stream::empty(),
        )))
    }
}

/// What an array COPY opens with.
struct ArrayCopyOpen<'a> {
    database_id: DatabaseId,
    array: &'a str,
    /// One per dim and attr.
    columns: usize,
    format: TextFormat,
}

/// The connection's session store, session and array COPY state.
pub(super) struct ArrayCopyConn<'a> {
    pub state: &'a SharedState,
    pub sessions: &'a SessionStore,
    pub session_id: SessionId,
    pub copies: &'a ArrayCopyState,
    pub conn_id: u64,
}

/// Load one `CopyData` message of the connection's open array COPY.
/// `Ok(false)` when no array COPY is open. A refusal drops the load.
pub(super) async fn feed_array_copy(conn: &ArrayCopyConn<'_>, data: &[u8]) -> PgWireResult<bool> {
    let Some(mut pending) = conn.copies.take(conn.conn_id) else {
        return Ok(false);
    };
    let txn_ctx = DmlTxnCtx {
        sessions: conn.sessions,
        session_id: conn.session_id,
    };
    let ctx = ArrayLoadCtx {
        state: conn.state,
        identity: &pending.identity,
        database_id: pending.database_id,
        txn_ctx: &txn_ctx,
    };
    let _request = conn.state.tenant_request_guard(pending.identity.tenant_id);
    match pending.loader.feed_text(&ctx, data).await {
        Ok(()) => {
            conn.copies.put(conn.conn_id, pending);
            Ok(true)
        }
        Err(e) => Err(refused(conn.sessions, conn.session_id, e)),
    }
}

/// Finish an array COPY: load its last record and pending chunks. Returns
/// the rows loaded, once every sync mirror holds them. A COPY in a block
/// waits at its COMMIT.
pub(super) async fn finish_array_copy(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    pending: ArrayCopyPending,
) -> PgWireResult<u64> {
    let ArrayCopyPending {
        identity,
        database_id,
        loader,
    } = pending;
    let txn_ctx = DmlTxnCtx {
        sessions,
        session_id,
    };
    let ctx = ArrayLoadCtx {
        state,
        identity: &identity,
        database_id,
        txn_ctx: &txn_ctx,
    };
    let _request = state.tenant_request_guard(identity.tenant_id);
    let rows = loader
        .finish(&ctx)
        .await
        .map_err(|e| refused(sessions, session_id, e))?;
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        await_database_sync(state, database_id)
            .await
            .map_err(|error| {
                let (_, code, message) =
                    crate::control::server::pgwire::types::error_to_sqlstate(&error);
                user_error(code, message)
            })?;
    }
    Ok(rows)
}

/// The pgwire error of a refused load. A refusal inside a transaction block
/// aborts the block, as a failed statement does, so the staged chunks roll
/// back.
fn refused(sessions: &SessionStore, session_id: SessionId, e: DdlError) -> PgWireError {
    if sessions.transaction_state(session_id) == TransactionState::InBlock {
        sessions.fail_transaction(session_id);
    }
    user_error(&e.sqlstate, e.message)
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

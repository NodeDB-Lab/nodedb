// SPDX-License-Identifier: BUSL-1.1

//! The transaction a `COPY ... FROM` row load runs in.
//!
//! A `COPY` is one statement, so it is atomic, as in PostgreSQL: a refused
//! row fails the load and stores no row of it. Inside a transaction block
//! every row stages into the client's transaction. Outside one the load runs
//! in an implicit transaction that commits when every row is written.
//!
//! A COMMIT that aborts on a serialization conflict runs the load again on a
//! new transaction, until the statement deadline. The file is already in
//! memory, so a new attempt reads the same rows. A load whose rows fire a
//! joined trigger body answers its first conflict: a body's effects outside
//! the transaction must not repeat.

use nodedb_mem::ScopedMemory;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::conn_scope::scoped_system_txn;
use crate::control::state::SharedState;
use crate::control::trigger::statement_txn::{
    ImplicitAttempt, ImplicitRetry, begin_implicit, finish_implicit, in_block,
};

use super::csv_import::import_csv;
use super::entry::RowFormat;
use super::import_ctx::ImportCtx;
use super::json_import::{import_json_array, import_ndjson};

/// One row load: the file's bytes, their format, and who loads them where.
pub(super) struct RowLoad<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: nodedb_types::TenantId,
    pub database_id: nodedb_types::DatabaseId,
    /// The session the `COPY` arrived on.
    pub txn_ctx: &'a DmlTxnCtx<'a>,
    pub memory: &'a ScopedMemory,
    pub format: RowFormat,
    pub collection: &'a str,
    pub path: &'a str,
    pub bytes: &'a [u8],
    /// The rows fire a BEFORE, INSTEAD OF or SYNC AFTER body.
    pub fires_joined_body: bool,
}

/// Load `load`'s rows in the load's transaction and return the rows written.
pub(super) async fn load_rows_in_txn(load: RowLoad<'_>) -> Result<usize, DdlError> {
    if in_block(load.txn_ctx) {
        // Boxed: the import's future is deep, and every caller's future
        // holds this one, so its layout would overflow the query depth.
        return Box::pin(import_rows(&load, load.txn_ctx)).await;
    }
    let mut pacing = ImplicitRetry::new(load.state, !load.fires_joined_body);
    loop {
        match scoped_system_txn(load_attempt(&load)).await {
            ImplicitAttempt::Committed(rows) => return Ok(rows),
            ImplicitAttempt::Failed(error) => return Err(error),
            ImplicitAttempt::Conflict(conflict) => {
                if !pacing.pause().await {
                    return Err(DdlError::from(crate::Error::from(conflict)));
                }
            }
        }
    }
}

/// One attempt: open an implicit transaction, load every row into it, and
/// commit it, or roll it back when a row is refused.
async fn load_attempt(load: &RowLoad<'_>) -> ImplicitAttempt<usize, DdlError> {
    let txn = match begin_implicit(load.state, load.identity) {
        Ok(txn) => txn,
        Err(error) => return ImplicitAttempt::Failed(DdlError::from(error)),
    };
    let result = match txn.txn_ctx() {
        // Boxed for the same reason as the block branch above.
        Ok(implicit_ctx) => Box::pin(import_rows(load, &implicit_ctx)).await,
        Err(error) => Err(DdlError::from(error)),
    };
    finish_implicit(txn, result, DdlError::from).await
}

/// Write every row of `load` on `txn_ctx`.
async fn import_rows(load: &RowLoad<'_>, txn_ctx: &DmlTxnCtx<'_>) -> Result<usize, DdlError> {
    let import_ctx = ImportCtx {
        state: load.state,
        identity: load.identity,
        tenant_id: load.tenant_id,
        database_id: load.database_id,
        txn_ctx,
        memory: load.memory,
    };
    let (collection, path, bytes) = (load.collection, load.path, load.bytes);
    match load.format {
        RowFormat::Ndjson => import_ndjson(&import_ctx, collection, path, bytes).await,
        RowFormat::JsonArray => import_json_array(&import_ctx, collection, path, bytes).await,
        RowFormat::Csv(csv) => import_csv(&import_ctx, collection, path, bytes, csv).await,
    }
}

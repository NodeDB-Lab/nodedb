// SPDX-License-Identifier: Apache-2.0

//! The remote whole-document replace.
//!
//! The replace is a `DELETE` then an `INSERT` of the new field set. An insert
//! merged over the old row would keep fields the new document dropped.
//!
//! The pair is atomic in both connection states:
//! - Inside the caller's transaction block, it runs under a savepoint. The put
//!   never commits or rolls back the caller's block.
//! - Outside a block, it runs in its own `BEGIN` / `COMMIT`.
//!
//! The driver does not expose the server's transaction status. The put opens
//! its savepoint first. The server refuses a savepoint outside a block with
//! SQLSTATE `25P01`, and that refusal selects the standalone path.
//!
//! A failed put inside a block rolls back to its savepoint and releases it.
//! The caller's block stays usable and keeps every write made before the put.
//! A put into an already aborted block is refused with SQLSTATE `25P02`, and
//! the block stays aborted.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

use crate::document_identity::check_identity_field;
use crate::sql_escape::quote_identifier;

use super::core::{NodeDbRemote, pg_error_detail};
use super::document_bind::{DocumentInsert, document_insert};

/// The savepoint a put opens inside the caller's transaction block.
const PUT_SAVEPOINT: &str = "nodedb_document_put";

/// What the put opened around its statements, and so what closes them.
enum PutScope {
    /// A savepoint inside the caller's transaction block.
    Savepoint,
    /// The put's own transaction block.
    Transaction,
}

/// The document a put writes, for error messages.
struct PutTarget<'a> {
    collection: &'a str,
    id: &'a str,
}

impl PutTarget<'_> {
    fn error(&self, step: &str, e: &tokio_postgres::Error) -> NodeDbError {
        NodeDbError::storage(format!(
            "document_put '{}'/'{}' {step} failed: {}",
            self.collection,
            self.id,
            pg_error_detail(e)
        ))
    }

    fn undo_error(
        &self,
        cause: &NodeDbError,
        step: &str,
        e: &tokio_postgres::Error,
    ) -> NodeDbError {
        NodeDbError::storage(format!(
            "document_put '{}'/'{}' failed: {cause}; the {step} that undoes it also failed: {}",
            self.collection,
            self.id,
            pg_error_detail(e)
        ))
    }
}

impl NodeDbRemote {
    /// Replace the document's whole field set, creating it when absent.
    pub(super) async fn document_put_impl(
        &self,
        collection: &str,
        doc: Document,
    ) -> NodeDbResult<()> {
        check_identity_field(collection, &doc.id, &doc.fields)?;
        let insert = document_insert(collection, &doc)?;
        let target = PutTarget {
            collection,
            id: &doc.id,
        };
        let client = self.client.lock().await;
        let scope = open_scope(&client, &target).await?;
        let written = replace(&client, &target, &insert).await;
        close_scope(&client, &target, scope, written).await
    }
}

/// Open a savepoint inside the caller's block, else the put's own block.
async fn open_scope(client: &Client, target: &PutTarget<'_>) -> NodeDbResult<PutScope> {
    match client
        .batch_execute(&format!("SAVEPOINT {PUT_SAVEPOINT}"))
        .await
    {
        Ok(()) => Ok(PutScope::Savepoint),
        Err(e) if e.code() == Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION) => {
            client
                .batch_execute("BEGIN")
                .await
                .map_err(|e| target.error("begin", &e))?;
            Ok(PutScope::Transaction)
        }
        Err(e) => Err(target.error("savepoint", &e)),
    }
}

/// Delete the old row and insert the new field set.
async fn replace(
    client: &Client,
    target: &PutTarget<'_>,
    insert: &DocumentInsert,
) -> NodeDbResult<()> {
    let delete_sql = format!(
        "DELETE FROM {} WHERE id = $1",
        quote_identifier(target.collection)
    );
    let delete = client
        .prepare_typed(&delete_sql, &[Type::TEXT])
        .await
        .map_err(|e| target.error("delete prepare", &e))?;
    client
        .execute(&delete, &[&target.id])
        .await
        .map_err(|e| target.error("delete", &e))?;
    let statement = client
        .prepare_typed(&insert.sql, &insert.types())
        .await
        .map_err(|e| target.error("insert prepare", &e))?;
    client
        .execute(&statement, &insert.values())
        .await
        .map_err(|e| target.error("insert", &e))?;
    Ok(())
}

/// Keep the write on success, undo it on error.
async fn close_scope(
    client: &Client,
    target: &PutTarget<'_>,
    scope: PutScope,
    written: NodeDbResult<()>,
) -> NodeDbResult<()> {
    match (scope, written) {
        (PutScope::Savepoint, Ok(())) => client
            .batch_execute(&format!("RELEASE SAVEPOINT {PUT_SAVEPOINT}"))
            .await
            .map_err(|e| target.error("release savepoint", &e)),
        (PutScope::Transaction, Ok(())) => client
            .batch_execute("COMMIT")
            .await
            .map_err(|e| target.error("commit", &e)),
        (PutScope::Savepoint, Err(cause)) => {
            let undo = format!(
                "ROLLBACK TO SAVEPOINT {PUT_SAVEPOINT}; RELEASE SAVEPOINT {PUT_SAVEPOINT}"
            );
            match client.batch_execute(&undo).await {
                Ok(()) => Err(cause),
                Err(e) => Err(target.undo_error(&cause, "rollback to savepoint", &e)),
            }
        }
        (PutScope::Transaction, Err(cause)) => match client.batch_execute("ROLLBACK").await {
            Ok(()) => Err(cause),
            Err(e) => Err(target.undo_error(&cause, "rollback", &e)),
        },
    }
}

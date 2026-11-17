// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for full-text index records.
//!
//! A full-text index record names the fields a write indexes. The Data Plane
//! reads that field list from the collection's registered config, so a put or
//! delete of a full-text record re-registers the collection on every local
//! core.

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::apply_outcome::register_outcome;

/// Re-register `collection` on every local Data Plane core, so its config
/// carries the text fields the catalog holds after this apply.
///
/// An error goes to [`register_outcome`], as a `PutCollection` register
/// error does: it stops the apply batch, and the re-delivered entry
/// re-registers again.
pub async fn reregister(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<()> {
    let result = reregister_text_fields(shared, database_id, tenant_id, collection).await;
    register_outcome(result, collection)
}

/// Re-register `collection` from its catalog row.
///
/// A collection the catalog does not hold, or holds soft-dropped, registers
/// nothing. Its own put or undrop registers it with every record it has.
///
/// `Err` means the catalog did not read or a core did not acknowledge the
/// Register.
async fn reregister_text_fields(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<()> {
    let stored = shared.credentials.catalog().get_collection(
        DatabaseId::new(database_id),
        tenant_id,
        collection,
    )?;
    match stored {
        Some(stored) if stored.is_active => {
            crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored(
                shared, &stored,
            )
            .await
        }
        Some(_) | None => Ok(()),
    }
}

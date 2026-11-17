// SPDX-License-Identifier: BUSL-1.1

//! Proposer helpers for the index identity registry.
//!
//! Every `CREATE [<kind>] INDEX` registers its index here and every drop
//! removes it, through the metadata raft group so all nodes list and resolve
//! the same set.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::{IndexKind, IndexRecordState, StoredIndexRecord};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::result::DdlError;

/// The identity of one index, as its creating statement declared it.
pub struct IndexRegistration<'a> {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub name: &'a str,
    pub kind: IndexKind,
    pub collection: &'a str,
    pub fields: Vec<String>,
    /// `Building` for a full-text index whose rebuild has not run yet.
    pub state: IndexRecordState,
}

/// Register an index so it is listable and droppable by name.
pub async fn propose_index_record(
    state: &SharedState,
    registration: &IndexRegistration<'_>,
) -> Result<(), DdlError> {
    propose_record(state, record_of(registration)).await
}

/// Register a full-text index with the analyzer and fuzzy default its
/// statement names. `None` means the statement names none.
pub async fn propose_text_index_record(
    state: &SharedState,
    registration: &IndexRegistration<'_>,
    analyzer: Option<String>,
    fuzzy_default: Option<bool>,
) -> Result<(), DdlError> {
    let mut record = record_of(registration);
    record.analyzer = analyzer;
    record.fuzzy_default = fuzzy_default;
    propose_record(state, record).await
}

/// The record `registration` declares, with no analyzer and no fuzzy
/// default.
fn record_of(registration: &IndexRegistration<'_>) -> StoredIndexRecord {
    StoredIndexRecord {
        database_id: registration.database_id.as_u64(),
        tenant_id: registration.tenant_id.as_u64(),
        name: registration.name.to_string(),
        kind: registration.kind,
        collection: registration.collection.to_string(),
        fields: registration.fields.clone(),
        is_active: true,
        state: registration.state,
        analyzer: None,
        fuzzy_default: None,
    }
}

/// Flip a `Building` index record to `Ready` on a fresh read.
///
/// A record the catalog no longer holds, or holds under another kind, is
/// left alone: a later statement dropped or replaced it.
pub async fn propose_index_ready(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    name: &str,
    kind: IndexKind,
) -> Result<(), DdlError> {
    let current = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), name)
        .map_err(|e| DdlError::from_error_in_context("read index registry", &e))?;
    match current {
        Some(mut record) if record.kind == kind && record.state == IndexRecordState::Building => {
            record.state = IndexRecordState::Ready;
            propose_record(state, record).await
        }
        Some(_) | None => Ok(()),
    }
}

/// The live index record that holds `name`, for a CREATE of a new index of
/// any kind. `None` when the name is free.
///
/// A record a `DROP INDEX` earlier in this transaction marked `Dropping`
/// frees the name. The CREATE replaces it, and the drop's deferred effect
/// finishes the old index at COMMIT. A committed `Dropping` record holds
/// the name until a `DROP INDEX` finishes its drop: the CREATE fails with
/// 55000. `command` names the statement in every error.
pub fn index_name_holder(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    name: &str,
    command: &str,
) -> Result<Option<StoredIndexRecord>, DdlError> {
    let read_registry = |e: crate::Error| {
        DdlError::from_error_in_context(&format!("{command}: read index registry"), &e)
    };
    let catalog = state.credentials.catalog();
    let Some(holder) = catalog
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), name)
        .map_err(read_registry)?
    else {
        return Ok(None);
    };
    if holder.state != IndexRecordState::Dropping {
        return Ok(Some(holder));
    }
    let committed = catalog
        .get_committed_index_record(database_id.as_u64(), tenant_id.as_u64(), name)
        .map_err(read_registry)?;
    if committed.is_some_and(|c| c.state == IndexRecordState::Dropping) {
        return Err(DdlError::new(
            "55000",
            format!(
                "{command}: index '{name}' on '{}' is still being dropped; run DROP INDEX \
                 {name} to finish the drop, then create it again",
                holder.collection
            ),
        ));
    }
    Ok(None)
}

/// Mark `record` `Dropping`, so it declares no field while its drop runs.
///
/// Inside an explicit transaction the entry is buffered and lands at
/// COMMIT with the rest of the transaction.
pub async fn propose_index_dropping(
    state: &SharedState,
    record: &StoredIndexRecord,
) -> Result<(), DdlError> {
    let mut dropping = record.clone();
    dropping.state = IndexRecordState::Dropping;
    propose_record(state, dropping).await
}

/// Propose one index record through the metadata group.
async fn propose_record(state: &SharedState, record: StoredIndexRecord) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutIndexRecord(Box::new(record));
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(())
}

/// Remove an index's identity record.
pub async fn propose_delete_index_record(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    name: &str,
    collection: &str,
    kind: IndexKind,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteIndexRecord {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        collection: collection.to_string(),
        kind,
    };
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    Ok(())
}

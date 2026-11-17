// SPDX-License-Identifier: BUSL-1.1

//! Build a full-text index from the rows a collection already stores.
//!
//! `CREATE SEARCH INDEX` registers its record `Building`, then runs
//! [`build_text_index`]. Every node rebuilds the collection's text index
//! from its stored document versions and sync frames, off its Data Plane
//! core, and the record flips to `Ready`.
//! `DROP INDEX` of a full-text index runs [`rebuild_text_index`] once the
//! record is `Dropping`, so the dropped fields leave the index before the
//! record does.
//!
//! Inside an explicit transaction both wait for COMMIT, after the buffered
//! record entries land. A ROLLBACK leaves the records as they were and no
//! rebuilt text. A record left `Building` or `Dropping` by an error or a
//! crash is finished by the boot reconcile.

use crate::control::security::catalog::{IndexKind, IndexRecordState};
use crate::control::server::shared::ddl::index_registry::propose_index_ready;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::result::DdlError;
pub(crate) use super::text_rebuild_drive::RebuildScope;
use super::text_rebuild_drive::{RebuildStep, drive_text_rebuild};

/// Rebuild `collection`'s text index on every node, then flip the
/// full-text record `index_name` to `Ready`.
///
/// A record the catalog no longer holds, or holds in a state other than
/// `Building`, is skipped. A later statement dropped it, and that drop runs
/// its own rebuild. A rebuild error leaves the record `Building`. The boot
/// reconcile runs the build again, and a `DROP INDEX` removes the record.
pub(crate) async fn build_text_index(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    index_name: &str,
) -> Result<(), DdlError> {
    let record = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), index_name)
        .map_err(|e| DdlError::from_error_in_context("read index registry", &e))?;
    let current = record.is_some_and(|r| {
        r.kind == IndexKind::FullText
            && r.collection == collection
            && r.state == IndexRecordState::Building
    });
    if !current {
        return Ok(());
    }
    rebuild_text_index(
        state,
        tenant_id,
        database_id,
        collection,
        RebuildScope::EveryNode,
    )
    .await?;
    propose_index_ready(
        state,
        database_id,
        tenant_id,
        index_name,
        IndexKind::FullText,
    )
    .await
}

/// Rebuild `collection`'s text index on the nodes `scope` covers, from the
/// fields its full-text records declare. A `Dropping` record declares none.
/// With no declared field left, each node only drops the collection's text
/// index. The op carries the field list, and each core installs it before
/// its rebuild starts.
///
/// Each node rebuilds off its Data Plane core and keeps serving meanwhile.
/// The call returns once every covered node cut its rebuild over. See
/// `text_rebuild_drive` for how the wait is bounded.
///
/// A collection the catalog no longer holds, or holds soft-dropped, is
/// skipped. Its drop reclaims the text index, and its undrop re-registers
/// it.
pub(crate) async fn rebuild_text_index(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    scope: RebuildScope,
) -> Result<(), DdlError> {
    let in_context = |e: &crate::Error| {
        DdlError::from_error_in_context(&format!("full-text index rebuild on '{collection}'"), e)
    };
    let Some(coll) = state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), collection)
        .map_err(|e| in_context(&e))?
        .filter(|coll| coll.is_active)
    else {
        return Ok(());
    };

    // Register the collection on this node first, so a write that lands
    // after the rebuild indexes the same fields. The post-apply register of
    // a transaction's buffered record runs asynchronously.
    super::super::dispatch_register_from_stored(state, &coll)
        .await
        .map_err(|e| in_context(&e))?;

    drive_text_rebuild(
        state,
        RebuildStep {
            tenant_id,
            database_id,
            collection,
        },
        scope,
    )
    .await
}

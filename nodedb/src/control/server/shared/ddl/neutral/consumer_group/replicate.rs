// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the consumer-group DDL handlers.
//!
//! Every mutation of `_system.consumer_groups` proposes a `CatalogEntry`, so
//! each node writes the row and installs it in its own `GroupRegistry`. A group
//! created on one node resolves on all.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::{ConsumerGroupDef, OffsetCommit, PartitionOffset};
use crate::types::DatabaseId;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply_async;

/// Propose the group definition. The leader reports the duplicate before
/// proposing, so apply is a create-only write that never rejects.
pub(super) async fn propose_create(
    state: &SharedState,
    def: &ConsumerGroupDef,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutConsumerGroupIfAbsent(Box::new(def.clone()));
    propose_and_apply_async(state, &entry).await
}

/// Propose removal of the group row, its registration, and its durable offsets
/// on every node.
pub(super) async fn propose_delete(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
    name: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::DeleteConsumerGroup {
        database_id: database_id.as_u64(),
        tenant_id,
        stream_name: stream_name.to_string(),
        name: name.to_string(),
        target_hlc: nodedb_types::Hlc::ZERO,
    };
    propose_and_apply_async(state, &entry).await
}

/// Propose raising the group's committed offsets. Every node raises its own
/// offset store on apply, so a consumer resumes from this commit on any node.
///
/// Offsets apply as a monotonic max and stamp no descriptor version, so the
/// commit takes no DDL preparation lease. Inside a transaction block it is
/// buffered until COMMIT, like every other DDL the transaction issues.
pub(super) async fn propose_commit_offsets(
    state: &SharedState,
    def: &ConsumerGroupDef,
    offsets: Vec<PartitionOffset>,
) -> Result<(), DdlError> {
    let commit = OffsetCommit {
        database_id: def.database_id,
        tenant_id: def.tenant_id,
        stream_name: def.stream_name.clone(),
        group_name: def.name.clone(),
        group_hlc: def.modification_hlc,
        offsets,
    };
    if crate::control::server::shared::session::ddl_buffer::try_buffer(
        CatalogEntry::CommitConsumerOffsets(Box::new(commit.clone())),
    ) {
        return Ok(());
    }
    crate::control::metadata_proposer::propose_cursor_commit(state, commit)
        .await
        .map(|_| ())
        .map_err(|e| DdlError::from_error_in_context("consumer offset commit failed", &e))
}

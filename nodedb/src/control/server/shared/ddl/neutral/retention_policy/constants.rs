// SPDX-License-Identifier: BUSL-1.1

//! Shared constants for retention policy DDL.

/// CRDT collection name for retention policy sync between Origin and Lite.
pub(super) const RETENTION_POLICIES_CRDT_COLLECTION: &str =
    crate::event::crdt_sync::types::DefinitionFamily::RetentionPolicies.collection();

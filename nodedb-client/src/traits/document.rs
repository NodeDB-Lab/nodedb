// SPDX-License-Identifier: Apache-2.0

//! Supporting types for document and collection-lifecycle operations.

use std::sync::Arc;

/// Event passed to `NodeDb::on_collection_purged` handlers.
///
/// Emitted on a sync client once it dropped the local state of a collection
/// Origin purged, so application code can flush UI caches, drop derived
/// indexes, etc. A purge the client missed while offline fires it at the
/// next connect. A handler must not block: it runs on the sync client's
/// receive loop.
#[derive(Debug, Clone)]
pub struct CollectionPurgedEvent {
    pub tenant_id: u64,
    pub name: String,
    /// Origin's metadata log position of the purge. Delivery is
    /// at-least-once, and a repeated notice of one purge carries the same
    /// position: `(name, purge_index)` identifies the purge.
    pub purge_index: u64,
}

/// Handler registered via `NodeDb::on_collection_purged`. Fn-ref
/// (not FnMut) so the same handler can fire from multiple threads
/// without interior mutability ceremony at every call site.
pub type CollectionPurgedHandler = Arc<dyn Fn(CollectionPurgedEvent) + Send + Sync + 'static>;

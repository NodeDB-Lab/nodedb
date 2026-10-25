// SPDX-License-Identifier: BUSL-1.1

//! Collection-schema announce helper.
//!
//! Builds the `CollectionSchema` frame that must precede the first shape
//! snapshot or delta for a collection on a sync session. The emit side of
//! the announce-precedes-data invariant: any peer that receives a delta or
//! snapshot for a collection has already received its descriptor.

use nodedb_types::{
    DatabaseId,
    sync::wire::{CollectionDescriptor, CollectionSchemaSyncMsg},
};

use tracing::warn;

use crate::control::state::SharedState;

use super::super::session::SyncSession;
use super::super::wire::{SyncFrame, SyncMessageType};

/// Build a `CollectionSchema` announce frame for `collection` if it has not
/// already been announced to the peer this session.
///
/// Returns `None` when the collection was already announced (idempotent) or
/// cannot be resolved in the catalog (logged; the caller then proceeds to
/// send the data frame anyway so delivery never regresses). The caller is
/// responsible for marking the collection announced only after the returned
/// frame has been sent successfully.
pub(in crate::control::server::sync) fn build_collection_schema_frame(
    shared: &SharedState,
    session: &SyncSession,
    tenant_id: u64,
    database_id: DatabaseId,
    collection: &str,
) -> Option<SyncFrame> {
    if session.announced_collections.contains(collection) {
        return None;
    }

    let stored = shared
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, collection)
        .ok()
        .flatten();

    let Some(stored) = stored else {
        warn!(
            session = %session.session_id,
            collection,
            tenant_id,
            "sync: collection not found in catalog; cannot announce schema before data"
        );
        return None;
    };

    let msg = CollectionSchemaSyncMsg {
        descriptor: CollectionDescriptor::from(&stored),
        // The collection's incarnation. A client states it on every delta it
        // pushes (`DeltaPushMsg::collection_incarnation`), and Origin refuses
        // a delta for an incarnation a drop and a same-name create replaced.
        creation_hlc: stored.incarnation,
    };

    SyncFrame::new_msgpack(SyncMessageType::CollectionSchema, &msg)
}

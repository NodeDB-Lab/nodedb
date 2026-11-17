// SPDX-License-Identifier: BUSL-1.1

//! Server-side resync of one collection after its pushes were dropped.
//!
//! One snapshot covers every document shape the session holds on the
//! collection: the rows the session may read that any of the shapes match.
//! The client replaces the collection's rows with it, so a row deleted, or
//! moved out of the session's view, while its push was dropped leaves the
//! client too.
//!
//! The snapshot is a read on the session's behalf, so it passes the gates a
//! shape snapshot passes: the read grant, blacklist and account status, the
//! tenant quota, then the RLS-scoped scan and column redaction.

use nodedb_types::sync::shape::ShapeType;
use nodedb_types::sync::wire::{CollectionResyncMsg, SyncFrame, SyncMessageType};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::sync::session::SyncSession;
use crate::control::server::sync::shape::ShapeScope;
use crate::control::state::SharedState;

use super::snapshot::{DocumentSnapshot, document_snapshot};
use super::subscribe::subscription_admission_scope;

/// A collection resync ready to send.
#[derive(Debug)]
pub(in crate::control::server::sync) struct CollectionResync {
    pub frame: SyncFrame,
    /// This node's WAL position when the read began, as a shape snapshot
    /// reports it. The client re-bases its shapes' LSN tracking at it. The
    /// server does not compare it with a push's LSN: the session cuts off
    /// stale pushes by queue position.
    pub snapshot_lsn: u64,
}

/// Why no collection resync went out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::control::server::sync) enum CollectionResyncFailure {
    /// The session holds no document shape on the collection: there is
    /// nothing to resync.
    NoShapes,
    /// The principal may not read the collection now. Its pushes are
    /// refused at push time too, so there is nothing to resync.
    Refused(String),
    /// The snapshot did not complete. The collection stays marked.
    Failed(String),
}

/// Take one snapshot of `collection` covering every document shape the
/// session holds on it.
pub(in crate::control::server::sync) async fn collection_resync(
    shared: &SharedState,
    session: &SyncSession,
    collection: &str,
) -> Result<CollectionResync, CollectionResyncFailure> {
    let Some(identity) = session.identity.as_ref() else {
        return Err(CollectionResyncFailure::Refused(
            "the session has no established identity".into(),
        ));
    };
    let database_id = session.database_id();
    let tenant_id = identity.tenant_id;

    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Read,
        &shared.permissions,
        &shared.roles,
        &ArcAuditEmitter(std::sync::Arc::clone(&shared.audit)),
    )
    .map_err(|error| CollectionResyncFailure::Refused(crate::Error::from(error).to_string()))?;
    let request = subscription_admission_scope(shared, identity, database_id, session)
        .map_err(|error| CollectionResyncFailure::Failed(error.to_string()))?;
    crate::control::server::session_auth::check_blacklist_and_status(shared, &request)
        .map_err(|error| CollectionResyncFailure::Refused(error.to_string()))?;
    shared
        .check_tenant_quota(tenant_id)
        .map_err(|error| CollectionResyncFailure::Failed(error.to_string()))?;

    let scope = ShapeScope {
        tenant_id: tenant_id.as_u64(),
        database_id,
    };
    let mut shape_ids = Vec::new();
    let mut predicates: Vec<Vec<u8>> = Vec::new();
    for shape_id in shared
        .shape_registry
        .shapes_for_session(&session.session_id)
    {
        let Some(shape) =
            shared
                .shape_registry
                .get_shape(&session.session_id, scope, shape_id.as_str())
        else {
            continue;
        };
        if let ShapeType::Document {
            collection: shape_collection,
            predicate,
        } = &shape.shape_type
            && shape_collection == collection
        {
            shape_ids.push(shape.shape_id.clone());
            predicates.push(predicate.clone());
        }
    }
    if shape_ids.is_empty() {
        return Err(CollectionResyncFailure::NoShapes);
    }
    let predicate_refs: Vec<&[u8]> = predicates.iter().map(Vec::as_slice).collect();

    // Read before the scan, as a shape snapshot reads it.
    let snapshot_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);
    let _request = shared.tenant_request_guard(tenant_id);
    let snapshot = document_snapshot(DocumentSnapshot {
        shared,
        shape_id: collection,
        collection,
        predicates: &predicate_refs,
        identity,
        database_id,
        peer_addr: &session.device_metadata.remote_addr,
    })
    .await
    .map_err(|failure| CollectionResyncFailure::Failed(failure.detail))?;

    let msg = CollectionResyncMsg {
        collection: collection.to_string(),
        shape_ids,
        data: snapshot.data,
        snapshot_lsn,
        doc_count: snapshot.doc_count,
    };
    let frame =
        SyncFrame::try_encode(SyncMessageType::CollectionResync, &msg).ok_or_else(|| {
            CollectionResyncFailure::Failed("the collection resync frame does not encode".into())
        })?;
    Ok(CollectionResync {
        frame,
        snapshot_lsn,
    })
}

// SPDX-License-Identifier: BUSL-1.1

//! Hold each Loro peer id to one producer, before its delta reaches the merge.
//!
//! Downstream of the merge there is nothing left to decide with: a delta whose
//! `(peer, counter)` range the document already covers is trimmed away, and
//! that is indistinguishable from an honest replay no matter how carefully the
//! apply reports it. Two replicas sharing a peer id therefore lose writes with
//! every counter green.
//!
//! Here the two *are* distinguishable, because the server knows which durable
//! producer each session belongs to. A peer id is claimed by the first producer
//! to use it in a collection and refused to any other — so the collision is
//! answered with a rejection the client can act on instead of a discarded write
//! it never hears about.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::control::security::catalog::sync_producer::PeerBindingKey;
use crate::control::state::SharedState;
use crate::control::sync_producer::PeerBindOutcome;
use crate::types::{DatabaseId, TenantId};

/// What the peer-id binding decided about one delta.
pub(super) enum PeerIdentity {
    /// The session's producer owns this peer id.
    Owned,
    /// There is no producer identity to bind to, so the delta proceeds
    /// unbound.
    ///
    /// This is the honest limit of the mechanism rather than a silent skip: an
    /// unfenced client (`producer_id == 0`) has no durable identity for a peer
    /// id to be held against, so a collision between two such clients still
    /// reaches the merge. What catches it there is the zero-import accounting
    /// on the apply — a refusal it cannot make, but a fact it can report.
    Unbound,
    /// Another producer owns this peer id. Writing under it will have the
    /// merge discard the delta.
    Collision { owner_producer_id: u64 },
}

pub(super) struct PeerIdentityRequest<'a> {
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
    pub(super) collection: &'a str,
    pub(super) peer_id: u64,
    pub(super) producer_id: u64,
}

/// Claim the delta's peer id for this session's producer, or report the
/// producer that already holds it.
///
/// A newly created binding is replicated before the delta is admitted. Trusting
/// the local claim alone will let two nodes each admit writes under the same
/// peer id during the proposal window — the collision this exists to prevent,
/// moved from between two clients to between two nodes.
pub(super) async fn admit_peer_identity(
    shared: &SharedState,
    request: PeerIdentityRequest<'_>,
) -> crate::Result<PeerIdentity> {
    let PeerIdentityRequest {
        database_id,
        tenant_id,
        collection,
        peer_id,
        producer_id,
    } = request;
    // `producer_id == 0` marks an unfenced session. `peer_id == 0` marks an
    // unset peer. Neither names an identity to bind.
    if producer_id == 0 || peer_id == 0 {
        return Ok(PeerIdentity::Unbound);
    }
    let Some(registry) = shared.producer_registry.as_deref() else {
        return Ok(PeerIdentity::Unbound);
    };

    let key = PeerBindingKey::new(
        database_id.as_u64(),
        tenant_id.as_u64(),
        collection,
        peer_id,
    );
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(i64::MAX);

    if let PeerBindOutcome::Conflict { owner_producer_id } =
        registry.bind_peer(&key, producer_id, now_ms)?
    {
        return Ok(PeerIdentity::Collision { owner_producer_id });
    }

    if registry.peer_binding_converged(&key) {
        return Ok(PeerIdentity::Owned);
    }

    // Replicate, then re-read: the apply is lowest-producer-id-wins, so a node
    // that lost a race it cannot see locally learns the real owner only once
    // the entry lands.
    crate::control::metadata_proposer::propose_sync_peer_bind(shared, &key, producer_id, now_ms)
        .await?;
    registry.mark_peer_binding_converged(&key);
    match registry.peer_owner(&key)? {
        Some(owner) if owner == producer_id => Ok(PeerIdentity::Owned),
        Some(owner_producer_id) => Ok(PeerIdentity::Collision { owner_producer_id }),
        // The row this call wrote cannot be absent; treating a missing one
        // as owned will admit a write under a peer id nothing claims.
        None => Err(crate::Error::Internal {
            detail: format!(
                "peer binding for {collection}/{peer_id} vanished between claim and read"
            ),
        }),
    }
}

/// The human-readable refusal a colliding client receives.
///
/// It names the remedy rather than the mechanism: the client cannot resolve a
/// producer id, but it can generate a fresh peer id and resync.
pub(super) fn peer_collision_reason(
    collection: &str,
    peer_id: u64,
    owner_producer_id: u64,
) -> String {
    warn!(
        %collection,
        peer_id,
        owner_producer_id,
        "sync: delta refused — its Loro peer id belongs to another producer; \
         writing under it would have the CRDT merge discard the delta"
    );
    format!(
        "PEER_ID_COLLISION: peer id {peer_id} on collection '{collection}' is already \
         owned by another replica; generate a new peer id and resync — writes sent \
         under a shared peer id are discarded by the CRDT merge"
    )
}

// SPDX-License-Identifier: BUSL-1.1

//! The one place a key's surrogate is minted: the leader of the key's
//! collection home vShard.
//!
//! A document row lives on its collection's home vShard, and a graph edge
//! binds each endpoint on the endpoint's key vShard. Both obtain the key's
//! surrogate here, so every owner holds the one value:
//!
//! 1. A binding the home leader's catalog holds is the answer.
//! 2. Otherwise the leader draws a fresh value and proposes a
//!    `ReplicatedWrite::SurrogateBind` for the key to the home vShard's group.
//!    Every replica binds it first-wins in log order, the leader included. A
//!    document write already in the log for the same key wins over it on every
//!    replica alike.
//! 3. The leader's own apply answers each key's winner in the entry's applied
//!    payload, and the leader answers that.
//!
//! The bind never enters the leader's catalog outside the log, so a replica
//! and the leader cannot disagree, and the binding survives a change of
//! leader. Without a data-group Raft (a single node) the local catalog is the
//! home, and binds directly.

use nodedb_types::{CollectionKey, Surrogate};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, propose_replicated_entry};
use crate::types::{TenantId, VShardId};

/// The surrogate of `(collection, pk)`, minted and bound on `home`'s group
/// when the key has none. Runs on the leader of `home`, the key's collection
/// home vShard.
pub(crate) async fn assign_at_home(
    state: &SharedState,
    home: VShardId,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pk: &[u8],
) -> crate::Result<Surrogate> {
    let mut bound = assign_many_at_home(state, home, collection, tenant_id, &[pk]).await?;
    bound.pop().ok_or_else(|| crate::Error::Internal {
        detail: format!(
            "surrogate bind of a key in '{}' on vShard {} returned no surrogate",
            collection.name(),
            home.as_u32()
        ),
    })
}

/// [`assign_at_home`] for many keys of one collection, in `pks` order. Every
/// key the home binds none of is minted in one `SurrogateBind` entry.
pub(crate) async fn assign_many_at_home(
    state: &SharedState,
    home: VShardId,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    pks: &[&[u8]],
) -> crate::Result<Vec<Surrogate>> {
    let local = state
        .surrogate_assigner
        .lookup_bound_many(collection, tenant_id, pks)?;
    let mut unbound: Vec<&[u8]> = Vec::new();
    let mut proposed: FxHashSet<&[u8]> = FxHashSet::default();
    for (&pk, hit) in pks.iter().zip(&local) {
        if hit.is_none() && proposed.insert(pk) {
            unbound.push(pk);
        }
    }
    let minted = mint_many_at_home(state, home, collection, tenant_id, &unbound).await?;
    let winners: FxHashMap<&[u8], Surrogate> = unbound.iter().copied().zip(minted).collect();
    pks.iter()
        .zip(local)
        .map(|(pk, hit)| {
            hit.or_else(|| winners.get(pk).copied())
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "surrogate bind of a key in '{}' applied on vShard {} but the key is \
                         unbound",
                        collection.name(),
                        home.as_u32()
                    ),
                })
        })
        .collect()
}

/// Bind every key of `unbound` on `home`'s group in one `SurrogateBind`
/// entry, and return each key's log-order winner, in `unbound` order.
///
/// `unbound` holds keys this node's catalog bound none of when the caller
/// read it. A key bound since keeps that binding: the apply is first-wins,
/// and the winner it answers is the earlier one. The apply on this node
/// binds every key before the proposal resolves, so each winner is in this
/// node's catalog on return. Runs on the leader of `home`.
pub(crate) async fn mint_many_at_home(
    state: &SharedState,
    home: VShardId,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    unbound: &[&[u8]],
) -> crate::Result<Vec<Surrogate>> {
    if unbound.is_empty() {
        return Ok(Vec::new());
    }
    let proposer = state.async_raft_proposer()?;
    let candidates = state
        .surrogate_assigner
        .mint_candidates(unbound.len())
        .await?;
    let binds: Vec<(Vec<u8>, u32)> = unbound
        .iter()
        .zip(candidates)
        .map(|(pk, candidate)| (pk.to_vec(), candidate.as_u32()))
        .collect();
    let entry = ReplicatedEntry::new(
        tenant_id.as_u64(),
        collection.database_id().as_u64(),
        home.as_u32(),
        ReplicatedWrite::SurrogateBind {
            collection: collection.name().to_string(),
            binds,
        },
    );
    let deadline = crate::control::server::shared::session::StatementDeadline::of_statement(state)
        .tokio_instant();
    let payload = propose_replicated_entry(state, proposer, entry, deadline).await?;
    // The apply answers the winner of every identity, in entry order.
    let winners: Vec<u32> =
        zerompk::from_msgpack(&payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "surrogate bind winners of '{}' on vShard {}: {e}",
                collection.name(),
                home.as_u32()
            ),
        })?;
    if winners.len() != unbound.len() {
        return Err(crate::Error::Internal {
            detail: format!(
                "surrogate bind on vShard {} answered {} of {} keys of '{}'",
                home.as_u32(),
                winners.len(),
                unbound.len(),
                collection.name()
            ),
        });
    }
    Ok(winners.into_iter().map(Surrogate::new).collect())
}

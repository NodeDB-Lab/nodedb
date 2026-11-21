// SPDX-License-Identifier: BUSL-1.1

//! The routed exchange for keys this node's catalog already missed.
//!
//! The caller reads this node's catalog first and passes the keys it binds
//! none of. These functions run no local read of their own. Every answer is
//! bound in this node's catalog on return, so the caller writes nothing.

use std::sync::Arc;

use nodedb_cluster::NexarTransport;
use nodedb_cluster::rpc_codec::ASSIGN_SURROGATE_KEY_BUDGET;
use nodedb_types::{CollectionKey, Surrogate};

use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId, VShardId};

use super::batch::key_batches;
use super::resolve::{ExchangeBatch, Route, build_request, route_for, send_to_leader};

/// Batch requests one exchange keeps in flight to a remote home leader. A
/// batch can fill a quarter of an RPC frame, so this bounds the bytes held
/// in flight.
pub(super) const MAX_ROUTED_BATCHES_IN_FLIGHT: usize = 4;

/// The home's winner for each key of `unbound`, in `unbound` order. Every
/// winner is bound in this node's catalog on return.
///
/// `unbound` holds distinct keys this node's catalog bound none of when the
/// caller read it. A key bound since keeps its binding: the home binds
/// first-wins and answers the earlier value.
pub(crate) async fn assign_unbound_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    unbound: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Surrogate>> {
    if unbound.is_empty() {
        return Ok(Vec::new());
    }
    let vshard = VShardId::from_collection(collection);
    match route_for(state, vshard, collection.name())? {
        // This node leads the home: its own apply binds every key.
        Route::Local => {
            let deadline = StatementDeadline::of_statement(state).tokio_instant();
            super::authority::mint_many_at_home(
                state, vshard, collection, tenant_id, unbound, deadline,
            )
            .await
        }
        Route::Remote { leader, transport } => {
            let route = RemoteRoute {
                state,
                transport,
                leader,
            };
            let all = ExchangeBatch {
                vshard,
                collection,
                tenant_id,
                pks: unbound,
                trace_id,
            };
            let answered = route.exchange(all, false).await?;
            let winners = answered
                .into_iter()
                .map(|answer| {
                    answer.ok_or_else(|| crate::Error::Internal {
                        detail: format!(
                            "surrogate-exchange: leader node {leader} answered an assign in \
                             '{}' with no surrogate",
                            collection.name()
                        ),
                    })
                })
                .collect::<crate::Result<Vec<Surrogate>>>()?;
            keep_winners(state, collection, tenant_id, unbound, &winners)
        }
    }
}

/// The home's binding of each key of `unbound`, in `unbound` order, `None`
/// for a key the home binds none of. Every found winner is bound in this
/// node's catalog on return.
///
/// `unbound` holds distinct keys this node's catalog bound none of when the
/// caller read it. A node that leads the home answers from that read.
pub(crate) async fn lookup_unbound_routed(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    unbound: &[&[u8]],
    trace_id: TraceId,
) -> crate::Result<Vec<Option<Surrogate>>> {
    if unbound.is_empty() {
        return Ok(Vec::new());
    }
    let vshard = VShardId::from_collection(collection);
    let Route::Remote { leader, transport } = route_for(state, vshard, collection.name())? else {
        // This node is the home: its catalog miss is the home's answer.
        return Ok(vec![None; unbound.len()]);
    };
    let route = RemoteRoute {
        state,
        transport,
        leader,
    };
    let all = ExchangeBatch {
        vshard,
        collection,
        tenant_id,
        pks: unbound,
        trace_id,
    };
    let answered = route.exchange(all, true).await?;
    // Every answered winner binds first-wins in one catalog transaction.
    let found: Vec<(&[u8], Surrogate)> = unbound
        .iter()
        .zip(&answered)
        .filter_map(|(&pk, answer)| answer.map(|winner| (pk, winner)))
        .collect();
    let mut bound = state
        .surrogate_assigner
        .bind_many(collection, tenant_id, &found)?
        .into_iter();
    let mut kept = Vec::with_capacity(answered.len());
    for answer in &answered {
        kept.push(match answer {
            Some(_) => Some(bound.next().ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "a found key of '{}' stayed unbound after bind",
                    collection.name()
                ),
            })?),
            None => None,
        });
    }
    Ok(kept)
}

/// Bind each remote winner in this node's catalog first-wins, in one catalog
/// transaction, and return the kept values in `missing` order.
fn keep_winners(
    state: &SharedState,
    collection: CollectionKey<'_>,
    tenant_id: TenantId,
    missing: &[&[u8]],
    winners: &[Surrogate],
) -> crate::Result<Vec<Surrogate>> {
    let rows: Vec<(&[u8], Surrogate)> = missing
        .iter()
        .copied()
        .zip(winners.iter().copied())
        .collect();
    state
        .surrogate_assigner
        .bind_many(collection, tenant_id, &rows)
}

/// The remote home leader one exchange sends its batches to.
struct RemoteRoute<'r> {
    state: &'r SharedState,
    transport: &'r Arc<NexarTransport>,
    leader: u64,
}

impl RemoteRoute<'_> {
    /// The leader's answer for each key of `all`, in its order: one request
    /// per frame-sized batch, at most `MAX_ROUTED_BATCHES_IN_FLIGHT` in
    /// flight. `None` marks a key the home binds none of.
    async fn exchange(
        &self,
        all: ExchangeBatch<'_>,
        lookup_only: bool,
    ) -> crate::Result<Vec<Option<Surrogate>>> {
        use futures::StreamExt;

        // Built in a loop, not by an iterator closure: a closure over a
        // borrowed key, held across the await below, fails every caller's
        // `Send` check.
        let mut requests = Vec::new();
        for pks in key_batches(all.pks, ASSIGN_SURROGATE_KEY_BUDGET) {
            let batch = ExchangeBatch {
                vshard: all.vshard,
                collection: all.collection,
                tenant_id: all.tenant_id,
                pks,
                trace_id: all.trace_id,
            };
            requests.push(self.send_batch(batch, lookup_only));
        }
        let answered: Vec<Vec<Option<Surrogate>>> = futures::stream::iter(requests)
            .buffered(MAX_ROUTED_BATCHES_IN_FLIGHT)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<crate::Result<_>>()?;
        Ok(answered.into_iter().flatten().collect())
    }

    /// One batch at the remote home leader. The request is built when the
    /// stream first polls it, so its budget is what is left at send time.
    async fn send_batch(
        &self,
        batch: ExchangeBatch<'_>,
        lookup_only: bool,
    ) -> crate::Result<Vec<Option<Surrogate>>> {
        let req = build_request(self.state, batch, lookup_only);
        let answers = send_to_leader(self.state, self.transport, self.leader, req).await?;
        Ok(answers
            .into_iter()
            .map(|answer| answer.map(Surrogate::new))
            .collect())
    }
}

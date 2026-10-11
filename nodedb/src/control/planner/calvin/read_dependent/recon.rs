// SPDX-License-Identifier: BUSL-1.1

//! The coordinator's read of a row a read-dependent transaction depends on.
//!
//! The read reaches the row's vShard owner through the gateway, as a
//! `SELECT` does, and returns the stored bytes a passive read of the same
//! row returns under the transaction's locks. The active participants
//! compare the two, so the read carries no row filter: both sides see the
//! row as stored.

use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// The stored bytes of key-value row `key` of `collection`, read on the
/// collection's vShard owner. `None` when the row is absent.
pub(super) async fn read_kv_row(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &QualifiedCollection,
    key: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: collection.clone(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    });
    let gateway = state.installed_gateway()?;
    let ctx = crate::control::gateway::core::QueryContext {
        tenant_id,
        trace_id: TraceId::ZERO,
        database_id,
        txn_id: None,
        linearizable: true,
    };
    let payloads = gateway.execute_internal(&ctx, plan).await?;
    // A KV get routes to the collection's one vShard. An absent row answers
    // with an empty payload: a stored row is a msgpack image, never empty.
    Ok(payloads
        .into_iter()
        .next()
        .filter(|bytes| !bytes.is_empty()))
}

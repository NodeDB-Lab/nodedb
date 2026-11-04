// SPDX-License-Identifier: BUSL-1.1

//! Routing gate for INSERT into a collection that declares `BALANCED ON (...)`.
//!
//! BALANCED is a write-BOUNDARY predicate: debits and credits arrive on
//! different rows, so the rows a statement writes are only meaningful together.
//! A single-row `INSERT` lowers to a `PointInsert` task, its own Data-Plane
//! request — its own boundary. On a balanced collection that would judge the
//! first leg of a journal on its own, which no journal can survive.
//!
//! So an INSERT into a balanced collection always lowers to a single
//! [`DocumentOp::BatchInsert`] page, as every multi-row document INSERT does
//! and `INSERT ... SELECT` does per page. The page applies inside ONE
//! Data-Plane transaction, so its rows are judged together and a refusal
//! leaves none of them behind. Each row is still judged as a lone
//! `PointInsert` would be: a duplicate key is refused.

use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Routing identity for the page built by [`balanced_batch_task`].
pub(in crate::control::planner::sql_plan_convert::dml) struct BalancedBatch<'a> {
    /// The target, qualified once for its database.
    pub collection: &'a nodedb_types::QualifiedCollection,
    pub tenant_id: TenantId,
    pub vshard: VShardId,
    /// `(document_id, msgpack body)` in statement order.
    pub documents: Vec<(String, Vec<u8>)>,
    /// Per-row surrogates, parallel to `documents`.
    pub surrogates: Vec<Surrogate>,
    /// `ON CONFLICT DO NOTHING`: a stored or repeated key skips its row.
    pub if_absent: bool,
}

/// The declarations that decide which shape an INSERT lowers to.
#[derive(Default)]
pub(in crate::control::planner::sql_plan_convert) struct WriteGates {
    /// The collection converges via CRDT last-writer-wins full replace.
    pub crdt: bool,
    /// The collection declares `BALANCED ON (...)`, so its rows are one set.
    pub balanced: bool,
    /// The columns the engine computes on every write. A write that supplies
    /// one is refused.
    pub generated: Vec<String>,
}

/// Read the INSERT routing gates and write checks from the collection's
/// catalog row.
///
/// One catalog read answers every question. Asking twice — once through
/// [`document_collection_is_crdt`](super::crdt_gate::document_collection_is_crdt)
/// and again for the constraint — would put a second catalog lookup on the path
/// of every INSERT in the system to learn something one row already carries.
///
/// A genuine catalog READ error propagates: routing an INSERT by a default
/// because the catalog could not be read would bypass CRDT convergence, or
/// split a balanced statement's boundary and refuse journals the constraint
/// permits. An absent credential store or an absent collection row declares
/// neither gate.
///
/// The lookup reads the catalog row `key` names. The catalog keys a
/// collection by its bare name, which `key` carries.
pub(in crate::control::planner::sql_plan_convert) fn document_collection_write_gates(
    ctx: &ConvertContext,
    key: nodedb_types::CollectionKey<'_>,
) -> crate::Result<WriteGates> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(WriteGates::default());
    };
    let catalog = credentials.catalog();
    Ok(catalog
        .get_collection(key.database_id(), ctx.tenant_id.as_u64(), key.name())?
        .map(|c| WriteGates {
            crdt: c.crdt,
            balanced: c.balanced.is_some(),
            generated: c.generated_columns(),
        })
        .unwrap_or_default())
}

/// The single `BatchInsert` task carrying every row of one INSERT statement:
/// a balanced INSERT of any size, and every multi-row document INSERT.
pub(in crate::control::planner::sql_plan_convert::dml) fn balanced_batch_task(
    batch: BalancedBatch<'_>,
    database_id: crate::types::DatabaseId,
) -> PhysicalTask {
    let BalancedBatch {
        collection,
        tenant_id,
        vshard,
        documents,
        surrogates,
        if_absent,
    } = batch;
    PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: collection.clone(),
            documents,
            if_absent,
            surrogates,
            // Filled by the passes that own them, exactly as the per-row
            // `PointInsert` shape leaves them: the RETURNING spec by the
            // protocol layer's injection pass, the read filter by the RLS
            // injection pass, and both materialized-sum slots by the
            // resolution pass that runs after conversion.
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

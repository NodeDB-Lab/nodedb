// SPDX-License-Identifier: BUSL-1.1

//! Routing gate for INSERT into a collection that declares `BALANCED ON (...)`.
//!
//! BALANCED is a write-BOUNDARY predicate: debits and credits arrive on
//! different rows, so the rows a statement writes are only meaningful together.
//! An `INSERT ... VALUES (a), (b)` normally lowers to ONE `PointInsert` TASK PER
//! ROW, and each task is a separate Data-Plane request — a separate boundary. On
//! a balanced collection that judges the first leg of a journal on its own,
//! which no journal can survive: the statement is refused before the second leg
//! is ever accounted.
//!
//! So an INSERT into a balanced collection lowers to a single
//! [`DocumentOp::BatchInsert`] page instead. That is the shape this codebase
//! already uses everywhere else rows arrive as a set (`INSERT ... SELECT`
//! lowers to it too), and it is what makes the statement one boundary: the page
//! applies inside ONE Data-Plane transaction, so its rows are judged together
//! and a refusal leaves none of them behind.
//!
//! The page shape is used for a single-row INSERT as well. Making it depend on
//! the row count would fork the write path on something the constraint does not
//! care about; a page of one row is judged exactly as a lone `PointInsert`
//! would be, and is refused for the same reason.

use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Routing identity for the page built by [`balanced_batch_task`].
pub(in crate::control::planner::sql_plan_convert::dml) struct BalancedBatch<'a> {
    pub collection: &'a str,
    pub tenant_id: TenantId,
    pub vshard: VShardId,
    /// `(document_id, msgpack body)` in statement order.
    pub documents: Vec<(String, Vec<u8>)>,
    /// Per-row surrogates, parallel to `documents`.
    pub surrogates: Vec<Surrogate>,
}

/// The declarations that decide which shape an INSERT lowers to.
#[derive(Default)]
pub(in crate::control::planner::sql_plan_convert::dml) struct WriteGates {
    /// The collection converges via CRDT last-writer-wins full replace.
    pub crdt: bool,
    /// The collection declares `BALANCED ON (...)`, so its rows are one set.
    pub balanced: bool,
}

/// Read both INSERT routing gates from the collection's catalog row.
///
/// One catalog read answers both questions. Asking twice — once through
/// [`document_collection_is_crdt`](super::crdt_gate::document_collection_is_crdt)
/// and again for the constraint — would put a second catalog lookup on the path
/// of every INSERT in the system to learn something one row already carries.
///
/// A genuine catalog READ error propagates: routing an INSERT by a default
/// because the catalog could not be read would bypass CRDT convergence, or
/// split a balanced statement's boundary and refuse journals the constraint
/// permits. An absent credential store or an absent collection row declares
/// neither gate.
pub(in crate::control::planner::sql_plan_convert::dml) fn document_collection_write_gates(
    ctx: &ConvertContext,
    collection: &str,
) -> crate::Result<WriteGates> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(WriteGates::default());
    };
    let catalog = credentials.catalog();
    // The caller routes the plan on the database-qualified collection, but the
    // catalog keys collections by the bare name, so strip the qualifier back off
    // here or an INSERT into a non-default database misses its row and silently
    // declares neither gate — losing CRDT convergence, and the BALANCED boundary
    // with it. Identity for `DatabaseId::DEFAULT`.
    let bare = crate::control::target_identity::bare_collection_name(ctx.database_id, collection);
    Ok(catalog
        .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), &bare)?
        .map(|c| WriteGates {
            crdt: c.crdt,
            balanced: c.balanced.is_some(),
        })
        .unwrap_or_default())
}

/// The single `BatchInsert` task carrying every row of one INSERT statement.
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
    } = batch;
    PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: nodedb_types::QualifiedCollection::new(database_id, collection),
            documents,
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

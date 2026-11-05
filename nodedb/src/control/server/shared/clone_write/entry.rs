// SPDX-License-Identifier: BUSL-1.1

//! Single hooked-in clone CoW write-interception entry point. Routes by plan
//! shape to the Document or KV copy-up/tombstone protocol; a write shape none
//! of those protocols claims is refused outright on a `Shadowed`/
//! `Materializing` clone (see [`refuse_unsupported_clone_write`]).

use nodedb_types::{CloneStatus, CollectionType, Lsn, TenantId};

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::util::{synthetic_affected_response, write_err};
use crate::types::TxnId;

/// Outcome of write-path clone interception.
pub(in crate::control::server) enum CloneWriteOutcome {
    /// No interception needed — caller must dispatch normally.
    Passthrough,
    /// The write was fully handled by the clone path. Caller uses this response.
    Handled(Response),
}

/// Outcome of clone interception for a write inside a transaction.
pub(in crate::control::server) enum TxnCloneWriteOutcome {
    /// Stage the write, retargeted when a copy-up moved its row.
    Passthrough,
    /// The copy-on-write answered the write: it stages nothing. Its
    /// tombstones commit with the transaction.
    Handled(Response),
    /// Stage the write, which now names only the rows the clone's target
    /// holds, and add `hidden` to its count: the source-only rows the
    /// copy-on-write's tombstones hid.
    Narrowed { hidden: u64 },
}

/// One copy-on-write step, before the caller's mode decides how the write
/// applies.
enum CloneStep {
    Passthrough,
    Handled(Response),
    /// A KV delete on a shadowed clone: its tombstones are recorded, and the
    /// keys the target holds still need removing.
    KvDelete(super::kv::KvCloneDelete),
}

/// Intercept a single write task for a cloned collection.
///
/// Called once per task from [`super::gate::intercept_and_authorize`], before
/// authorization — never called directly by a protocol handler.
pub(in crate::control::server) async fn maybe_intercept_clone_write(
    state: &SharedState,
    task: &mut PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
) -> crate::Result<CloneWriteOutcome> {
    match intercept(state, task, identity, tenant_id, None).await? {
        CloneStep::Passthrough => Ok(CloneWriteOutcome::Passthrough),
        CloneStep::Handled(resp) => Ok(CloneWriteOutcome::Handled(resp)),
        CloneStep::KvDelete(delete) => Ok(CloneWriteOutcome::Handled(
            super::kv::apply_kv_clone_delete(state, task, delete).await?,
        )),
    }
}

/// Intercept a single write task for a cloned collection inside transaction
/// `txn_id`, before the write stages.
///
/// The probes read the transaction's overlay. The tombstones and copy-up
/// mappings are catalog entries: the connection's transaction buffers them
/// (`cow_entry::replicate_async`), so they commit with the write and a rollback
/// drops them. A copy-up's target row is an exact copy of its source row, so
/// it applies at once: a rolled-back transaction leaves the clone reading the
/// same row. A KV delete stages its removal of the target's rows instead of
/// applying it.
pub(in crate::control::server) async fn maybe_intercept_clone_write_in_txn(
    state: &SharedState,
    task: &mut PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    txn_id: TxnId,
) -> crate::Result<TxnCloneWriteOutcome> {
    match intercept(state, task, identity, tenant_id, Some(txn_id)).await? {
        CloneStep::Passthrough => Ok(TxnCloneWriteOutcome::Passthrough),
        CloneStep::Handled(resp) => Ok(TxnCloneWriteOutcome::Handled(resp)),
        CloneStep::KvDelete(delete) => match delete.narrowed {
            None => Ok(TxnCloneWriteOutcome::Handled(synthetic_affected_response(
                state.next_request_id(),
                Lsn::new(0),
                delete.source_only_hidden,
            ))),
            Some(narrowed) => {
                task.plan = *narrowed;
                Ok(TxnCloneWriteOutcome::Narrowed {
                    hidden: delete.source_only_hidden,
                })
            }
        },
    }
}

/// Route a write by plan shape to its engine's copy-on-write protocol.
async fn intercept(
    state: &SharedState,
    task: &mut PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    txn_id: Option<TxnId>,
) -> crate::Result<CloneStep> {
    // Classify first: the shape check borrows the plan, and the document
    // arm needs it mutably (a copy-up retargets the plan's surrogate).
    enum Shape {
        Document,
        KvMutate,
        KvInsert,
        None,
    }
    let shape = match &task.plan {
        PhysicalPlan::Document(
            DocumentOp::PointUpdate { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::BatchInsert { .. },
        ) => Shape::Document,
        PhysicalPlan::Kv(KvOp::FieldSet { .. } | KvOp::Delete { .. }) => Shape::KvMutate,
        PhysicalPlan::Kv(
            KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::BatchPut { .. },
        ) => Shape::KvInsert,
        _ => Shape::None,
    };
    let outcome = match shape {
        Shape::Document => {
            super::document::intercept_doc_clone_write(state, task, identity, tenant_id, txn_id)
                .await?
        }
        Shape::KvMutate => {
            return super::kv::intercept_kv_clone_write(state, task, identity, tenant_id, txn_id)
                .await
                .map(|step| match step {
                    super::kv::KvCloneStep::Passthrough => CloneStep::Passthrough,
                    super::kv::KvCloneStep::Delete(delete) => CloneStep::KvDelete(delete),
                });
        }
        Shape::KvInsert => {
            super::kv_insert::intercept_kv_clone_insert(state, task, tenant_id).await?
        }
        Shape::None => refuse_unsupported_clone_write(state, task, tenant_id)?,
    };
    Ok(match outcome {
        CloneWriteOutcome::Passthrough => CloneStep::Passthrough,
        CloneWriteOutcome::Handled(resp) => CloneStep::Handled(resp),
    })
}

/// Refuse a write shape none of `document`/`kv`/`kv_insert` claims when it
/// targets a `Shadowed`/`Materializing` clone. The supported set is never
/// hand-listed: it is exactly what the `Shape` match above routes to a real
/// CoW module, so a forgotten new-engine arm there is a compile-time miss,
/// never a silent bypass here.
fn refuse_unsupported_clone_write(
    state: &SharedState,
    task: &PhysicalTask,
    tenant_id: TenantId,
) -> crate::Result<CloneWriteOutcome> {
    if !plan_is_write(&task.plan) {
        return Ok(CloneWriteOutcome::Passthrough);
    }
    let Some(qualified) = extract_collection(&task.plan) else {
        return Ok(CloneWriteOutcome::Passthrough);
    };

    let db_id = task.database_id;
    // A plan names a collection qualified and an array bare.
    let coll_name = nodedb_types::CollectionKey::from_relation_name(db_id, qualified)?.name();
    let catalog = state.credentials.catalog();

    let Some(desc) = catalog
        .get_collection(db_id, tenant_id.as_u64(), coll_name)
        .map_err(|e| write_err(format!("clone write gate: get_collection: {e}")))?
    else {
        return Ok(CloneWriteOutcome::Passthrough);
    };
    if desc.cloned_from.is_none() {
        return Ok(CloneWriteOutcome::Passthrough);
    }
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
        CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
    }

    let database = catalog
        .get_database(db_id)
        .map_err(|e| write_err(format!("clone write gate: get_database: {e}")))?
        .map(|d| d.name)
        .unwrap_or_else(|| db_id.to_string());

    // Document/KV DO have a CoW module (`document.rs`/`kv.rs`) — reaching
    // here for one of them means the op shape is outside its point-op
    // protocol, not that the engine lacks support. Columnar (plain,
    // timeseries, spatial) has no CoW module at all. Exhaustive: a third
    // `CollectionType` forces a decision here too.
    let reason = match desc.collection_type {
        CollectionType::Document(_) | CollectionType::KeyValue(_) => {
            "supports copy-on-write for point writes only; this write's shape is outside that protocol"
        }
        CollectionType::Columnar(_) => "has no copy-on-write support",
    };

    Err(crate::Error::CloneWriteRequiresMaterialize {
        collection: coll_name.to_string(),
        engine: desc.collection_type.as_str().to_string(),
        database,
        reason,
    })
}

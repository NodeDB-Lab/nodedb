// SPDX-License-Identifier: BUSL-1.1

//! KV engine CoW write interception (FieldSet / Delete).

use std::sync::Arc;

use nodedb_types::{CloneStatus, TenantId};

use crate::bridge::envelope::Response;
use crate::control::clone::copyup::{KvCopyUpParams, perform_kv_clone_copyup};
use crate::control::clone::tombstone::{KvTombstoneParams, perform_kv_clone_tombstone};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::session::ddl_buffer::buffered_clone_suppressions;
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::state::SharedState;
use crate::types::TxnId;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::copy_source::{clone_source_view, copy_source_key};
use super::probes::{
    ProbeTarget, RawTarget, dispatch_data_plane_raw, fetch_kv_source_value, probe_kv_key_in_target,
};
use super::util::{synthetic_affected_response, write_err};

/// What the copy-on-write decided for a KV delete on a shadowed clone. Every
/// key's tombstone is already recorded.
pub(super) struct KvCloneDelete {
    /// The delete narrowed to the keys the clone's target holds, which it
    /// still removes. `None` when the target holds none of them. Boxed: a
    /// plan dwarfs every other step the enums carrying this hold.
    pub narrowed: Option<Box<PhysicalPlan>>,
    /// Source-only keys the tombstones hid. The delete removed these rows
    /// from the clone's view, and the tombstones report no count of their own.
    pub source_only_hidden: u64,
}

/// One KV copy-on-write step.
pub(super) enum KvCloneStep {
    /// Dispatch the write as planned.
    Passthrough,
    /// A delete on a shadowed clone. The caller's mode removes the target's
    /// rows.
    Delete(KvCloneDelete),
}

/// Handle KV CoW write interception (FieldSet / Delete). Inside transaction
/// `txn_id` the presence probes read its overlay, and a key the transaction
/// already tombstoned counts as absent from the source.
pub(super) async fn intercept_kv_clone_write(
    state: &SharedState,
    task: &PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    txn_id: Option<TxnId>,
) -> crate::Result<KvCloneStep> {
    let (collection_qualified, kv_key) = match &task.plan {
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection, key, ..
        }) => (collection.as_str(), key.clone()),
        PhysicalPlan::Kv(KvOp::Delete {
            collection,
            keys,
            returning,
            rls_write_check,
            rls_filters,
            ..
        }) => {
            return intercept_kv_clone_delete(
                state,
                task,
                identity,
                KvDeleteTarget {
                    tenant_id,
                    collection,
                    keys,
                    has_returning: returning.is_some(),
                    rls_write_check,
                    rls_filters,
                    txn_id,
                },
            )
            .await;
        }
        _ => return Ok(KvCloneStep::Passthrough),
    };

    // FieldSet path: check clone status, copy-up if needed.
    let db_id = task.database_id;
    let coll_name =
        nodedb_types::CollectionKey::from_qualified_str(db_id, collection_qualified)?.name();

    let catalog = state.credentials.catalog();

    let desc = catalog
        .get_collection(db_id, tenant_id.as_u64(), coll_name)
        .map_err(|e| write_err(format!("clone kv write: get_collection: {e}")))?;
    let Some(desc) = desc else {
        return Ok(KvCloneStep::Passthrough);
    };

    let Some(ref origin) = desc.cloned_from else {
        return Ok(KvCloneStep::Passthrough);
    };
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(KvCloneStep::Passthrough),
        CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
    }

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        origin.source_database,
        &origin.source_collection,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &emitter,
    )?;

    let key_in_target = probe_kv_key_in_target(
        state,
        identity,
        ProbeTarget {
            tenant_id,
            db_id,
            collection_qualified,
            txn_id,
        },
        &kv_key,
    )
    .await
    .map_err(|e| write_err(format!("clone kv write probe: {e}")))?;

    if key_in_target {
        // Row exists in target — let the normal FieldSet proceed.
        return Ok(KvCloneStep::Passthrough);
    }

    let kv_key_str = String::from_utf8_lossy(&kv_key).into_owned();
    // A key this transaction tombstoned, or a committed tombstone, is gone
    // from the clone: the FieldSet finds no row, and the source row is never
    // copied back.
    let qualified =
        crate::control::planner::sql_plan_convert::convert::db_qualified(db_id, coll_name);
    if buffered_clone_suppressions(&qualified)
        .kv_keys
        .contains(&kv_key_str)
        || catalog
            .is_kv_clone_tombstoned(&qualified, &kv_key_str)
            .map_err(|e| write_err(format!("clone kv write: tombstone lookup: {e}")))?
    {
        return Ok(KvCloneStep::Passthrough);
    }

    // Fetch the source version the clone reads, and copy it up to target.
    let source_coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        origin.source_database,
        origin.source_collection.as_str(),
    );
    let view = clone_source_view(
        state,
        tenant_id,
        origin,
        desc.bitemporal,
        &source_coll_qualified,
    );
    let source = copy_source_key(state, identity, view, &kv_key)
        .await
        .map_err(|e| write_err(format!("clone kv copyup fetch: {e}")))?;

    let Some(source) = source else {
        // Row absent in source — let normal FieldSet run (no-op or error from DP).
        return Ok(KvCloneStep::Passthrough);
    };

    perform_kv_clone_copyup(KvCopyUpParams {
        state,
        tenant_id,
        target_db_id: db_id,
        target_collection: coll_name,
        source,
    })
    .await
    .map_err(|e| write_err(format!("clone kv copyup: {e}")))?;

    // Tombstone the source key so future clone reads do not merge in the
    // now-superseded source row.  The copy-up wrote the row to the target
    // and the FieldSet will overwrite it; the source copy must be hidden.
    perform_kv_clone_tombstone(KvTombstoneParams {
        tenant_id,
        state,
        target_db_id: db_id,
        target_collection: coll_name,
        kv_key: kv_key_str,
    })
    .await
    .map_err(|e| write_err(format!("clone kv tombstone after copyup: {e}")))?;

    // Fall through: let the original FieldSet dispatch to the target.
    Ok(KvCloneStep::Passthrough)
}

/// The clone collection, keys and write gate a KV delete names.
struct KvDeleteTarget<'a> {
    tenant_id: TenantId,
    collection: &'a nodedb_types::QualifiedCollection,
    keys: &'a [Vec<u8>],
    has_returning: bool,
    rls_write_check: &'a nodedb_types::RlsWriteCheck,
    rls_filters: &'a [u8],
    txn_id: Option<TxnId>,
}

/// Record a tombstone for every key a KV delete on a shadowed clone names,
/// and sort the keys by where they live.
async fn intercept_kv_clone_delete(
    state: &SharedState,
    task: &PhysicalTask,
    identity: &AuthenticatedIdentity,
    target: KvDeleteTarget<'_>,
) -> crate::Result<KvCloneStep> {
    let KvDeleteTarget {
        tenant_id,
        collection,
        keys,
        has_returning,
        rls_write_check,
        rls_filters,
        txn_id,
    } = target;
    let collection_qualified = collection.as_str();
    let db_id = task.database_id;
    let coll_name =
        nodedb_types::CollectionKey::from_qualified_str(db_id, collection_qualified)?.name();

    let catalog = state.credentials.catalog();

    let desc = catalog
        .get_collection(db_id, tenant_id.as_u64(), coll_name)
        .map_err(|e| write_err(format!("clone kv delete: get_collection: {e}")))?;
    let Some(desc) = desc else {
        return Ok(KvCloneStep::Passthrough);
    };
    let Some(ref origin) = desc.cloned_from else {
        return Ok(KvCloneStep::Passthrough);
    };
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(KvCloneStep::Passthrough),
        CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
    }
    // A row this delete hides only by tombstone lives in the source, so the
    // clone has no stored pre-image to project for it. The reply is a
    // synthesized count, which the RETURNING renderer cannot decode as rows;
    // refusing beats answering the wrong shape.
    if has_returning {
        return Err(crate::Error::BadRequest {
            detail: "RETURNING is not supported on a DELETE against a shadowed clone: \
                     rows hidden by tombstone have no stored pre-image to project. \
                     Materialize the clone first, or SELECT the rows before deleting."
                .to_string(),
        });
    }

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        origin.source_database,
        &origin.source_collection,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &emitter,
    )?;

    // Split each key into one of two paths:
    //   • key absent in target (source-only) → record a tombstone so future
    //     scans hide the source row.
    //   • key present in target (already copied up or written in this clone)
    //     → the target row is removed, and a tombstone is ALSO recorded so any
    //     surviving source row remains hidden after deletion.
    //
    // Tombstoning unconditionally for target-resident keys is safe: the
    // source row (if any) must always be hidden in this clone after the user
    // has issued a DELETE. `source_only_hidden` counts keys the tombstone
    // alone removed from this clone's view: absent from the target but present
    // in the source, and not already tombstoned by this transaction or a
    // committed delete. A key in neither target nor source removed nothing.
    let mut keys_in_target: Vec<Vec<u8>> = Vec::new();
    let mut source_only_hidden = 0u64;
    let source_coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        origin.source_database,
        origin.source_collection.as_str(),
    );
    let view = clone_source_view(
        state,
        tenant_id,
        origin,
        desc.bitemporal,
        &source_coll_qualified,
    );
    let qualified =
        crate::control::planner::sql_plan_convert::convert::db_qualified(db_id, coll_name);
    let mut already_hidden = buffered_clone_suppressions(&qualified).kv_keys;
    already_hidden.extend(
        catalog
            .list_kv_clone_tombstones(&qualified)
            .map_err(|e| write_err(format!("clone kv delete: tombstone list: {e}")))?,
    );
    for key in keys {
        let key_str = String::from_utf8_lossy(key).into_owned();
        let key_in_target = probe_kv_key_in_target(
            state,
            identity,
            ProbeTarget {
                tenant_id,
                db_id,
                collection_qualified,
                txn_id,
            },
            key,
        )
        .await
        .map_err(|e| write_err(format!("clone kv delete probe: {e}")))?;

        if !key_in_target && !already_hidden.contains(&key_str) {
            let source_value = fetch_kv_source_value(state, identity, view, key)
                .await
                .map_err(|e| write_err(format!("clone kv delete source probe: {e}")))?;
            if source_value.is_some() {
                source_only_hidden += 1;
            }
        }

        perform_kv_clone_tombstone(KvTombstoneParams {
            tenant_id,
            state,
            target_db_id: db_id,
            target_collection: coll_name,
            kv_key: key_str,
        })
        .await
        .map_err(|e| write_err(format!("clone kv tombstone: {e}")))?;

        if key_in_target {
            keys_in_target.push(key.clone());
        }
    }

    let narrowed = (!keys_in_target.is_empty()).then(|| {
        Box::new(PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.clone(),
            keys: keys_in_target,
            // The narrowed delete is the same statement's write, so it carries
            // the same compiled predicate: dropping it here will launder a
            // governed delete into an ungoverned one for exactly the keys that
            // resolve to real target rows.
            rls_write_check: rls_write_check.clone(),
            // `has_returning` refused a projection above.
            returning: None,
            rls_filters: rls_filters.to_vec(),
            // The narrowed delete answers a count, not a sync ack.
            provenance: None,
        }))
    });
    Ok(KvCloneStep::Delete(KvCloneDelete {
        narrowed,
        source_only_hidden,
    }))
}

/// Outside a transaction: remove the target's rows of `delete` at once and
/// answer the statement's whole count.
pub(super) async fn apply_kv_clone_delete(
    state: &SharedState,
    task: &PhysicalTask,
    delete: KvCloneDelete,
) -> crate::Result<Response> {
    let KvCloneDelete {
        narrowed,
        source_only_hidden,
    } = delete;
    let Some(narrowed) = narrowed else {
        return Ok(synthetic_affected_response(
            state.next_request_id(),
            crate::types::Lsn::new(0),
            source_only_hidden,
        ));
    };
    let resp = dispatch_data_plane_raw(
        state,
        RawTarget {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            txn_id: None,
        },
        *narrowed,
    )
    .await
    .map_err(|e| write_err(format!("clone kv delete dispatch: {e}")))?;

    // Total = keys removed from the target + keys the tombstones hid in the
    // source. Re-wrap so the client sees one count for the one statement it
    // issued.
    let dispatched = require_affected_count(resp.payload.as_ref())
        .map_err(|e| write_err(format!("clone kv delete count: {e}")))?;
    Ok(synthetic_affected_response(
        state.next_request_id(),
        resp.watermark_lsn,
        dispatched + source_only_hidden,
    ))
}

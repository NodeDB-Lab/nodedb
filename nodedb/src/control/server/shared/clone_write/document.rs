// SPDX-License-Identifier: BUSL-1.1

//! Document engine CoW write interception.
//!
//! On a `Shadowed`/`Materializing` clone: UPDATE copies the source row up then
//! applies; DELETE tombstones the source surrogate; INSERT/PUT/UPSERT tombstones
//! the same-key source row without a copy-up.

use std::sync::Arc;

use nodedb_types::{CloneStatus, DatabaseId, Lsn, Surrogate, TenantId};

use crate::control::clone::copyup::{CopyUpParams, perform_clone_copyup};
use crate::control::clone::tombstone::{TombstoneParams, perform_clone_tombstone};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::copy_source::{clone_source_view, copy_source_row};
use super::entry::CloneWriteOutcome;
use super::probes::{ProbeTarget, fetch_source_row, probe_row_in_target};
use super::util::{synthetic_affected_response, write_err};
use crate::control::server::shared::session::ddl_buffer::buffered_clone_suppressions;
use crate::types::TxnId;

/// The clone-relevant shape of one document write.
enum DocWriteKind<'a> {
    Update {
        document_id: &'a str,
        /// Target-side surrogate carried by the plan, used to probe the clone.
        /// `None` when the key has no target binding.
        surrogate: Option<Surrogate>,
    },
    Delete {
        document_id: &'a str,
        /// See `Update::surrogate`.
        surrogate: Option<Surrogate>,
    },
    /// Insert / put / upsert. Carries one id per row the statement writes.
    Insert { document_ids: Vec<&'a str> },
}

/// One document write reduced to what the CoW protocol needs.
struct DocWrite<'a> {
    collection_qualified: &'a str,
    kind: DocWriteKind<'a>,
}

/// Classify a plan into the CoW shape it needs, or `None` when the clone write
/// path has nothing to do for it.
fn classify(plan: &PhysicalPlan) -> Option<DocWrite<'_>> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            ..
        }) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Update {
                document_id: document_id.as_str(),
                surrogate: *surrogate,
            },
        }),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        }) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Delete {
                document_id: document_id.as_str(),
                surrogate: *surrogate,
            },
        }),
        PhysicalPlan::Document(
            DocumentOp::PointInsert {
                collection,
                document_id,
                ..
            }
            | DocumentOp::PointPut {
                collection,
                document_id,
                ..
            }
            | DocumentOp::Upsert {
                collection,
                document_id,
                ..
            },
        ) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Insert {
                document_ids: vec![document_id.as_str()],
            },
        }),
        PhysicalPlan::Document(
            DocumentOp::BatchInsert {
                collection,
                documents,
                ..
            }
            | DocumentOp::BatchUpsert {
                collection,
                documents,
                ..
            },
        ) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Insert {
                document_ids: documents.iter().map(|(id, _)| id.as_str()).collect(),
            },
        }),
        _ => None,
    }
}

/// Resolve the surrogate the source database bound to `document_id`, at the
/// source collection's home through the async routed exchange.
///
/// `None` means the source never held that primary key.
async fn source_surrogate(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    document_id: &str,
) -> crate::Result<Option<Surrogate>> {
    crate::control::server::surrogate_exchange::lookup_surrogate_routed(
        state,
        nodedb_types::CollectionKey::from_qualified_str(source_db_id, source_coll_qualified)?,
        tenant_id,
        document_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| write_err(format!("clone write source surrogate lookup: {e}")))
}

/// Point a `PointUpdate` plan at the surrogate the copy-up bound in the target
/// database. The plan resolved the pk read-only, so before the copy-up there
/// was no target binding to carry and the plan holds `None`.
fn retarget_point_update(plan: &mut PhysicalPlan, target: Surrogate) {
    if let PhysicalPlan::Document(DocumentOp::PointUpdate { surrogate, .. }) = plan {
        *surrogate = Some(target);
    }
}

/// Handle Document CoW write interception. Inside transaction `txn_id` the
/// presence probes read its overlay, and a source row the transaction already
/// tombstoned or copied up counts as gone from the source.
pub(super) async fn intercept_doc_clone_write(
    state: &SharedState,
    task: &mut PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    txn_id: Option<TxnId>,
) -> crate::Result<CloneWriteOutcome> {
    let Some(write) = classify(&task.plan) else {
        return Ok(CloneWriteOutcome::Passthrough);
    };

    let catalog = state.credentials.catalog();

    let db_id = task.database_id;
    let coll_name =
        nodedb_types::CollectionKey::from_qualified_str(db_id, write.collection_qualified)?.name();

    let desc = catalog
        .get_collection(db_id, tenant_id.as_u64(), coll_name)
        .map_err(|e| write_err(format!("clone write: get_collection: {e}")))?;
    let Some(desc) = desc else {
        return Ok(CloneWriteOutcome::Passthrough);
    };

    let Some(ref origin) = desc.cloned_from else {
        return Ok(CloneWriteOutcome::Passthrough);
    };
    match desc.clone_status {
        CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
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

    let source_db_id = origin.source_database;
    let source_coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        source_db_id,
        origin.source_collection.as_str(),
    );
    // Source rows this transaction's buffered tombstones and copy-ups hide,
    // and source rows a committed delete tombstoned.
    let target_qualified =
        crate::control::planner::sql_plan_convert::convert::db_qualified(db_id, coll_name);
    let mut hidden = buffered_clone_suppressions(&target_qualified).surrogates;
    hidden.extend(
        catalog
            .list_clone_tombstones(&target_qualified)
            .map_err(|e| write_err(format!("clone write: tombstone list: {e}")))?,
    );
    let view = clone_source_view(
        state,
        tenant_id,
        origin,
        desc.bitemporal,
        &source_coll_qualified,
    );
    let target = ProbeTarget {
        tenant_id,
        db_id,
        collection_qualified: write.collection_qualified,
        txn_id,
    };

    match write.kind {
        DocWriteKind::Insert { document_ids } => {
            for document_id in document_ids {
                let Some(surrogate) = source_surrogate(
                    state,
                    tenant_id,
                    source_db_id,
                    &source_coll_qualified,
                    document_id,
                )
                .await?
                else {
                    continue;
                };
                // A binding without a live row makes the tombstone a read no-op —
                // not worth a probe round trip per inserted key.
                perform_clone_tombstone(TombstoneParams {
                    tenant_id,
                    state,
                    target_db_id: db_id,
                    target_collection: coll_name,
                    source_surrogate: surrogate,
                })
                .await
                .map_err(|e| write_err(format!("clone insert tombstone: {e}")))?;
            }
            Ok(CloneWriteOutcome::Passthrough)
        }

        DocWriteKind::Delete {
            document_id,
            surrogate,
        } => {
            let row_in_target =
                probe_row_in_target(state, identity, target, document_id, surrogate)
                    .await
                    .map_err(|e| write_err(format!("clone write probe: {e}")))?;

            let src = source_surrogate(
                state,
                tenant_id,
                source_db_id,
                &source_coll_qualified,
                document_id,
            )
            .await?;

            // Tombstone regardless of target residency — after DELETE the clone
            // must never show the source copy again.
            if let Some(src) = src {
                perform_clone_tombstone(TombstoneParams {
                    tenant_id,
                    state,
                    target_db_id: db_id,
                    target_collection: coll_name,
                    source_surrogate: src,
                })
                .await
                .map_err(|e| write_err(format!("clone tombstone: {e}")))?;
            }

            if row_in_target {
                return Ok(CloneWriteOutcome::Passthrough);
            }

            // The source read decides rows-affected (1 or 0) — a resolved surrogate
            // is not evidence the row exists, since a surrogate outlives its row.
            // A row this transaction already hid is gone from the clone.
            let source_row = match src.filter(|src| !hidden.contains(&src.as_u32())) {
                Some(src) => fetch_source_row(state, identity, view, document_id, src)
                    .await
                    .map_err(|e| write_err(format!("clone delete source probe: {e}")))?,
                None => None,
            };

            Ok(CloneWriteOutcome::Handled(synthetic_affected_response(
                state.next_request_id(),
                Lsn::new(0),
                u64::from(source_row.is_some()),
            )))
        }

        DocWriteKind::Update {
            document_id,
            surrogate,
        } => {
            let row_in_target =
                probe_row_in_target(state, identity, target, document_id, surrogate)
                    .await
                    .map_err(|e| write_err(format!("clone write probe: {e}")))?;

            if row_in_target {
                return Ok(CloneWriteOutcome::Passthrough);
            }

            let Some(src) = source_surrogate(
                state,
                tenant_id,
                source_db_id,
                &source_coll_qualified,
                document_id,
            )
            .await?
            else {
                return Ok(CloneWriteOutcome::Passthrough);
            };
            // A row this transaction already hid updates nothing.
            if hidden.contains(&src.as_u32()) {
                return Ok(CloneWriteOutcome::Passthrough);
            }

            let source_row = copy_source_row(state, identity, view, document_id, src)
                .await
                .map_err(|e| write_err(format!("clone write fetch source: {e}")))?;

            let Some(source_row) = source_row else {
                return Ok(CloneWriteOutcome::Passthrough);
            };

            let target_surrogate = perform_clone_copyup(CopyUpParams {
                state,
                tenant_id,
                target_db_id: db_id,
                target_collection: coll_name,
                source_surrogate: src,
                source_doc_id: document_id.to_string(),
                source_row,
            })
            .await
            .map_err(|e| write_err(format!("clone copyup: {e}")))?;

            // The copied-up row lives under a target surrogate the plan
            // cannot know: hand it to the passthrough dispatch so the
            // UPDATE lands on that row instead of an unbound key.
            retarget_point_update(&mut task.plan, target_surrogate);
            Ok(CloneWriteOutcome::Passthrough)
        }
    }
}

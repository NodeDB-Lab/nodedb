// SPDX-License-Identifier: BUSL-1.1

//! A predicate `UPDATE` or `DELETE` whose collection fires a BEFORE, INSTEAD
//! OF or SYNC AFTER row body, in its statement's transaction.
//!
//! A row body binds one row, and a predicate write names no row until it
//! runs. The route therefore finds the rows the predicate matches first, as
//! the transaction sees them (its own staged writes included), and writes
//! each one by its primary key. Every point write then takes the row route:
//! it reads its OLD row, fires its bodies and stages.

use nodedb_physical::physical_plan::{
    DocumentOp, DocumentResolveOutcome, DocumentResolvedMutation,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::columnar::{DocumentMode, StrictSchema};
use nodedb_types::{CollectionType, RlsWriteCheck, Surrogate};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::server::shared::sql::staging_predicates::StagedTagKind;
use crate::control::target_identity::{TargetPk, derive_document_id, resolve_target_pk};
use crate::control::trigger::TriggerScope;
use crate::control::trigger::statement_txn::fires_joined_body;

use super::joined::task_events;
use super::route::TxnTaskContext;

/// The point writes a predicate `UPDATE` or `DELETE` in `task` expands to,
/// with the tag the statement renders. `None` for any other task, and for a
/// predicate write whose collection fires no row body.
pub(super) async fn expand_predicate_write(
    ctx: &TxnTaskContext<'_>,
    task: &PhysicalTask,
) -> crate::Result<Option<(Vec<PhysicalTask>, StagedTagKind)>> {
    let (collection, kind) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) => {
            (collection, StagedTagKind::Update)
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) => {
            (collection, StagedTagKind::Delete)
        }
        _ => return Ok(None),
    };
    let Some((events_collection, events)) = task_events(task) else {
        return Ok(None);
    };
    let scope = TriggerScope {
        database_id: task.database_id,
        tenant_id: task.tenant_id,
    };
    if !events
        .into_iter()
        .any(|event| fires_joined_body(ctx.state, scope, &events_collection, event))
    {
        return Ok(None);
    }

    let matched = matched_rows(ctx, task).await?;
    if matched.is_empty() {
        return Ok(Some((Vec::new(), kind)));
    }
    let bare = nodedb_types::CollectionKey::from_qualified(task.database_id, collection)?.name();
    let entry = ctx
        .state
        .credentials
        .catalog()
        .get_collection(task.database_id, task.tenant_id.as_u64(), bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: task.tenant_id,
            collection: collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&entry, "predicate write")?;
    let strict = match &entry.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => Some(schema),
        _ => None,
    };

    let mut ops = Vec::with_capacity(matched.len());
    for (surrogate, pre_image) in matched {
        let document_id = row_document_id(&target_pk, strict, &pre_image, surrogate)?;
        ops.push(PhysicalTask {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: point_write(&task.plan, document_id, surrogate)?,
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
    }
    Ok(Some((ops, kind)))
}

/// `(surrogate, pre-image)` of every row the predicate matches in the
/// transaction. The probe is a read-only `BulkDelete` resolve over the same
/// filters: it reports each matched row's surrogate and stored bytes. It
/// writes nothing, so it decides no write policy: each point write carries
/// the statement's policy and is judged on its own row.
async fn matched_rows(
    ctx: &TxnTaskContext<'_>,
    task: &PhysicalTask,
) -> crate::Result<Vec<(Surrogate, Vec<u8>)>> {
    let (collection, filters, declared_primary_key, system_time) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            filters,
            declared_primary_key,
            system_time,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            filters,
            declared_primary_key,
            system_time,
            ..
        }) => (collection, filters, declared_primary_key, system_time),
        _ => {
            return Err(crate::Error::Internal {
                detail: "the predicate probe was handed a write that is not a predicate write"
                    .into(),
            });
        }
    };
    let probe =
        PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(DocumentOp::BulkDelete {
            collection: collection.clone(),
            filters: filters.clone(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: declared_primary_key.clone(),
            // The write's own match scope: the block's snapshot.
            system_time: *system_time,
        })));
    let txn_id = ctx.txn.sessions.tx_id(ctx.txn.session_id);
    let response = dispatch_local(
        ctx.state,
        task.tenant_id,
        task.database_id,
        collection.as_str(),
        probe,
        txn_id,
    )
    .await?;
    let payload = payload_or_typed_error(response)?;
    let outcome: DocumentResolveOutcome =
        zerompk::from_msgpack(&payload).map_err(|e| crate::Error::Codec {
            detail: format!("predicate probe on '{collection}': {e}"),
        })?;
    outcome
        .mutations
        .into_iter()
        .map(|mutation| match mutation {
            DocumentResolvedMutation::Delete {
                surrogate,
                precondition: Some(pre_image),
                ..
            } => Ok((surrogate, pre_image)),
            other => Err(crate::Error::Internal {
                detail: format!("the predicate probe answered a row without its image: {other:?}"),
            }),
        })
        .collect()
}

/// The primary key of a matched row, read from its stored image: a strict
/// row's key columns decode through the tuple schema it was written with.
fn row_document_id(
    target_pk: &TargetPk,
    strict: Option<&StrictSchema>,
    pre_image: &[u8],
    surrogate: Surrogate,
) -> crate::Result<String> {
    let (Some(schema), TargetPk::Field { name, .. }) = (strict, target_pk) else {
        return Ok(derive_document_id(target_pk, pre_image, surrogate));
    };
    let decoder = nodedb_strict::TupleDecoder::new(schema);
    let version = decoder
        .schema_version(pre_image)
        .map_err(|e| strict_error(name, &e))?;
    let written = if version < schema.version {
        schema.schema_for_version(version)
    } else {
        schema.clone()
    };
    let value = nodedb_strict::TupleDecoder::new(&written)
        .extract_by_name(pre_image, name)
        .map_err(|e| strict_error(name, &e))?;
    nodedb_types::value_to_pk_string(&value).ok_or_else(|| crate::Error::Internal {
        detail: format!("strict row key column '{name}' holds no key value"),
    })
}

fn strict_error(column: &str, error: &nodedb_strict::StrictError) -> crate::Error {
    crate::Error::Internal {
        detail: format!("decode strict row key column '{column}': {error}"),
    }
}

/// The point write one matched row of `plan` becomes.
fn point_write(
    plan: &PhysicalPlan,
    document_id: String,
    surrogate: Surrogate,
) -> crate::Result<PhysicalPlan> {
    let pk_bytes = document_id.clone().into_bytes();
    Ok(match plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
            ..
        }) => PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: collection.clone(),
            document_id,
            surrogate: Some(surrogate),
            pk_bytes,
            updates: updates.clone(),
            returning: returning.clone(),
            rls_filters: rls_filters.clone(),
            rls_write_check: rls_write_check.clone(),
            resolved_sum_targets: resolved_sum_targets.clone(),
            declared_primary_key: declared_primary_key.clone(),
        }),
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            ..
        }) => PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: collection.clone(),
            document_id,
            surrogate: Some(surrogate),
            pk_bytes,
            returning: returning.clone(),
            rls_filters: rls_filters.clone(),
            rls_write_check: rls_write_check.clone(),
            resolved_sum_targets: resolved_sum_targets.clone(),
        }),
        _ => {
            return Err(crate::Error::Internal {
                detail: "a point write was derived from a write that is not a predicate write"
                    .into(),
            });
        }
    })
}

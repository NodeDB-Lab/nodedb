// SPDX-License-Identifier: BUSL-1.1

//! Apply one resolved document mutation inside its statement's transaction.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::{RlsWriteCheck, Surrogate};
use redb::WriteTransaction;

use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::{WriteSetRow, removed_row_image};
use crate::data::executor::enforcement::chain_guard::ChainGuard;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::enforcement::unique::UniqueJudge;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

use super::apply_state::{ResolvedApplyState, ResolvedEvent};

pub(super) struct ApplyResolvedPut<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub precondition: Option<&'a [u8]>,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub rls_write_check: &'a RlsWriteCheck,
}

pub(super) struct ApplyResolvedDelete<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub rls_write_check: &'a RlsWriteCheck,
}

impl CoreLoop {
    /// Prepare, admit, and install one row without committing or publishing.
    pub(super) fn apply_resolved_document_put(
        &mut self,
        task: &ExecutionTask,
        txn: &WriteTransaction,
        state: &mut ResolvedApplyState,
        put: ApplyResolvedPut<'_>,
    ) -> crate::Result<()> {
        let database_id = task.request.database_id.as_u64();
        let storage_key = StorageKey::for_surrogate(put.surrogate);
        let identity = RowIdentity::from_user_key(put.document_id);
        state.touched.push((put.collection.to_owned(), storage_key));
        let mut chain = ChainGuard::begin(self, database_id, put.tid, put.collection);
        let result = (|| -> crate::Result<()> {
            if chain.enabled()
                && self
                    .current_row_body_in_txn(
                        txn,
                        database_id,
                        put.tid,
                        put.collection,
                        &storage_key,
                    )?
                    .is_none()
            {
                chain.chain_insert(self, put.surrogate, put.value)?;
            }
            let prepared = self.prepare_point_put(
                txn,
                PointPutParams {
                    database_id,
                    tid: put.tid,
                    collection: put.collection,
                    storage_key,
                    surrogate: put.surrogate,
                    value: put.value,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: UniqueJudge::Unit,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: put.resolved_sum_targets,
                    // Resolve already judged assigned fields on existing rows.
                    guards: if put.precondition.is_none() {
                        GuardScope::Document
                    } else {
                        GuardScope::InjectOnly
                    },
                },
            )?;
            rls_write_gate::admit_installed_row(
                put.rls_write_check,
                prepared.installed(),
                &identity,
                &self.identity_column(database_id, put.tid, put.collection),
                put.tid,
                put.collection,
                &self.crs_context(),
            )?;
            let mut outcome = self.apply_prepared_point_put(txn, prepared)?;
            state.undo.append(&mut outcome.memory_undo);
            chain.settle_installed(self, put.surrogate, &mut outcome.installed)?;
            chain.persist_head(self, txn)?;

            let event = self.prepare_installed_put_event(
                &WriteSetRow {
                    database_id,
                    tid: put.tid,
                    collection: put.collection,
                    identity: identity.clone(),
                },
                &outcome.installed,
                outcome.prior_value.as_deref(),
            )?;
            state.write_set.push(
                WriteSetEntry::installed_put(
                    put.surrogate.as_u32(),
                    identity.clone(),
                    outcome.installed.clone(),
                    event.clone(),
                )
                .in_collection(put.collection.to_owned()),
            );
            state.events.push(ResolvedEvent::Put {
                collection: put.collection.to_owned(),
                identity,
                event,
            });
            // Current-state effects never run for a carried historical version.
            if outcome.history.is_none() {
                let images = match outcome.prior_value.as_deref() {
                    Some(old) => WriteImages::Update {
                        old: ImageBody::Stored(old),
                        new: ImageBody::Stored(&outcome.installed.stored),
                    },
                    None => WriteImages::Insert {
                        new: ImageBody::Stored(&outcome.installed.stored),
                    },
                };
                let enforcement = write_hook::run(
                    self,
                    txn,
                    &HookCtx {
                        database_id,
                        tid: put.tid,
                        collection: put.collection,
                        resolved_targets: put.resolved_sum_targets,
                        deferred_sum_targets: &[],
                        wal_lsn: task.wal_lsn(),
                        target_unique: UniqueJudge::Unit,
                    },
                    images,
                )?;
                state.retain_enforcement(put.collection, enforcement);
            }
            Ok(())
        })();
        // Retain even an unsettled guard so the statement clears its pending intent on refusal.
        state.chains.push(chain);
        result
    }

    /// Install one removal and queue its publication inside the statement boundary.
    pub(super) fn apply_resolved_document_delete(
        &mut self,
        task: &ExecutionTask,
        txn: &WriteTransaction,
        state: &mut ResolvedApplyState,
        del: ApplyResolvedDelete<'_>,
    ) -> crate::Result<()> {
        let database_id = task.request.database_id.as_u64();
        let storage_key = StorageKey::for_surrogate(del.surrogate);
        let identity = RowIdentity::from_user_key(del.document_id);
        state.touched.push((del.collection.to_owned(), storage_key));
        if let Some(prior) =
            self.current_row_body_in_txn(txn, database_id, del.tid, del.collection, &storage_key)?
        {
            let schema = self.capture_storage_schema(&(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(del.tid),
                del.collection.to_owned(),
            ))?;
            let strict_schema = match schema.as_ref() {
                nodedb_physical::physical_plan::StorageMode::Schemaless => None,
                nodedb_physical::physical_plan::StorageMode::Strict { schema } => Some(schema),
            };
            rls_write_gate::admit_stored_row(
                del.rls_write_check,
                rls_write_gate::GatedRow {
                    body: &prior,
                    identity: &identity,
                    strict_schema,
                    identity_column: &self.identity_column(database_id, del.tid, del.collection),
                },
                del.tid,
                del.collection,
                &self.crs_context(),
            )?;
        }
        let mut outcome = self.apply_point_delete(
            txn,
            PointDeleteParams {
                database_id,
                tid: del.tid,
                collection: del.collection,
                document_id: del.document_id,
                surrogate: del.surrogate,
                user_roles: &task.request.user_roles,
                enforce: true,
                resolved_targets: del.resolved_sum_targets,
            },
        )?;
        state.undo.append(&mut outcome.memory_undo);
        if let Some(prior) = outcome.prior_value.as_deref() {
            let old_value = self
                .stored_event_image(
                    database_id,
                    del.tid,
                    del.collection,
                    identity.as_str(),
                    prior,
                )
                .map_err(|_| crate::Error::Serialization {
                    format: "binary_tuple".into(),
                    detail: format!(
                        "deleted event row '{}' of '{}' cannot decode before statement commit",
                        identity.as_str(),
                        del.collection
                    ),
                })?;
            state.events.push(ResolvedEvent::Delete {
                collection: del.collection.to_owned(),
                identity: identity.clone(),
                old_value,
            });
            if let Some(sys_from) = outcome.version_sys_from {
                state.write_set.push(
                    removed_row_image(del.surrogate.as_u32(), identity, sys_from)
                        .in_collection(del.collection.to_owned()),
                );
            }
            if outcome.history.is_none() {
                let enforcement = write_hook::run(
                    self,
                    txn,
                    &HookCtx {
                        database_id,
                        tid: del.tid,
                        collection: del.collection,
                        resolved_targets: del.resolved_sum_targets,
                        deferred_sum_targets: &[],
                        wal_lsn: task.wal_lsn(),
                        target_unique: UniqueJudge::Unit,
                    },
                    WriteImages::Delete {
                        old: ImageBody::Stored(prior),
                    },
                )?;
                state.retain_enforcement(del.collection, enforcement);
            }
        }
        Ok(())
    }
}

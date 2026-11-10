// SPDX-License-Identifier: BUSL-1.1

//! One actual in-transaction preparation shared by RLS and document application.

use super::{
    materialized::MaterializedDocumentPut,
    types::{PointPutOutcome, PointPutParams},
};
use crate::{data::executor::core_loop::CoreLoop, types::SharedInstalledDocument};
use redb::WriteTransaction;

/// The immutable row and write policy selected immediately before application.
pub(in crate::data::executor) struct PreparedPointPut<'a> {
    materialized: MaterializedDocumentPut<'a>,
}
impl PreparedPointPut<'_> {
    pub(in crate::data::executor) fn installed(&self) -> &SharedInstalledDocument {
        &self.materialized.installed
    }
}
impl CoreLoop {
    /// Select this operation's exact row once, while its caller owns the transaction.
    pub(in crate::data::executor) fn prepare_point_put<'a>(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'a>,
    ) -> crate::Result<PreparedPointPut<'a>> {
        let config_key = (
            crate::types::DatabaseId::new(params.database_id),
            crate::types::TenantId::new(params.tid),
            params.collection.to_string(),
        );
        if matches!(
            self.chain
                .intents
                .get(&(config_key.clone(), params.surrogate.as_u32())),
            Some(crate::data::executor::enforcement::chain_guard::ChainIntent::Keep { .. })
        ) {
            return self.prepare_point_put_body(txn, params, None, false);
        }
        let frozen = self.freeze_document_body(super::FreezeDocumentBodyInput {
            config_key: &config_key,
            surrogate: params.surrogate,
            value: params.value,
            guards: params.guards,
        })?;
        self.prepare_point_put_body(txn, params, Some(&frozen), false)
    }

    pub(super) fn prepare_point_put_body<'a>(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'a>,
        frozen: Option<&super::FrozenDocumentBody>,
        fresh: bool,
    ) -> crate::Result<PreparedPointPut<'a>> {
        let PointPutParams {
            database_id,
            tid,
            collection,
            storage_key,
            surrogate,
            value,
            index_text,
            user_roles,
            enforce,
            unique,
            wal_lsn,
            resolved_targets,
            guards,
        } = params;
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );

        if frozen.is_some()
            && let Some(crate::data::executor::enforcement::chain_guard::ChainIntent::Link { head }) =
                self.chain
                    .intents
                    .get(&(config_key.clone(), surrogate.as_u32()))
        {
            let predecessor = self
                .sparse
                .get_chain_head_in_txn(txn, database_id, tid, collection)?
                .unwrap_or_else(crate::types::hash_chain::ChainHead::genesis);
            if *head != predecessor {
                return Err(crate::Error::DataPlane(
                    crate::bridge::envelope::ErrorCode::ConflictRetry,
                ));
            }
        }

        // Every write appends a version of the row. A stamp in
        // `apply_scope.bitemporal_stamps` (WAL redo replay, a committed
        // redo install) fixes the version at the EXACT stamp the live write
        // used, independent of `doc_configs` (empty during replay). Absent a
        // stamp the write takes its system-time ordinal, raised above the
        // row's newest version. A `bitemporal=true` collection carries the
        // stamp into its strict tuple's temporal columns. Any other
        // collection's body carries no temporal column.
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let carried = if fresh {
            None
        } else {
            self.apply_scope
                .bitemporal_stamps
                .get(&surrogate.as_u32())
                .copied()
        };
        let (stamped, sys_from, valid_from_ms, valid_until_ms) = match carried {
            Some(stamp) => (
                true,
                stamp.sys_from,
                stamp.valid_from_ms,
                stamp.valid_until_ms,
            ),
            None => (
                bitemporal,
                self.row_version_ordinal_in_txn(txn, database_id, tid, collection, &storage_key)?,
                i64::MIN,
                i64::MAX,
            ),
        };

        // Shared with the governed-write RESOLVE pass.
        let body = match frozen {
            Some(frozen) => {
                self.complete_frozen_body(frozen, stamped, sys_from, valid_from_ms, valid_until_ms)?
            }
            None => self.build_stored_body(super::stored_body::StoredBodyInput {
                config_key: &config_key,
                surrogate,
                value,
                bitemporal: stamped,
                sys_from,
                valid_from_ms,
                valid_until_ms,
                guards,
            })?,
        };
        let schema = self.capture_storage_schema(&config_key)?;
        let installed = crate::types::InstalledDocument {
            canonical: body.value,
            stored: body.stored,
            version: crate::bridge::envelope::RowVersion {
                sys_from,
                valid_from_ms,
                valid_until_ms,
            },
            schema,
            chain_head: None,
        };
        Ok(PreparedPointPut {
            materialized: super::materialized::MaterializedDocumentPut {
                row: super::types::InstalledDocumentRow {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    surrogate,
                    wal_lsn,
                },
                installed,
                index_text,
                user_roles,
                enforce,
                unique,
                resolved_targets,
                check_history: carried.is_some(),
            },
        })
    }

    /// Apply the prepared image and original policy without deriving another body.
    pub(in crate::data::executor) fn apply_prepared_point_put(
        &mut self,
        txn: &WriteTransaction,
        prepared: PreparedPointPut<'_>,
    ) -> crate::Result<PointPutOutcome> {
        self.apply_materialized_document(txn, prepared.materialized)
    }
}

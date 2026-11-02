// SPDX-License-Identifier: BUSL-1.1

//! A put installed below the row's newest stored version (see
//! `handlers::point::history_install`).

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::point::history_install::{HistoryIndex, HistorySlot};
use crate::engine::document::store::StorageKey;
use crate::engine::sparse::btree_versioned::VersionedPut;
use crate::types::{DatabaseId, TenantId};

use super::types::PointPutOutcome;

/// One put a carried stamp places below the row's newest version.
pub(in crate::data::executor::handlers::point) struct HistoryPut<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: &'a StorageKey,
    /// The body the write submitted, guards applied.
    pub value: &'a [u8],
    /// The bytes the version stores.
    pub stored: Vec<u8>,
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub slot: HistorySlot,
}

impl CoreLoop {
    /// Store the version at its ordinal and write its index entries against
    /// its neighbours. Runs no current-state side effect. Does NOT commit.
    pub(in crate::data::executor::handlers::point) fn apply_history_put(
        &self,
        txn: &WriteTransaction,
        put: HistoryPut<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let HistoryPut {
            database_id,
            tid,
            collection,
            storage_key,
            value,
            stored,
            sys_from,
            valid_from_ms,
            valid_until_ms,
            slot,
        } = put;
        self.sparse.versioned_put_in_txn(
            txn,
            VersionedPut {
                database_id,
                tenant: tid,
                coll: collection,
                doc_id: storage_key,
                sys_from,
                valid_from_ms,
                valid_until_ms,
                body: &stored,
            },
        )?;
        // An indexed collection's body must decode, as on the current path.
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let doc = match self.doc_configs.get(&config_key) {
            Some(config) if !config.index_paths.is_empty() => {
                Some(doc_format::decode_document(value)?)
            }
            _ => None,
        };
        let index = self.write_history_index_in_txn(
            txn,
            HistoryIndex {
                database_id,
                tid,
                collection,
                storage_key,
                sys_from,
                doc: doc.as_ref(),
                slot: &slot,
            },
        )?;
        Ok(PointPutOutcome {
            prior_value: slot.predecessor,
            stored_value: stored,
            version_sys_from: sys_from,
            index_tuples: index.at_version,
            memory_undo: Vec::new(),
            stats_prior: Vec::new(),
            history: Some(index.at_successor),
        })
    }
}

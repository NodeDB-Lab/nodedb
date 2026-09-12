// SPDX-License-Identifier: BUSL-1.1

//! Shared routing context for a single staged point write.

use nodedb_types::{RowIdentity, Surrogate};

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Collection overlay key: `(database, tenant, collection)`.
pub(super) type CollKey = (DatabaseId, TenantId, String);

/// The invariant routing identity of one staged point write, bundled so the
/// per-op helpers stay within a sane argument count.
///
/// `document_id` is the row's client identity, the overlay's doc-id key.
/// A Document op carries the plan's resolved identity. A KV op carries
/// [`kv_row_identity`](super::stage_kv::kv_row_identity) of its raw key.
pub(in crate::data::executor) struct StageCtx<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub database_id: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub document_id: RowIdentity,
    pub surrogate: Surrogate,
    pub coll_key: CollKey,
}

impl<'a> StageCtx<'a> {
    pub(in crate::data::executor) fn new(
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        document_id: RowIdentity,
        surrogate: Surrogate,
    ) -> Self {
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        Self {
            task,
            tid,
            database_id: task.request.database_id.as_u64(),
            txn_id,
            collection,
            document_id,
            surrogate,
            coll_key,
        }
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::VerifyHashChain`: walk one collection's hash chain over its raw
//! stored rows and report the first break.
//!
//! The walk runs here, where the stored bytes live, so every link is
//! recomputed from the exact contents the insert hashed. The response payload
//! is a MessagePack [`ChainVerdict`].

use nodedb_types::StorageKey;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_verify::ChainWalk;
use crate::data::executor::enforcement::hash_chain::{self, ChainHead};
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::types::hash_chain::ChainVerdict;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_verify_hash_chain(
        &self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        let verdict = self.walk_hash_chain(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
        );
        let payload = verdict.and_then(|verdict| {
            zerompk::to_msgpack_vec(&verdict).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("hash-chain verdict: {e}"),
            })
        });
        match payload {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Walk `collection`'s chain from genesis against its persisted head.
    pub(in crate::data::executor) fn walk_hash_chain(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<ChainVerdict> {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let config =
            self.doc_configs
                .get(&key)
                .ok_or_else(|| crate::Error::CollectionNotFound {
                    tenant_id: key.1,
                    collection: collection.to_string(),
                })?;
        let head = self
            .chain
            .heads
            .get(&key)
            .cloned()
            .unwrap_or_else(ChainHead::genesis);
        let mut walk = ChainWalk::new(head);
        self.for_each_chain_row(database_id, tid, collection, config, |row_id, view| {
            walk.index(row_id, hash_chain::stored_link(view).as_ref());
        })?;
        self.for_each_chain_row(database_id, tid, collection, config, |row_id, view| {
            if let Some(link) = hash_chain::stored_link(view) {
                walk.check(row_id, &link, &hash_chain::canonical_contents(view));
            }
        })?;
        Ok(walk.finish())
    }

    /// Visit every current row of `collection` as `(row id, decoded view)`.
    ///
    /// A bitemporal collection's current rows live on the versioned table.
    fn for_each_chain_row(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        config: &CollectionConfig,
        mut visit: impl FnMut(&str, &serde_json::Value),
    ) -> crate::Result<()> {
        let mut decode_and_visit = |key: &StorageKey, bytes: &[u8]| -> crate::Result<()> {
            let view = self.decode_stored_document(config, bytes)?;
            visit(&key.to_string(), &view);
            Ok(())
        };
        self.for_each_current_row(database_id, tid, collection, |key, bytes| {
            decode_and_visit(key, bytes).map(|()| std::ops::ControlFlow::Continue(()))
        })
    }
}

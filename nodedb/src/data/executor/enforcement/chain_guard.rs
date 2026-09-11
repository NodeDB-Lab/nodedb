// SPDX-License-Identifier: BUSL-1.1

//! Advancing — and un-advancing — a collection's hash-chain head around one
//! INSERT.
//!
//! The chain rewrites the row BODY (it injects `_chain_hash`), so it must run
//! BEFORE the body is encoded and stored, which is why it is a pre-write call
//! and not part of the image-folding funnel. What makes it need a guard rather
//! than a function call is the head: it is mutated in memory before the write
//! is known to succeed, and persisted inside the write's own transaction. Every
//! path that abandons the write between those two points has to put the head
//! back, and there is exactly one correct pre-image to put back — the one
//! captured before the advance.
//!
//! Holding the capture in a value the caller carries is what makes "restore it"
//! a single call at each abort site instead of a rule each handler re-derives.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::hash_chain;
use crate::types::{DatabaseId, TenantId};

/// A collection's hash-chain head across one write.
pub(in crate::data::executor) struct ChainGuard {
    /// Key the head is tracked under, in memory and on disk.
    key: (DatabaseId, TenantId, String),
    /// Whether the collection declares `HASH_CHAIN`.
    enabled: bool,
    /// The head as it stood BEFORE this write. `None` = not a hash-chain
    /// collection; `Some(None)` = no prior head (genesis); `Some(Some(prev))` =
    /// prior head present.
    prior: Option<Option<String>>,
    /// Whether this write actually advanced the head.
    mutated: bool,
}

impl ChainGuard {
    /// Capture the head pre-image before anything touches it.
    pub(in crate::data::executor) fn begin(
        core: &CoreLoop,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Self {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let enabled = core
            .doc_configs
            .get(&key)
            .is_some_and(|c| c.enforcement.hash_chain);
        let prior = if enabled {
            Some(core.chain_hashes.get(&key).cloned())
        } else {
            None
        };
        Self {
            key,
            enabled,
            prior,
            mutated: false,
        }
    }

    /// Whether the collection declares `HASH_CHAIN`.
    pub(in crate::data::executor) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Link one INSERT into the chain, returning the body to store.
    ///
    /// `Ok(None)` means the chain is disabled and the caller stores the
    /// submitted body unchanged.
    pub(in crate::data::executor) fn chain_insert(
        &mut self,
        core: &mut CoreLoop,
        database_id: u64,
        tid: u64,
        document_id: &str,
        value: &[u8],
    ) -> crate::Result<Option<Vec<u8>>> {
        let chained = hash_chain::apply_chain_on_insert(
            &mut core.chain_hashes,
            database_id,
            tid,
            &self.key.2,
            document_id,
            value,
            self.enabled,
        )?;
        if chained.is_some() {
            self.mutated = true;
        }
        Ok(chained)
    }

    /// Persist the advanced head inside the caller's write transaction.
    ///
    /// Head and row commit or roll back as one atomic unit: a head that can
    /// advance without its row (or a row that lands without its head) is the
    /// broken-chain bug persistence exists to prevent. A no-op when this write
    /// advanced nothing.
    pub(in crate::data::executor) fn persist_head(
        &self,
        core: &CoreLoop,
        txn: &WriteTransaction,
    ) -> crate::Result<()> {
        if !self.mutated {
            return Ok(());
        }
        let Some(head) = core.chain_hashes.get(&self.key).cloned() else {
            return Ok(());
        };
        core.sparse.put_chain_head_in_txn(
            txn,
            self.key.0.as_u64(),
            self.key.1.as_u64(),
            &self.key.2,
            &head,
        )
    }

    /// Put the captured head pre-image back after an abandoned write.
    ///
    /// In-memory only, and correctly so: every caller aborts before its write
    /// transaction commits, so the persisted head was never written. Reversing
    /// a head that already reached disk is the rollback path's job
    /// (`undo_chain_hash`).
    pub(in crate::data::executor) fn restore(&self, core: &mut CoreLoop) {
        if !self.mutated {
            return;
        }
        match &self.prior {
            Some(None) => {
                core.chain_hashes.remove(&self.key);
            }
            Some(Some(prev)) => {
                core.chain_hashes.insert(self.key.clone(), prev.clone());
            }
            None => {}
        }
    }

    /// The head pre-image a durable undo entry restores on rollback.
    pub(in crate::data::executor) fn prior(&self) -> Option<Option<String>> {
        self.prior.clone()
    }
}

/// Undo the in-memory side effects an abort AFTER `apply_point_put` leaves
/// behind, before the caller drops its transaction uncommitted.
///
/// `apply_point_put` populates the read-through document cache with the body it
/// wrote. Dropping the redb transaction reverses the durable write but not that
/// cache entry, so every subsequent read of the row would be served the
/// post-image of a write that never landed — a row visible to readers and
/// absent from storage. Restoring the hash-chain head is the same class of
/// in-memory reversal, so both happen here rather than one being remembered at
/// each abort site and the other forgotten.
pub(in crate::data::executor) fn abort_after_apply(
    core: &mut CoreLoop,
    guard: &ChainGuard,
    database_id: u64,
    tid: u64,
    collection: &str,
    row_key: &crate::engine::document::store::StorageKey,
) {
    guard.restore(core);
    core.doc_cache
        .invalidate(database_id, tid, collection, row_key);
}

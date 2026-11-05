// SPDX-License-Identifier: BUSL-1.1

//! Shared state reads and mutation constructors for resolving a governed KV
//! write. Every resolver here reads state and computes images; none writes.
//! Each mutation carries a `precondition` (exactly what it read) so the
//! apply can refuse a resolution state has moved past.

use nodedb_physical::physical_plan::{KvResolveOutcome, KvResolvedMutation};
use nodedb_types::DatabaseId;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;

/// What a resolver returns: the decided mutations and the decided reply, or
/// the error the live handler would have returned for the same input.
pub(super) type ResolveResult = Result<KvResolveOutcome, ErrorCode>;

impl CoreLoop {
    /// The stored body of `key`, or `None` when absent or expired. Becomes
    /// the mutation's `precondition`: `Some` requires exact bytes, `None`
    /// requires the key to still be absent.
    pub(super) fn kv_resolve_read(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        self.kv_engine.get(did, tid, collection, key, now_ms)
    }

    /// The absolute expiry instant `key` currently carries, or `0` for none.
    /// `atomic_put` preserves an existing TTL where `KvEngine::put` clears
    /// it, so a resolved `Put` must read it here, not infer from `ttl_ms == 0`.
    pub(super) fn kv_resolve_preserved_expiry(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) -> u64 {
        match self.kv_engine.get_ttl_meta(did, tid, collection, key) {
            Some(meta) if meta.has_ttl => meta.expire_at_ms,
            _ => 0,
        }
    }
}

/// Bundled inputs for [`put_mutation`] — a plain positional list of seven
/// exceeds clippy's arity threshold.
pub(super) struct ResolvedPut<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: Vec<u8>,
    pub ttl_ms: u64,
    pub expire_at_ms: u64,
    pub surrogate: nodedb_types::Surrogate,
    pub precondition: Option<Vec<u8>>,
}

/// Build the `Put` mutation a resolved write applies.
pub(super) fn put_mutation(
    database_id: DatabaseId,
    put: ResolvedPut<'_>,
) -> Result<KvResolvedMutation, ErrorCode> {
    let collection = qualified(database_id, put.collection)?;
    Ok(KvResolvedMutation::Put {
        collection,
        key: put.key.to_vec(),
        value: put.value,
        ttl_ms: put.ttl_ms,
        expire_at_ms: put.expire_at_ms,
        surrogate: put.surrogate,
        precondition: put.precondition,
    })
}

/// The stored collection name for `collection`, or the error a name that
/// does not match `database_id` raises.
pub(super) fn qualified(
    database_id: DatabaseId,
    collection: &str,
) -> Result<nodedb_types::QualifiedCollection, ErrorCode> {
    nodedb_types::QualifiedCollection::from_stored(database_id, collection.to_owned())
        .map_err(|e| ErrorCode::from(crate::Error::from(e)))
}

/// Bundled inputs for [`rewrite_mutation`]: a new value for a row the key
/// already holds. It carries no surrogate: the row keeps its bound identity.
pub(super) struct ResolvedRewrite<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: Vec<u8>,
    pub ttl_ms: u64,
    pub expire_at_ms: u64,
    pub precondition: Vec<u8>,
}

/// Build the `Rewrite` mutation a resolved write applies.
pub(super) fn rewrite_mutation(
    database_id: DatabaseId,
    rewrite: ResolvedRewrite<'_>,
) -> Result<KvResolvedMutation, ErrorCode> {
    let collection = qualified(database_id, rewrite.collection)?;
    Ok(KvResolvedMutation::Rewrite {
        collection,
        key: rewrite.key.to_vec(),
        value: rewrite.value,
        ttl_ms: rewrite.ttl_ms,
        expire_at_ms: rewrite.expire_at_ms,
        precondition: rewrite.precondition,
    })
}

/// Build the `Delete` mutation a resolved write applies.
pub(super) fn delete_mutation(
    database_id: DatabaseId,
    collection: &str,
    key: &[u8],
    precondition: Option<Vec<u8>>,
) -> Result<KvResolvedMutation, ErrorCode> {
    let collection = qualified(database_id, collection)?;
    Ok(KvResolvedMutation::Delete {
        collection,
        key: key.to_vec(),
        precondition,
    })
}

/// Absolute expiry for a write that installs `ttl_ms` from `now_ms`, matching
/// `KvEngine::put`: a zero TTL means no expiry at all, not "expire now".
pub(super) fn expiry_from_ttl(ttl_ms: u64, now_ms: u64) -> u64 {
    if ttl_ms == 0 { 0 } else { now_ms + ttl_ms }
}

/// The outcome of a resolver that decided exactly one mutation.
pub(super) fn one(mutation: KvResolvedMutation, response_payload: Vec<u8>) -> KvResolveOutcome {
    KvResolveOutcome {
        mutations: vec![mutation],
        response_payload,
    }
}

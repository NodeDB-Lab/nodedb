// SPDX-License-Identifier: BUSL-1.1

//! Shared state reads and mutation constructors for resolving a governed
//! document write. Every resolver here reads state and computes images; none
//! writes. Each mutation carries a `precondition` (the exact stored bytes
//! read) so the apply can refuse a resolution state has moved past.

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, DocumentResolvedMutation, ResolvedSumTarget, ReturningSpec, StorageMode,
};
use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// What a resolver returns: the decided mutations and the decided reply, or the
/// error the live handler would have returned for the same input.
pub(super) type ResolveResult = Result<DocumentResolveOutcome, ErrorCode>;

/// The per-collection facts every document resolver reads once. Held rather
/// than re-derived per row, or a bulk resolve pays both probes per match.
pub(super) struct DocResolveCtx {
    pub database_id: u64,
    pub tid: u64,
    /// `Some` exactly when the collection stores Binary Tuples.
    pub strict_schema: Option<StrictSchema>,
    pub bitemporal: bool,
}

impl CoreLoop {
    /// Read the per-collection facts a resolve pass needs.
    pub(super) fn doc_resolve_ctx(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> DocResolveCtx {
        let database_id = task.request.database_id.as_u64();
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self
            .doc_configs
            .get(&config_key)
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });
        DocResolveCtx {
            database_id,
            tid,
            strict_schema,
            bitemporal: self.is_bitemporal(database_id, tid, collection),
        }
    }

    /// The row's raw stored body, or `None` when absent. Reads through the
    /// same current-state view the live write path uses; becomes the
    /// mutation's `precondition`.
    pub(super) fn doc_resolve_read(
        &self,
        ctx: &DocResolveCtx,
        collection: &str,
        row_key: &StorageKey,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        self.doc_current_bytes(ctx.database_id, ctx.tid, collection, row_key)
    }

    /// The same current-state read, for a caller with no [`DocResolveCtx`] —
    /// the apply path, which checks one precondition per mutation.
    pub(in crate::data::executor) fn doc_current_bytes(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        row_key: &StorageKey,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let read = if self.is_bitemporal(database_id, tid, collection) {
            self.sparse
                .versioned_get_current(database_id, tid, collection, &row_key.to_string())
        } else {
            self.sparse.get(database_id, tid, collection, row_key)
        };
        read.map_err(ErrorCode::from)
    }
}

/// Bundled inputs for [`put_mutation`] — a plain positional list of six exceeds
/// clippy's arity threshold.
pub(super) struct ResolvedPut<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// Pre-encode MessagePack body — see `DocumentResolvedMutation`.
    pub value: Vec<u8>,
    /// Raw stored bytes read at resolve time, `None` for an absent row.
    pub precondition: Option<Vec<u8>>,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// One decided row write.
pub(super) fn put_mutation(put: ResolvedPut<'_>) -> DocumentResolvedMutation {
    DocumentResolvedMutation::Put {
        collection: nodedb_types::QualifiedCollection::from_stored(put.collection.to_owned()),
        document_id: put.document_id.to_owned(),
        surrogate: put.surrogate,
        pk_bytes: put.document_id.as_bytes().to_vec(),
        value: put.value,
        precondition: put.precondition,
        resolved_sum_targets: put.resolved_sum_targets.to_vec(),
    }
}

/// One decided row removal.
pub(super) fn delete_mutation(
    collection: &str,
    document_id: &str,
    surrogate: Surrogate,
    precondition: Option<Vec<u8>>,
    resolved_sum_targets: &[ResolvedSumTarget],
) -> DocumentResolvedMutation {
    DocumentResolvedMutation::Delete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        precondition,
        resolved_sum_targets: resolved_sum_targets.to_vec(),
    }
}

/// The storage key for a row identity — the form every document reader uses.
pub(super) fn row_key_of(surrogate: Surrogate) -> StorageKey {
    StorageKey::for_surrogate(surrogate)
}

/// The `{"affected": N}` reply a write with no `RETURNING` clause returns.
pub(super) fn affected_payload(affected: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
    nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", affected as i64);
    payload
}

/// The reply a write returns, decided at resolve time — built through the
/// same `RETURNING` projection live handlers use, over the stored image
/// resolve computed.
pub(super) fn resolved_response_payload(
    returning: Option<&ReturningSpec>,
    rls_filters: &[u8],
    strict_schema: Option<&StrictSchema>,
    rows: &[(&RowIdentity, &[u8])],
) -> Result<Vec<u8>, ErrorCode> {
    match returning {
        Some(spec) => {
            returning_rows::build_stored_rows_payload(spec, rls_filters, strict_schema, rows)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                })
        }
        None => Ok(affected_payload(rows.len())),
    }
}

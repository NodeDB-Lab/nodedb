// SPDX-License-Identifier: BUSL-1.1

//! Resolver for `BatchUpsert`. Judges every row as `execute_upsert_batch`
//! judges it and ships one `Put` per key the page writes. A key the page
//! names twice ships its final body, with the stored row the page read as
//! its precondition: the apply checks every precondition before it applies
//! any.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, ResolvedSumTarget, ReturningSpec, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

use super::context::{
    ResolveResult, ResolvedPut, put_mutation, resolved_response_payload, row_key_of,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::stored_body::StoredBodyInput;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::upsert::merge_upsert_body;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;

/// Borrowed arguments for [`CoreLoop::resolve_upsert_batch`].
pub(super) struct ResolveUpsertBatch<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [Surrogate],
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// One key the page writes.
struct ResolvedKey<'a> {
    document_id: &'a str,
    surrogate: Surrogate,
    /// The stored row the page read. `None` when the key was absent.
    precondition: Option<Vec<u8>>,
    /// The MessagePack body the page stores.
    body: Vec<u8>,
    /// The fields the merges wrote. `None` for an absent key.
    assigned: Option<Vec<String>>,
}

impl CoreLoop {
    /// Resolve a `BatchUpsert` to the row writes it would apply.
    pub(super) fn resolve_upsert_batch(
        &self,
        task: &ExecutionTask,
        args: ResolveUpsertBatch<'_>,
    ) -> ResolveResult {
        let ResolveUpsertBatch {
            tid,
            collection,
            documents,
            surrogates,
            on_conflict_updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        if surrogates.len() != documents.len() {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "document batch upsert for '{collection}' carries {} documents but {} \
                     surrogates; every row must carry its own surrogate",
                    documents.len(),
                    surrogates.len(),
                ),
            });
        }
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let mut keys: Vec<ResolvedKey<'_>> = Vec::new();
        let mut slot_of: HashMap<Surrogate, usize> = HashMap::new();
        // The key slot of every row, in `documents` order.
        let mut row_slots: Vec<usize> = Vec::with_capacity(documents.len());
        for ((document_id, value), &surrogate) in documents.iter().zip(surrogates) {
            if let Some(&slot) = slot_of.get(&surrogate) {
                let Some(key) = keys.get_mut(slot) else {
                    continue;
                };
                let existing = nodedb_types::value_from_msgpack(&key.body).map_err(|_| {
                    ErrorCode::Internal {
                        detail: "failed to decode document for upsert".into(),
                    }
                })?;
                let merged =
                    merge_upsert_body(existing, value, on_conflict_updates, &self.crs_context())?;
                key.body = merged.body;
                if let Some(assigned) = &mut key.assigned {
                    assigned.extend(merged.assigned);
                }
                row_slots.push(slot);
                continue;
            }
            let existing = self.doc_resolve_read(&ctx, collection, &row_key_of(surrogate))?;
            let (body, assigned) = match &existing {
                Some(current_bytes) => {
                    let existing_value =
                        crate::data::executor::handlers::upsert::decode_stored_row(
                            current_bytes,
                            ctx.strict_schema.as_ref(),
                        )?;
                    let merged = merge_upsert_body(
                        existing_value,
                        value,
                        on_conflict_updates,
                        &self.crs_context(),
                    )?;
                    (merged.body, Some(merged.assigned))
                }
                None => (value.clone(), None),
            };
            slot_of.insert(surrogate, keys.len());
            row_slots.push(keys.len());
            keys.push(ResolvedKey {
                document_id: document_id.as_str(),
                surrogate,
                precondition: existing,
                body,
                assigned,
            });
        }

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let sys_from = self.bitemporal_ordinal();
        let mut stored_images: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        for key in &keys {
            let row_identity =
                crate::engine::document::store::StorageKey::for_surrogate(key.surrogate)
                    .to_identity();
            rls_write_gate::admit_stored_row(
                rls_write_check,
                rls_write_gate::GatedRow {
                    body: &key.body,
                    identity: &row_identity,
                    strict_schema: None,
                    identity_column: &ctx.identity_column,
                },
                tid,
                collection,
                &self.crs_context(),
            )
            .map_err(ErrorCode::from)?;
            let guards = match &key.assigned {
                Some(assigned) => {
                    crate::data::executor::enforcement::typeguard::GuardScope::Assigned(assigned)
                }
                None => crate::data::executor::enforcement::typeguard::GuardScope::Document,
            };
            // `RETURNING` projects the stored image the apply builds.
            let stored = self
                .build_stored_body(StoredBodyInput {
                    config_key: &config_key,
                    surrogate: key.surrogate,
                    value: &key.body,
                    bitemporal: ctx.bitemporal,
                    sys_from,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    guards,
                })
                .map_err(ErrorCode::from)?
                .stored;
            stored_images.push(stored);
        }

        // One reply row per page row, each its key's stored image.
        let identities: Vec<RowIdentity> = keys
            .iter()
            .map(|key| RowIdentity::from_user_key(key.document_id))
            .collect();
        let rows: Vec<(&RowIdentity, &[u8])> = row_slots
            .iter()
            .filter_map(|&slot| Some((identities.get(slot)?, stored_images.get(slot)?.as_slice())))
            .collect();
        let response_payload =
            resolved_response_payload(returning, rls_filters, &ctx, &rows, &self.crs_context())?;

        Ok(DocumentResolveOutcome {
            mutations: keys
                .into_iter()
                .map(|key| {
                    put_mutation(ResolvedPut {
                        collection,
                        document_id: key.document_id,
                        surrogate: key.surrogate,
                        value: key.body,
                        precondition: key.precondition,
                        resolved_sum_targets,
                    })
                })
                .collect(),
            response_payload,
        })
    }
}

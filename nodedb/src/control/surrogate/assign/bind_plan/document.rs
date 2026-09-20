// SPDX-License-Identifier: BUSL-1.1

//! Document ops: every row is keyed by `document_id.as_bytes()`, the same key
//! the planner assigned under.

use nodedb_physical::physical_plan::{DocumentOp, DocumentResolvedMutation};

use super::binder::IdentityBinder;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut DocumentOp) -> crate::Result<()> {
    match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            surrogate,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            document_id,
            surrogate,
            ..
        }
        | DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            ..
        }
        | DocumentOp::Upsert {
            collection,
            document_id,
            surrogate,
            ..
        } => binder.resolve_in_place(collection.as_str(), document_id.as_bytes(), surrogate),
        DocumentOp::BatchInsert {
            collection,
            documents,
            surrogates,
            ..
        } => {
            // `zip` would truncate silently; a row with no identity is a
            // malformed plan, refused here.
            if documents.len() != surrogates.len() {
                return Err(crate::Error::Serialization {
                    format: "physical_plan".into(),
                    detail: format!(
                        "batch insert into '{}' carries {} documents but {} surrogates; every \
                         row must carry its own surrogate",
                        collection.as_str(),
                        documents.len(),
                        surrogates.len(),
                    ),
                });
            }
            for ((document_id, _value), surrogate) in documents.iter().zip(surrogates.iter_mut()) {
                binder.resolve_in_place(collection.as_str(), document_id.as_bytes(), surrogate)?;
            }
            Ok(())
        }
        // The NOT-MATCHED rows a resolved MERGE inserts, keyed by the document
        // id the orchestrator derived when it assigned each surrogate. The
        // join-keyed slot the handler applies is index-aligned with the
        // identities, so an earlier binding replaces the carried value in both.
        DocumentOp::Merge {
            target_collection,
            resolved_inserts,
            resolved_insert_identities,
            ..
        } => {
            for (index, (document_id, surrogate)) in
                resolved_insert_identities.iter_mut().enumerate()
            {
                let carried = nodedb_types::Surrogate::new(*surrogate);
                let bound =
                    binder.resolve(target_collection.as_str(), document_id.as_bytes(), carried)?;
                if bound == carried {
                    continue;
                }
                *surrogate = bound.as_u32();
                if let Some((_, applied)) = resolved_inserts
                    .as_mut()
                    .and_then(|inserts| inserts.get_mut(index))
                    && *applied == carried.as_u32()
                {
                    *applied = bound.as_u32();
                }
            }
            Ok(())
        }
        DocumentOp::ResolveWrite(inner) => bind(binder, inner),
        DocumentOp::ResolvedWrite { mutations, .. } => {
            for mutation in mutations.iter_mut() {
                match mutation {
                    DocumentResolvedMutation::Put {
                        collection,
                        document_id,
                        surrogate,
                        ..
                    }
                    | DocumentResolvedMutation::Delete {
                        collection,
                        document_id,
                        surrogate,
                        ..
                    } => binder.resolve_in_place(
                        collection.as_str(),
                        document_id.as_bytes(),
                        surrogate,
                    )?,
                }
            }
            Ok(())
        }
        // A predicate or scan-driven write names its rows by surrogate alone
        // once applied; nothing here carries a key to bind.
        DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. } => Ok(()),
    }
}

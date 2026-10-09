// SPDX-License-Identifier: BUSL-1.1

//! Document-engine write keys.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use super::plan::{not_a_write, unsequenced};
use super::set::WriteKeys;

/// Add the write keys of document op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &DocumentOp) -> crate::Result<()> {
    match op {
        // A point writer locks its row's surrogate and its row id, so it
        // orders against a writer that knows either.
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
        | DocumentOp::Upsert {
            collection,
            document_id,
            surrogate,
            ..
        } => {
            keys.rows(collection.as_str(), [surrogate.as_u32()]);
            keys.row_id(collection.as_str(), document_id);
        }
        // The TARGET row's identity: two balance writes onto one row
        // serialize, and balance writes onto distinct rows do not. Its
        // document id is the surrogate's hex, not a key the row binds under.
        DocumentOp::ApplyBalanceDelta {
            collection,
            surrogate,
            ..
        } => keys.rows(collection.as_str(), [surrogate.as_u32()]),
        // A key unbound in its database names no row yet: it locks only its
        // row id. The row id orders it after an insert that binds the key, so
        // its rebind at dispatch finds that binding on every replica.
        DocumentOp::PointDelete {
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
        } => {
            if let Some(surrogate) = surrogate {
                keys.rows(collection.as_str(), [surrogate.as_u32()]);
            }
            keys.row_id(collection.as_str(), document_id);
        }
        // Every row it inserts, by surrogate and row id.
        DocumentOp::BatchInsert {
            collection,
            documents,
            surrogates,
            ..
        } => {
            keys.rows(collection.as_str(), surrogates.iter().map(|s| s.as_u32()));
            for (document_id, _) in documents {
                keys.row_id(collection.as_str(), document_id);
            }
        }
        DocumentOp::InsertSelect {
            target_collection, ..
        } => keys.whole_collection(target_collection.as_str()),
        DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::Truncate { collection, .. } => keys.whole_collection(collection.as_str()),
        // Control-Plane orchestrators resolve these into concrete point
        // writes before dispatch; the routing oracle names them unroutable.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => {
            return Err(unsequenced(
                "a cross-collection document write has no enforced source/target co-location",
            ));
        }
        DocumentOp::ResolvedWrite { .. } => {
            return Err(unsequenced(
                "a resolved governed document write is proposed by the write-resolve \
                 orchestrator",
            ));
        }
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => {
            return Err(not_a_write("a document read or index DDL op"));
        }
    }
    Ok(())
}

// SPDX-License-Identifier: BUSL-1.1

//! Plan classification for transaction resolve: which KV and document
//! collections a transaction wrote. Those two engines serialize from the
//! overlay, so the plan walk only collects collections and rejects the ops
//! that leave no staged post-image.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{DocumentOp, KvOp};

/// Classify a KV op for transaction resolve: collect the collection of a
/// row-level write into `collections`, skip read-only ops, and reject the ops
/// that have no row-level redo representation.
pub(super) fn classify_kv_op(op: &KvOp, collections: &mut BTreeSet<String>) -> crate::Result<()> {
    match op {
        // Row-level writes: the resolved post-image (value or tombstone) is in
        // the overlay, keyed by collection.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Transfer { collection, .. }
        // Predicate DML stages each matched row's post-image or tombstone
        // at statement time, so the overlay carries it like a keyed write.
        | KvOp::PredicateUpdate { collection, .. }
        | KvOp::PredicateDelete { collection, .. } => {
            collections.insert(collection.to_string());
            Ok(())
        }
        // `TransferItem` moves a row across collections: the source holds a
        // staged tombstone and the destination a staged value.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            collections.insert(source_collection.to_string());
            collections.insert(dest_collection.to_string());
            Ok(())
        }

        // Read-only: nothing staged, nothing to persist.
        KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        // Read-only: reports what a governed write would apply, stages
        // nothing.
        | KvOp::ResolveWrite(_) => Ok(()),

        // Resolve-before-propose is an autocommit path: never staged into an
        // overlay, so no row-level redo shape carries it.
        KvOp::ResolvedWrite { .. } => Err(crate::Error::PlanError {
            detail: "kv resolved write is not supported in transaction resolve".to_string(),
        }),

        // A standalone TTL delta has no value post-image, and KV redo carries
        // TTL only as part of a value put, so rejecting avoids a silent drop.
        KvOp::Expire { .. } | KvOp::Persist { .. } => Err(crate::Error::PlanError {
            detail: "kv EXPIRE/PERSIST is not supported in transaction resolve".to_string(),
        }),

        // Truncate: staged as an overlay marker; the serializer emits the
        // `kv_truncate` redo ahead of the collection's row entries.
        KvOp::Truncate { collection, .. } => {
            collections.insert(collection.to_string());
            Ok(())
        }

        // Index / DDL: never stageable into the overlay, so no row-level
        // redo shape carries them.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Err(crate::Error::PlanError {
            detail: "kv index/DDL op is not supported in transaction resolve".to_string(),
        }),
    }
}

/// Classify a Document op for transaction resolve: collect the collection of a
/// staged point/bulk write into `collections`, skip read-only ops, and reject
/// the writes that leave no overlay post-image.
pub(super) fn classify_document_op(
    op: &DocumentOp,
    collections: &mut BTreeSet<String>,
) -> crate::Result<()> {
    match op {
        // Staged writes: the resolved post-image is in the overlay, keyed by the
        // user primary key. RETURNING doesn't affect staging, so these serialize
        // from the overlay like any other point/bulk write.
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        // A balance write stages like any other point write: one target row,
        // one absolute post-image, keyed by the row's own surrogate.
        | DocumentOp::ApplyBalanceDelta { collection, .. } => {
            collections.insert(collection.to_string());
            Ok(())
        }
        // `INSERT ... SELECT` stages the copied rows into the target collection.
        DocumentOp::InsertSelect {
            target_collection, ..
        } => {
            collections.insert(target_collection.to_string());
            Ok(())
        }

        // Read-only families: scans, lookups, point-gets, and estimates carry
        // no persisted post-image.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. } => Ok(()),

        // Resolve-before-propose is an autocommit path: never staged into an
        // overlay, so no row-level redo shape carries it.
        DocumentOp::ResolvedWrite { .. } => Err(crate::Error::PlanError {
            detail: "document resolved write is not supported in transaction resolve".to_string(),
        }),

        // Join/merge have no per-surrogate post-image; `BatchInsert` rides the
        // buffered-plan path. None is staged, so rejecting avoids a lossy redo.
        DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::BatchInsert { .. } => Err(crate::Error::PlanError {
            detail: "document join/merge/batch DML has no staged post-image and is not \
                     supported in transaction resolve"
                .to_string(),
        }),

        // Truncate: staged as an overlay marker; the serializer emits a
        // `Delete` per removed base row ahead of the collection's overlay
        // entries.
        DocumentOp::Truncate { collection, .. } => {
            collections.insert(collection.to_string());
            Ok(())
        }

        // Index / DDL: never stageable into the overlay, so no row-level
        // redo shape carries them.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => Err(crate::Error::PlanError {
            detail: "document index/DDL op is not supported in transaction resolve".to_string(),
        }),
    }
}

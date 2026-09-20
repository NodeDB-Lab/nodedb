// SPDX-License-Identifier: BUSL-1.1

//! Decode the vector-primary direct-write wire variants back into
//! `PhysicalPlan::Vector`.
//!
//! Every surrogate is rebuilt verbatim from the record; `entry.rs` binds the
//! insert family to its primary key afterwards. `DELETE` / `UPDATE` carry
//! their targets verbatim: a point-targeted write names the leader's
//! surrogates, which every replica bound when the rows were inserted; a
//! predicate-targeted write re-resolves the predicate against the replica's
//! own sidecar rows, exactly as `KvPredicateDelete` does.

use nodedb_physical::physical_plan::{
    ReturningSpec, UpdateValue, VectorDirectWriteIntent, VectorOp, VectorResolvedMutation,
    VectorWriteTargets,
};
use nodedb_types::Surrogate;

use crate::bridge::envelope::PhysicalPlan;

/// `ReplicatedWrite::VectorDirectDelete` → `VectorOp::DirectDelete`.
pub(super) fn direct_delete(
    collection: &str,
    field: &str,
    targets: &VectorWriteTargets,
    returning: Option<ReturningSpec>,
    rls_filters: &[u8],
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DirectDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
        targets: targets.clone(),
        returning,
        rls_filters: rls_filters.to_vec(),
        // A write-policy collection never reaches this shape: the leader
        // refuses to propose a predicate it cannot decide for a follower.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
    })
}

/// `ReplicatedWrite::VectorDirectTruncate` → `VectorOp::DirectTruncate`.
pub(super) fn direct_truncate(
    collection: &str,
    field: &str,
    restart_identity: bool,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DirectTruncate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        field: field.to_owned(),
        restart_identity,
    })
}

/// Fields of the `VectorDirectUpdate` wire variant, bundled so
/// [`direct_update`] stays under the `too_many_arguments` clippy threshold.
pub(super) struct DirectUpdateFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field: &'a str,
    pub(super) targets: &'a VectorWriteTargets,
    pub(super) new_vector: &'a Option<Vec<f32>>,
    pub(super) payload_patch: &'a [(String, UpdateValue)],
    pub(super) quantization: nodedb_types::VectorQuantization,
    pub(super) storage_dtype: nodedb_types::VectorStorageDtype,
    pub(super) payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub(super) returning: Option<ReturningSpec>,
    pub(super) rls_filters: &'a [u8],
}

/// `ReplicatedWrite::VectorDirectUpdate` → `VectorOp::DirectUpdate`.
pub(super) fn direct_update(f: DirectUpdateFields<'_>) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DirectUpdate {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        field: f.field.to_owned(),
        targets: f.targets.clone(),
        new_vector: f.new_vector.clone(),
        payload_patch: f.payload_patch.to_vec(),
        quantization: f.quantization,
        storage_dtype: f.storage_dtype,
        payload_indexes: f.payload_indexes.to_vec(),
        returning: f.returning,
        rls_filters: f.rls_filters.to_vec(),
        // See `direct_delete`.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
    })
}

/// Fields of the `DirectUpsert` wire variant, bundled so [`direct_upsert`]
/// stays under the `too_many_arguments` clippy threshold.
pub(super) struct DirectUpsertFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field: &'a str,
    pub(super) surrogate: u32,
    pub(super) pk_bytes: &'a [u8],
    pub(super) vector: &'a [f32],
    pub(super) payload: &'a [u8],
    pub(super) quantization: nodedb_types::VectorQuantization,
    pub(super) storage_dtype: nodedb_types::VectorStorageDtype,
    pub(super) payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub(super) intent: VectorDirectWriteIntent,
    pub(super) on_conflict_updates: &'a [(String, UpdateValue)],
    pub(super) returning: Option<ReturningSpec>,
    pub(super) rls_filters: &'a [u8],
}

pub(super) fn direct_upsert(f: DirectUpsertFields) -> crate::Result<PhysicalPlan> {
    let surrogate = Surrogate::new(f.surrogate);
    let collection = nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned());
    // Carried on the record — a replay re-executes this write for the
    // originating request, not just for the follower's own state.
    let op = match f.intent {
        VectorDirectWriteIntent::Insert => VectorOp::DirectInsert {
            collection,
            field: f.field.to_owned(),
            surrogate,
            pk_bytes: f.pk_bytes.to_vec(),
            vector: f.vector.to_vec(),
            payload: f.payload.to_vec(),
            quantization: f.quantization,
            storage_dtype: f.storage_dtype,
            payload_indexes: f.payload_indexes.to_vec(),
            returning: f.returning,
            rls_filters: f.rls_filters.to_vec(),
        },
        VectorDirectWriteIntent::InsertIfAbsent => VectorOp::DirectInsertIfAbsent {
            collection,
            field: f.field.to_owned(),
            surrogate,
            pk_bytes: f.pk_bytes.to_vec(),
            vector: f.vector.to_vec(),
            payload: f.payload.to_vec(),
            quantization: f.quantization,
            storage_dtype: f.storage_dtype,
            payload_indexes: f.payload_indexes.to_vec(),
            returning: f.returning,
            rls_filters: f.rls_filters.to_vec(),
        },
        VectorDirectWriteIntent::Upsert => VectorOp::DirectUpsert {
            collection,
            field: f.field.to_owned(),
            surrogate,
            pk_bytes: f.pk_bytes.to_vec(),
            vector: f.vector.to_vec(),
            payload: f.payload.to_vec(),
            quantization: f.quantization,
            storage_dtype: f.storage_dtype,
            payload_indexes: f.payload_indexes.to_vec(),
            returning: f.returning,
            rls_filters: f.rls_filters.to_vec(),
            on_conflict_updates: f.on_conflict_updates.to_vec(),
            // The leader decided the policy with a live identity before it
            // proposed; a replica applies what was committed.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        },
    };
    Ok(PhysicalPlan::Vector(op))
}

/// Fields of the `VectorResolvedDirectWrite` wire variant, bundled so
/// [`resolved_direct_write`] stays under the `too_many_arguments` clippy
/// threshold.
pub(super) struct ResolvedDirectWriteFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field: &'a str,
    pub(super) quantization: nodedb_types::VectorQuantization,
    pub(super) storage_dtype: nodedb_types::VectorStorageDtype,
    pub(super) payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub(super) mutations: &'a [VectorResolvedMutation],
    pub(super) response_payload: &'a [u8],
}

/// `ReplicatedWrite::VectorResolvedDirectWrite` → `VectorOp::ResolvedDirectWrite`.
///
/// Every mutation travels verbatim: a `Delete` / `Update` names a surrogate
/// bound when the row was inserted, an `Upsert` carries the leader-assigned
/// surrogate `entry.rs` binds to the row's primary key.
pub(super) fn resolved_direct_write(f: ResolvedDirectWriteFields<'_>) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
        collection: nodedb_types::QualifiedCollection::from_stored(f.collection.to_owned()),
        field: f.field.to_owned(),
        quantization: f.quantization,
        storage_dtype: f.storage_dtype,
        payload_indexes: f.payload_indexes.to_vec(),
        mutations: f.mutations.to_vec(),
        response_payload: f.response_payload.to_vec(),
        // The leader decided the policy with a live identity before it
        // proposed; a replica applies what was committed.
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
    })
}

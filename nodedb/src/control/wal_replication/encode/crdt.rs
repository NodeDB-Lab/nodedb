// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Crdt` variants into `ReplicatedWrite`.

use super::super::types::{ConstraintChangeOp, ReplicatedWrite};
use nodedb_physical::physical_plan::CrdtOp;

/// Encode a `CrdtOp` write variant into its `ReplicatedWrite` wire shape.
///
/// Exhaustive over `CrdtOp` (not a catch-all): a new variant forces an
/// explicit decision here instead of silently falling through, mirroring
/// `vector::encode`'s exhaustiveness guarantee.
///
/// `SetConstraints` / `DropConstraints` encode to `ReplicatedWrite::
/// ConstraintChange` so a constraint change installs on every follower's CRDT
/// validator immediately on the write, closing the window in which the leader
/// enforces a constraint its followers do not. The `constraint_reconcile`
/// bootstrap path stays the catch-up safety net for a lagging/new replica;
/// both are fenced idempotent by the monotonic `constraint_version`.
///
/// Returns `None` for the read-only / DDL-observability variants (`Read`,
/// `ReadConstraints`, `SetPolicy`, `GetPolicy`, `ReadAtVersion`,
/// `GetVersionVector`, `ExportDelta`), for `CompactAtVersion`, and for
/// `RestoreToVersion`. `CompactAtVersion` discards durable oplog entries, so it
/// is a write; it replicates as a `CompactHistory` catalog entry that every
/// node dispatches from post-apply, never through this encoder.
/// `RestoreToVersion` is deliberately not encoded here: the
/// restore path replicates its effect as a forward delta wrapped in
/// `CrdtOp::Apply`, which then follows the normal apply replication route.
/// Encoding the restore op directly would double-apply the change and is
/// non-deterministic across replicas.
pub(super) fn encode(op: &CrdtOp) -> Option<ReplicatedWrite> {
    Some(match op {
        CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            // Unused downstream: the sync DLQ path carries its own
            // `mutation_id` from `DeltaPushMsg`, never this one.
            mutation_id: _,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
        } => apply(ApplyFields {
            collection: collection.as_str(),
            document_id,
            delta,
            peer_id: *peer_id,
            provenance: super::entry::encode_provenance(provenance),
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: *expected_frontier_digest,
            surrogate: surrogate.as_u32(),
        }),
        CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            // See `Apply` above.
            mutation_id: _,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        } => ReplicatedWrite::CrdtApplyAuthenticated {
            collection: collection.to_string(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            provenance: super::entry::encode_provenance(&Some(provenance.clone())),
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: *expected_frontier_digest,
            auth_user_id: *auth_user_id,
            auth_device_id: *auth_device_id,
            auth_seq_no: *auth_seq_no,
            delta_signature: *delta_signature,
            signing_required: *signing_required,
            surrogate: surrogate.as_u32(),
        },
        CrdtOp::ImportSnapshot {
            tenant_id,
            collection,
            bytes,
        } => import_snapshot(*tenant_id, collection.as_str(), bytes),
        CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            surrogate,
        } => list_insert(
            collection.as_str(),
            document_id,
            list_path,
            *index,
            fields_json,
            surrogate.as_u32(),
        ),
        CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            surrogate,
        } => list_delete(
            collection.as_str(),
            document_id,
            list_path,
            *index,
            surrogate.as_u32(),
        ),
        CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate,
        } => list_move(
            collection.as_str(),
            document_id,
            list_path,
            *from_index,
            *to_index,
            surrogate.as_u32(),
        ),
        CrdtOp::DocUpsert {
            collection,
            document_id,
            fields_json,
            surrogate,
            partial,
            verb,
            returning,
            rls_filters,
        } => ReplicatedWrite::CrdtDocUpsert {
            collection: collection.as_str().to_owned(),
            document_id: document_id.clone(),
            surrogate: surrogate.as_u32(),
            fields_json: fields_json.clone(),
            partial: *partial,
            verb: *verb,
            returning: super::entry::encode_returning(returning),
            rls_filters: rls_filters.clone(),
        },
        CrdtOp::DocDelete {
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
        } => doc_delete(
            collection.as_str(),
            document_id,
            surrogate.as_u32(),
            super::entry::encode_returning(returning),
            rls_filters,
        ),
        CrdtOp::SetConstraints {
            collection,
            constraint_version,
            constraints,
        } => set_constraints(collection.as_str(), *constraint_version, constraints),
        CrdtOp::DropConstraints {
            collection,
            constraint_version,
        } => drop_constraints(collection.as_str(), *constraint_version),
        // `CompactAtVersion` discards durable oplog entries. It replicates as
        // a `CompactHistory` catalog entry that every node dispatches from
        // post-apply, so it never rides this encoder.
        CrdtOp::CompactAtVersion { .. } => return None,
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::RestoreToVersion { .. } => return None,
    })
}

/// Encode `SetConstraints` as a `ConstraintChange` install. The full constraint
/// blob set is carried verbatim; the apply path fences on `constraint_version`.
pub(super) fn set_constraints(
    collection: &str,
    constraint_version: u64,
    constraints: &[Vec<u8>],
) -> ReplicatedWrite {
    ReplicatedWrite::ConstraintChange {
        collection: collection.to_owned(),
        op: ConstraintChangeOp::Set,
        constraint_version,
        constraints: constraints.to_vec(),
    }
}

/// Encode `DropConstraints` as a `ConstraintChange` removal — no blobs, fenced
/// by `constraint_version` exactly as the install is.
pub(super) fn drop_constraints(collection: &str, constraint_version: u64) -> ReplicatedWrite {
    ReplicatedWrite::ConstraintChange {
        collection: collection.to_owned(),
        op: ConstraintChangeOp::Drop,
        constraint_version,
        constraints: Vec::new(),
    }
}

/// One CRDT delta apply, as it goes onto the wire.
pub(super) struct ApplyFields<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub delta: &'a [u8],
    pub peer_id: u64,
    pub provenance: Option<Vec<u8>>,
    pub constraint_version_required: u64,
    pub expected_frontier_digest: Option<[u8; 32]>,
    pub surrogate: u32,
}

pub(super) fn apply(fields: ApplyFields<'_>) -> ReplicatedWrite {
    let ApplyFields {
        collection,
        document_id,
        delta,
        peer_id,
        provenance,
        constraint_version_required,
        expected_frontier_digest,
        surrogate,
    } = fields;
    match expected_frontier_digest {
        Some(expected_frontier_digest) => ReplicatedWrite::CrdtApplyFenced {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            surrogate,
        },
        None => ReplicatedWrite::CrdtApply {
            collection: collection.to_owned(),
            document_id: document_id.to_owned(),
            delta: delta.to_vec(),
            peer_id,
            provenance,
            constraint_version_required,
            surrogate,
        },
    }
}

pub(super) fn import_snapshot(tenant_id: u64, collection: &str, bytes: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::CrdtImportCollection {
        tenant_id,
        collection: collection.to_owned(),
        bytes: bytes.to_vec(),
    }
}

/// `index` is the Data Plane's `usize` list position, widened losslessly to
/// the wire's `u64` (every supported target's `usize` fits in `u64`).
pub(super) fn list_insert(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields_json: &str,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: index as u64,
        fields_json: fields_json.to_owned(),
        surrogate,
    }
}

/// See [`list_insert`] for the `index` widening note.
pub(super) fn list_delete(
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        index: index as u64,
        surrogate,
    }
}

/// See [`list_insert`] for the `index` widening note (applies to both
/// `from_index` and `to_index` here).
pub(super) fn list_move(
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtListMove {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        list_path: list_path.to_owned(),
        from_index: from_index as u64,
        to_index: to_index as u64,
        surrogate,
    }
}

pub(super) fn doc_delete(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::CrdtDocDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        returning,
        rls_filters: rls_filters.to_vec(),
    }
}

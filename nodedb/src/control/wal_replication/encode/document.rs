// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Document` variants into `ReplicatedWrite`.
//!
//! A document write that maintains a derived total carries the join-key →
//! target-surrogate resolution, copied onto the record so no applier re-derives it.

use super::super::types::{ReplicatedSumTarget, ReplicatedWrite};
use nodedb_physical::physical_plan::{DocumentResolvedMutation, ResolvedSumTarget, UpdateValue};
use nodedb_types::Surrogate;

/// `resolved_sum_targets` + `deferred_sum_targets` bundled for `point_insert`
/// — plain positional arguments there exceed clippy's arity lint once
/// `returning` joins the signature.
pub(super) struct SumFields<'a> {
    pub resolved: &'a [ResolvedSumTarget],
    pub deferred: &'a [String],
}

/// The RETURNING wire pair, already msgpack-encoded by `encode_returning` /
/// left raw for `rls_filters` — see `ReplicatedWrite::PointPut::returning`.
/// Bundled for the same arity reason as `SumFields`.
pub(super) struct WireReturning<'a> {
    pub returning: Option<Vec<u8>>,
    pub rls_filters: &'a [u8],
}

/// Flatten a plan's resolution into the authoritative wire shape (`Surrogate`
/// travels as bare `u32`, like every other identity on this wire). An entry
/// with no target collection is dropped rather than guessed at.
fn wire_target_bindings(resolved: &[ResolvedSumTarget]) -> Vec<ReplicatedSumTarget> {
    resolved
        .iter()
        .filter_map(|entry| {
            entry
                .target_collection
                .as_ref()
                .map(|target_collection| ReplicatedSumTarget {
                    target_collection: target_collection.clone(),
                    join_value: entry.join_value.clone(),
                    surrogate: entry.surrogate.as_u32(),
                })
        })
        .collect()
}

/// The superseded `(join_value, surrogate)` shape, kept populated so an older
/// peer binary still reads it correctly. Derived from the authoritative slot,
/// never carried separately, so the two can't disagree. First binding wins per join value.
fn wire_targets(resolved: &[ResolvedSumTarget]) -> Vec<(String, u32)> {
    let mut legacy: Vec<(String, u32)> = Vec::with_capacity(resolved.len());
    for entry in resolved {
        if legacy.iter().any(|(value, _)| *value == entry.join_value) {
            continue;
        }
        legacy.push((entry.join_value.clone(), entry.surrogate.as_u32()));
    }
    legacy
}

pub(super) fn point_put(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::PointPut {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning,
        rls_filters: rls_filters.to_vec(),
    }
}

pub(super) fn point_insert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    if_absent: bool,
    surrogate: u32,
    sums: SumFields<'_>,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::PointInsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        if_absent,
        surrogate,
        resolved_sum_targets: wire_targets(sums.resolved),
        resolved_sum_target_bindings: wire_target_bindings(sums.resolved),
        deferred_sum_targets: sums.deferred.to_vec(),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn point_delete(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::PointDelete {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning,
        rls_filters: rls_filters.to_vec(),
    }
}

pub(super) fn point_update(
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: WireReturning<'_>,
    declared_primary_key: Option<&str>,
) -> ReplicatedWrite {
    ReplicatedWrite::PointUpdate {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        updates: updates.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
        declared_primary_key: declared_primary_key.map(str::to_owned),
    }
}

pub(super) fn upsert(
    collection: &str,
    document_id: &str,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    surrogate: u32,
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::DocUpsert {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        value: value.to_vec(),
        on_conflict_updates: on_conflict_updates.to_vec(),
        surrogate,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning: returning.returning,
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn batch_insert(
    collection: &str,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
    resolved_sum_targets: &[ResolvedSumTarget],
    deferred_sum_targets: &[String],
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::DocBatchInsert {
        collection: collection.to_owned(),
        documents: documents.to_vec(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        deferred_sum_targets: deferred_sum_targets.to_vec(),
        returning,
        rls_filters: rls_filters.to_vec(),
    }
}

/// Replicates as a plain `DocTruncate` entry: clearing is idempotent, so every
/// replica safely re-executes it. The balance cleared rows fed isn't re-derivable,
/// so its resolution rides along.
pub(super) fn truncate(
    collection: &str,
    restart_identity: bool,
    resolved_sum_targets: &[ResolvedSumTarget],
) -> ReplicatedWrite {
    ReplicatedWrite::DocTruncate {
        collection: collection.to_owned(),
        restart_identity,
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
    }
}

/// Single-shard bulk predicate writes replicate as a plain `BulkDml` entry: each
/// replica re-scans local state at the committed log position and applies the
/// predicate deterministically. An OLLP-prepared plan is Calvin's, not encoded here.
pub(super) fn bulk_delete(
    collection: &str,
    filters: &[u8],
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
    declared_primary_key: Option<&str>,
) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: false,
        updates: Vec::new(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning,
        rls_filters: rls_filters.to_vec(),
        declared_primary_key: declared_primary_key.map(str::to_owned),
    }
}

pub(super) fn bulk_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, UpdateValue)],
    resolved_sum_targets: &[ResolvedSumTarget],
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
    declared_primary_key: Option<&str>,
) -> ReplicatedWrite {
    ReplicatedWrite::BulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: true,
        updates: updates.to_vec(),
        resolved_sum_targets: wire_targets(resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(resolved_sum_targets),
        returning,
        rls_filters: rls_filters.to_vec(),
        declared_primary_key: declared_primary_key.map(str::to_owned),
    }
}

/// Replicates as a plain `InsertSelect` entry: each replica re-scans the source
/// at the committed log position and copies matches, reusing each row's surrogate/doc_id.
pub(super) fn insert_select(
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
    column_map: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::InsertSelect {
        target_collection: target_collection.to_owned(),
        source_collection: source_collection.to_owned(),
        source_filters: source_filters.to_vec(),
        source_limit,
        column_map: column_map.to_vec(),
    }
}

/// Encode a resolved document write: every row mutation the Control Plane
/// decided, plus the reply. Nothing is re-derived — bodies are pre-encode
/// MessagePack and preconditions are raw stored bytes, so every replica agrees.
pub(super) fn resolved_write(
    mutations: &[DocumentResolvedMutation],
    response_payload: &[u8],
) -> ReplicatedWrite {
    use super::super::types::DocumentResolvedMutationWire as W;

    ReplicatedWrite::DocumentResolvedWrite {
        mutations: mutations
            .iter()
            .map(|m| match m {
                DocumentResolvedMutation::Put {
                    collection,
                    document_id,
                    surrogate,
                    // Decode re-derives this from `document_id.as_bytes()`.
                    pk_bytes: _,
                    value,
                    precondition,
                    resolved_sum_targets,
                } => W::Put {
                    collection: collection.as_str().to_owned(),
                    document_id: document_id.clone(),
                    surrogate: surrogate.as_u32(),
                    value: value.clone(),
                    precondition: precondition.clone(),
                    resolved_sum_targets: wire_target_bindings(resolved_sum_targets),
                },
                DocumentResolvedMutation::Delete {
                    collection,
                    document_id,
                    surrogate,
                    // Decode re-derives this from `document_id.as_bytes()`.
                    pk_bytes: _,
                    precondition,
                    resolved_sum_targets,
                } => W::Delete {
                    collection: collection.as_str().to_owned(),
                    document_id: document_id.clone(),
                    surrogate: surrogate.as_u32(),
                    precondition: precondition.clone(),
                    resolved_sum_targets: wire_target_bindings(resolved_sum_targets),
                },
            })
            .collect(),
        response_payload: response_payload.to_vec(),
    }
}

/// Replicates as the delta it is, modelled on `KvIncr`: each replica applies it
/// once in log order onto its own prior balance. The decimal travels as a
/// string because `f64` is lossy past 15 significant digits.
pub(super) fn apply_balance_delta(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    column: &str,
    delta: &str,
    join_column: &str,
    join_value: &str,
) -> ReplicatedWrite {
    ReplicatedWrite::ApplyBalanceDelta {
        collection: collection.to_owned(),
        document_id: document_id.to_owned(),
        surrogate,
        column: column.to_owned(),
        delta: delta.to_owned(),
        join_column: join_column.to_owned(),
        join_value: join_value.to_owned(),
    }
}

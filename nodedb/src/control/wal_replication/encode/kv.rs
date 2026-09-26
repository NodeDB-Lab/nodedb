// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Kv` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use super::entry::{encode_provenance, encode_returning};
use nodedb_physical::physical_plan::{KvCounterShape, ReturningSpec, UpdateValue};
use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;

/// Resolve the wall-clock instant for a TTL-bearing write once, at proposal
/// time. `None` when `ttl_ms == 0`. Every replica computes `expire_at_ms`
/// from this same instant instead of an independent wall-clock read.
fn resolve_now_ms(ttl_ms: u64) -> Option<u64> {
    if ttl_ms == 0 {
        None
    } else {
        Some(crate::engine::kv::current_ms())
    }
}

/// The RETURNING wire pair every `Put`/`Insert`-family KV op carries — see
/// `ReplicatedWrite::KvPut::returning`. Bundled — plain positional arguments
/// exceed clippy's arity lint once every KV write function carries it.
pub(super) struct WireReturning<'a> {
    pub returning: &'a Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

/// The row a KV put writes, and the identity it is written under.
pub(super) struct WirePut<'a> {
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub surrogate: u32,
}

pub(super) fn put(
    put: WirePut<'_>,
    returning: WireReturning<'_>,
    provenance: &Option<SyncProvenance>,
) -> ReplicatedWrite {
    let WirePut {
        collection,
        key,
        value,
        ttl_ms,
        surrogate,
    } = put;
    ReplicatedWrite::KvPut {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
        provenance: encode_provenance(provenance),
    }
}

/// Encode a KV predicate `UPDATE`: the predicate itself, never a row set. Only
/// reached for a collection with no write policy.
pub(super) fn predicate_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvPredicateUpdate {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        updates: updates.to_vec(),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

/// Encode a KV predicate `DELETE` — see [`predicate_update`].
pub(super) fn predicate_delete(
    collection: &str,
    filters: &[u8],
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvPredicateDelete {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn delete(
    collection: &str,
    keys: &[Vec<u8>],
    returning: WireReturning<'_>,
    provenance: &Option<SyncProvenance>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvDelete {
        collection: collection.to_owned(),
        keys: keys.to_vec(),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
        provenance: encode_provenance(provenance),
    }
}

pub(super) fn insert(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsert {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn insert_if_absent(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    surrogate: u32,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsertIfAbsent {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn insert_on_conflict_update(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
    surrogate: u32,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvInsertOnConflictUpdate {
        collection: collection.to_owned(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms,
        updates: updates.to_vec(),
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn batch_put(
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    surrogates: &[Surrogate],
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvBatchPut {
        collection: collection.to_owned(),
        entries: entries.to_vec(),
        ttl_ms,
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        resolved_now_ms: resolve_now_ms(ttl_ms),
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn expire(collection: &str, key: &[u8], ttl_ms: u64) -> ReplicatedWrite {
    // `ttl_ms == 0` is a legitimate "expire now" request, so the instant always resolves.
    ReplicatedWrite::KvExpire {
        collection: collection.to_owned(),
        key: key.to_vec(),
        ttl_ms,
        resolved_now_ms: Some(crate::engine::kv::current_ms()),
    }
}

pub(super) fn persist(collection: &str, key: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::KvPersist {
        collection: collection.to_owned(),
        key: key.to_vec(),
    }
}

pub(super) fn incr(
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
    shape: &KvCounterShape,
) -> ReplicatedWrite {
    ReplicatedWrite::KvIncr {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta,
        ttl_ms,
        surrogate,
        resolved_now_ms: resolve_now_ms(ttl_ms),
        shape: shape.clone(),
    }
}

pub(super) fn incr_float(
    collection: &str,
    key: &[u8],
    delta: &str,
    surrogate: u32,
    shape: &KvCounterShape,
) -> ReplicatedWrite {
    ReplicatedWrite::KvIncrFloat {
        collection: collection.to_owned(),
        key: key.to_vec(),
        delta: delta.to_owned(),
        surrogate,
        shape: shape.clone(),
    }
}

pub(super) fn cas(
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvCas {
        collection: collection.to_owned(),
        key: key.to_vec(),
        expected: expected.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
    }
}

pub(super) fn get_set(
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::KvGetSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        new_value: new_value.to_vec(),
        surrogate,
        rls_filters: rls_filters.to_vec(),
    }
}

/// Fields of `KvOp::RegisterSortedIndex`, bundled so
/// [`register_sorted_index`] stays under the `too_many_arguments` clippy
/// threshold.
pub(super) struct RegisterSortedIndexFields<'a> {
    pub(super) collection: &'a str,
    pub(super) index_name: &'a str,
    pub(super) sort_columns: &'a [(String, String)],
    pub(super) key_column: &'a str,
    pub(super) window_type: &'a str,
    pub(super) window_timestamp_column: &'a str,
    pub(super) window_start_ms: u64,
    pub(super) window_end_ms: u64,
}

pub(super) fn register_sorted_index(f: RegisterSortedIndexFields) -> ReplicatedWrite {
    ReplicatedWrite::KvRegisterSortedIndex {
        collection: f.collection.to_owned(),
        index_name: f.index_name.to_owned(),
        sort_columns: f.sort_columns.to_vec(),
        key_column: f.key_column.to_owned(),
        window_type: f.window_type.to_owned(),
        window_timestamp_column: f.window_timestamp_column.to_owned(),
        window_start_ms: f.window_start_ms,
        window_end_ms: f.window_end_ms,
    }
}

pub(super) fn drop_sorted_index(index_name: &str) -> ReplicatedWrite {
    ReplicatedWrite::KvDropSortedIndex {
        index_name: index_name.to_owned(),
    }
}

pub(super) fn register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> ReplicatedWrite {
    ReplicatedWrite::KvRegisterIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
        field_position,
        backfill,
    }
}

pub(super) fn drop_index(collection: &str, field: &str) -> ReplicatedWrite {
    ReplicatedWrite::KvDropIndex {
        collection: collection.to_owned(),
        field: field.to_owned(),
    }
}

pub(super) fn field_set(
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
    if_present: bool,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::KvFieldSet {
        collection: collection.to_owned(),
        key: key.to_vec(),
        updates: updates.to_vec(),
        surrogate,
        if_present,
        returning: encode_returning(returning.returning),
        rls_filters: returning.rls_filters.to_vec(),
    }
}

pub(super) fn transfer(
    collection: &str,
    source_key: &[u8],
    dest_key: &[u8],
    field: &str,
    amount: f64,
    debit_surrogate: u32,
    credit_surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvTransfer {
        collection: collection.to_owned(),
        source_key: source_key.to_vec(),
        dest_key: dest_key.to_vec(),
        field: field.to_owned(),
        amount,
        debit_surrogate,
        credit_surrogate,
    }
}

/// `KvOp::Truncate` replicates as a plain `KvTruncate` entry: same
/// autocommit-only, idempotent-replay contract as `document::truncate`.
pub(super) fn truncate(collection: &str, restart_identity: bool) -> ReplicatedWrite {
    ReplicatedWrite::KvTruncate {
        collection: collection.to_owned(),
        restart_identity,
    }
}

/// Encode a resolved KV write: every mutation the Control Plane decided, plus
/// the reply. Nothing re-derived — each mutation carries the resolved absolute
/// instant, so every replica installs identical expiry.
pub(super) fn resolved_write(
    mutations: &[nodedb_physical::physical_plan::KvResolvedMutation],
    response_payload: &[u8],
) -> ReplicatedWrite {
    use super::super::types::KvResolvedMutationWire as W;
    use nodedb_physical::physical_plan::KvResolvedMutation as M;

    ReplicatedWrite::KvResolvedWrite {
        mutations: mutations
            .iter()
            .map(|m| match m {
                M::Put {
                    collection,
                    key,
                    value,
                    ttl_ms,
                    expire_at_ms,
                    surrogate,
                    precondition,
                } => W::Put {
                    collection: collection.as_str().to_owned(),
                    key: key.clone(),
                    value: value.clone(),
                    ttl_ms: *ttl_ms,
                    expire_at_ms: *expire_at_ms,
                    surrogate: surrogate.as_u32(),
                    precondition: precondition.clone(),
                },
                M::Delete {
                    collection,
                    key,
                    precondition,
                } => W::Delete {
                    collection: collection.as_str().to_owned(),
                    key: key.clone(),
                    precondition: precondition.clone(),
                },
                M::Expire {
                    collection,
                    key,
                    ttl_ms,
                    resolved_now_ms,
                    precondition,
                } => W::Expire {
                    collection: collection.as_str().to_owned(),
                    key: key.clone(),
                    ttl_ms: *ttl_ms,
                    resolved_now_ms: *resolved_now_ms,
                    precondition: precondition.clone(),
                },
                M::Persist {
                    collection,
                    key,
                    precondition,
                } => W::Persist {
                    collection: collection.as_str().to_owned(),
                    key: key.clone(),
                    precondition: precondition.clone(),
                },
            })
            .collect(),
        response_payload: response_payload.to_vec(),
    }
}

pub(super) fn transfer_item(
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> ReplicatedWrite {
    ReplicatedWrite::KvTransferItem {
        source_collection: source_collection.to_owned(),
        dest_collection: dest_collection.to_owned(),
        item_key: item_key.to_vec(),
        dest_key: dest_key.to_vec(),
        surrogate,
    }
}

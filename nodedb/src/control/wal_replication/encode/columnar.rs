// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial`
//! sync-engine variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, UpdateValue};
use nodedb_types::Surrogate;

/// Fields the leader knows about a `ColumnarOp::Insert` that must cross the
/// wire so a follower re-derives the SAME executed plan, not a
/// re-hardcoded one. Bundled into a struct — plain positional arguments here
/// exceed clippy's arity lint.
pub(super) struct ColumnarIngestFields<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub surrogates: &'a [Surrogate],
    pub schema_bytes: &'a [u8],
    pub provenance: Option<Vec<u8>>,
    pub returning: Option<Vec<u8>>,
    pub rls_filters: &'a [u8],
}

pub(super) fn columnar_ingest(fields: ColumnarIngestFields<'_>) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarIngest {
        collection: fields.collection.to_owned(),
        payload: fields.payload.to_vec(),
        schema_bytes: fields.schema_bytes.to_vec(),
        surrogates: fields.surrogates.iter().map(|s| s.as_u32()).collect(),
        provenance: fields.provenance,
        format: fields.format.to_owned(),
        intent: fields.intent,
        on_conflict_updates: fields.on_conflict_updates.to_vec(),
        returning: fields.returning,
        rls_filters: fields.rls_filters.to_vec(),
    }
}

pub(super) fn timeseries_ingest(
    collection: &str,
    payload: &[u8],
    format: &str,
    surrogates: &[Surrogate],
    provenance: Option<Vec<u8>>,
    returning: Option<Vec<u8>>,
    rls_filters: &[u8],
) -> ReplicatedWrite {
    ReplicatedWrite::TimeseriesIngest {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        format: format.to_owned(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        provenance,
        returning,
        rls_filters: rls_filters.to_vec(),
    }
}

pub(super) fn fts_index(
    collection: &str,
    surrogate: u32,
    text: &str,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::FtsIndex {
        collection: collection.to_owned(),
        surrogate,
        text: text.to_owned(),
        provenance,
    }
}

pub(super) fn fts_delete(
    collection: &str,
    surrogate: u32,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::FtsDelete {
        collection: collection.to_owned(),
        surrogate,
        provenance,
    }
}

pub(super) fn spatial_insert(
    collection: &str,
    field: &str,
    surrogate: u32,
    geometry: &nodedb_types::geometry::Geometry,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::SpatialInsert {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate,
        // Geometry is plain serializable data — encoding is infallible (same
        // contract as `ReplicatedEntry::to_bytes`). Fail loud rather than
        // replicate empty bytes that would error on follower decode.
        geometry_bytes: zerompk::to_msgpack_vec(geometry)
            .expect("Geometry serialization is infallible"),
        provenance,
    }
}

pub(super) fn spatial_delete(
    collection: &str,
    field: &str,
    surrogate: u32,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::SpatialDelete {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate,
        provenance,
    }
}

/// Columnar predicate DELETE / UPDATE replicates as a `ColumnarBulkDml`
/// entry: each replica re-scans local columnar state at the committed log
/// position and applies the predicate deterministically (Raft log order ⇒
/// identical prior state ⇒ identical matching set), exactly like the
/// Document `BulkDml` sibling.
pub(super) fn bulk_delete(collection: &str, filters: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: false,
        updates: Vec::new(),
    }
}

pub(super) fn bulk_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: true,
        updates: updates.to_vec(),
    }
}

/// Columnar resolved-row-set UPDATE (governed by an RLS write policy): the
/// Control Plane already resolved the predicate to concrete rows and decided
/// the policy against their exact post-images, so every replica applies
/// exactly these rows and evaluates nothing.
pub(super) fn bulk_resolved_update(
    collection: &str,
    rows: &[(nodedb_types::Value, Vec<nodedb_types::Value>)],
) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDmlResolved {
        collection: collection.to_owned(),
        is_update: true,
        rows: rows
            .iter()
            .map(|(pk, new_row)| super::super::types::ColumnarResolvedRow {
                pk_msgpack: encode_resolved_value(pk),
                new_row_msgpack: encode_resolved_value(&nodedb_types::Value::Array(
                    new_row.clone(),
                )),
            })
            .collect(),
    }
}

/// Columnar resolved-row-set DELETE (governed by an RLS write policy): see
/// [`bulk_resolved_update`].
pub(super) fn bulk_resolved_delete(
    collection: &str,
    pks: &[nodedb_types::Value],
) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDmlResolved {
        collection: collection.to_owned(),
        is_update: false,
        rows: pks
            .iter()
            .map(|pk| super::super::types::ColumnarResolvedRow {
                pk_msgpack: encode_resolved_value(pk),
                new_row_msgpack: Vec::new(),
            })
            .collect(),
    }
}

/// Encode a resolved row's `Value` for the wire. Same infallible contract as
/// `geometry_bytes` above: a `Value` these rows carry never fails to encode.
fn encode_resolved_value(value: &nodedb_types::Value) -> Vec<u8> {
    nodedb_types::value_to_msgpack(value).expect("resolved row Value serialization is infallible")
}

/// `ColumnarOp::Truncate` replicates as a plain `ColumnarTruncate` entry:
/// same idempotent-replay contract as `kv::truncate`.
pub(super) fn truncate(collection: &str, restart_identity: bool) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarTruncate {
        collection: collection.to_owned(),
        restart_identity,
    }
}

/// `TimeseriesOp::Truncate` replicates as a plain `TimeseriesTruncate`
/// entry: same contract as [`truncate`].
pub(super) fn timeseries_truncate(collection: &str, restart_identity: bool) -> ReplicatedWrite {
    ReplicatedWrite::TimeseriesTruncate {
        collection: collection.to_owned(),
        restart_identity,
    }
}

// SPDX-License-Identifier: BUSL-1.1

//! Classify the columnar-storage-family engine ops (`ColumnarOp`,
//! `TimeseriesOp`, `TextOp`, `SpatialOp`) into an optional `ReplicatedWrite`.
//!
//! Each function is exhaustive over its op enum: a new variant is a compile error.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::columnar;
use super::columnar::ColumnarIngestFields;
use super::entry::{encode_provenance, encode_returning};
use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TextOp, TimeseriesOp};

/// Encode a `ColumnarOp` write variant, `Ok(None)` for scans, or an error when
/// the op cannot be replicated safely. `Update`/`Delete` refuse when
/// `rls_write_check.has_predicate()` — see the arm below.
pub(super) fn columnar_write(op: &ColumnarOp) -> crate::Result<Option<ReplicatedWrite>> {
    Ok(Some(match op {
        ColumnarOp::Insert {
            collection,
            payload,
            format,
            intent,
            on_conflict_updates,
            surrogates,
            schema_bytes,
            provenance,
            // wal_lsn omitted: a follower allocates its own LSN at apply time.
            wal_lsn: _,
            // Plain insert: policy decided at plan time. ON CONFLICT DO UPDATE defers —
            // its merged row exists only in the handler, so the guard below refuses it.
            rls_write_check,
            returning,
            rls_filters,
        } => {
            refuse_governed_merge(collection.as_str(), on_conflict_updates, rls_write_check)?;
            columnar::columnar_ingest(ColumnarIngestFields {
                collection: collection.as_str(),
                payload,
                format,
                intent: *intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance: encode_provenance(provenance),
                returning: encode_returning(returning),
                rls_filters,
            })
        }
        // A governed predicate needs a writing identity a follower lacks — must
        // resolve to a concrete row set before reaching this encoder.
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check,
        } => {
            refuse_governed_predicate_dml(collection.as_str(), rls_write_check)?;
            columnar::bulk_delete(collection.as_str(), filters)
        }
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check,
        } => {
            refuse_governed_predicate_dml(collection.as_str(), rls_write_check)?;
            columnar::bulk_update(collection.as_str(), filters, updates)
        }

        // Already resolved: the wire shape carries the row set, never a
        // predicate, so no refusal check is needed.
        ColumnarOp::ResolvedUpdate {
            collection,
            rows,
            rls_write_check: _,
        } => columnar::bulk_resolved_update(collection.as_str(), rows),
        ColumnarOp::ResolvedDelete {
            collection,
            pks,
            rls_write_check: _,
        } => columnar::bulk_resolved_delete(collection.as_str(), pks),

        // Refused at injection under a write policy, so no predicate to
        // resolve; replicates as a plain whole-collection clear.
        ColumnarOp::Truncate {
            collection,
            restart_identity,
        } => columnar::truncate(collection.as_str(), *restart_identity),

        // Not a write — reads/scans. `ResolveDml` only reports the row set a DML would touch.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => return Ok(None),
    }))
}

/// Refuse `INSERT ... ON CONFLICT DO UPDATE` on a governed collection: its
/// merged row exists only in the handler, and a follower has no writing
/// identity to evaluate the compiled predicate against.
fn refuse_governed_merge(
    collection: &str,
    on_conflict_updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
    rls_write_check: &nodedb_types::RlsWriteCheck,
) -> crate::Result<()> {
    if on_conflict_updates.is_empty() || !rls_write_check.has_predicate() {
        return Ok(());
    }
    Err(crate::Error::PlanError {
        detail: format!(
            "INSERT ... ON CONFLICT DO UPDATE on '{collection}' cannot be replicated because it \
             carries an RLS write policy: the merged row exists only where it is persisted, and a \
             follower has no writing identity to evaluate the policy against. It must be resolved \
             to a concrete row set before it is proposed."
        ),
    })
}

/// Refuse a predicate `UPDATE` / `DELETE` on a collection that carries an RLS
/// write policy — see [`columnar_write`]'s `Delete` / `Update` arms for the
/// full reasoning.
fn refuse_governed_predicate_dml(
    collection: &str,
    rls_write_check: &nodedb_types::RlsWriteCheck,
) -> crate::Result<()> {
    if rls_write_check.has_predicate() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "columnar predicate UPDATE/DELETE on '{collection}' cannot be replicated as a \
                 predicate because it carries an RLS write policy: a follower has no writing \
                 identity to evaluate the predicate against. It must be resolved to a concrete \
                 row set before it is proposed."
            ),
        });
    }
    Ok(())
}

/// Encode a `TimeseriesOp` write variant into its `ReplicatedWrite` wire
/// shape, or `None` for scans.
pub(super) fn timeseries_write(op: &TimeseriesOp) -> Option<ReplicatedWrite> {
    Some(match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            // wal_lsn omitted: a follower allocates its own LSN at apply time.
            wal_lsn: _,
            surrogates,
            provenance,
            // Not replicated, same reason as `ColumnarOp::Insert` above.
            rls_write_check: _,
            returning,
            rls_filters,
        } => columnar::timeseries_ingest(columnar::TimeseriesIngestFields {
            collection: collection.as_str(),
            payload,
            format,
            surrogates,
            // Read once, here on the proposer: every replica stamps an
            // untimed row with this instant instead of its own clock.
            default_timestamp_ms: crate::engine::kv::current_ms() as i64,
            provenance: encode_provenance(provenance),
            returning: encode_returning(returning),
            rls_filters,
        }),

        TimeseriesOp::Truncate {
            collection,
            restart_identity,
        } => columnar::timeseries_truncate(collection.as_str(), *restart_identity),

        // Not a write — reads / scans, and the read-only resolve pass.
        TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => return None,
    })
}

/// Encode a `TextOp` write variant into its `ReplicatedWrite` wire shape, or
/// `None` for the search / DDL-config variants.
pub(super) fn text_write(op: &TextOp) -> Option<ReplicatedWrite> {
    Some(match op {
        TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance,
        } => columnar::fts_index(
            collection.as_str(),
            surrogate.as_u32(),
            text,
            encode_provenance(provenance),
        ),
        TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance,
        } => columnar::fts_delete(
            collection.as_str(),
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),

        // Not a write — searches and the config-only analyzer binding.
        TextOp::Search { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::SetTextConfig { .. } => return None,
    })
}

/// Encode a `SpatialOp` write variant into its `ReplicatedWrite` wire shape,
/// or `None` for scans.
pub(super) fn spatial_write(op: &SpatialOp) -> Option<ReplicatedWrite> {
    Some(match op {
        SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        } => columnar::spatial_insert(
            collection.as_str(),
            field,
            surrogate.as_u32(),
            geometry,
            encode_provenance(provenance),
        ),
        SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        } => columnar::spatial_delete(
            collection.as_str(),
            field,
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),

        // Not a write — R-tree index scan.
        SpatialOp::Scan { .. } => return None,
    })
}

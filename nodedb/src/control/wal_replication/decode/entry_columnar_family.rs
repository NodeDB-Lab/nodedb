// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial` — the
//! columnar-storage-family engines plus their overlay sync engines (FTS,
//! spatial). The sync-engine ingest/index arms delegate to
//! [`super::super::decode_sync_engines`]; the columnar predicate-DML arm
//! delegates to [`super::columnar`].
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. None of these
//! arms scope by tenancy, so this group takes no [`DecodeCtx`]. `write` is
//! guaranteed by the caller to already be one of these variants — see
//! `entry_document::decode_arm` for the trailing-arm contract.
//!
//! [`DecodeCtx`]: super::ctx::DecodeCtx

use super::super::decode_sync_engines;
use super::super::decode_sync_engines::ColumnarIngestWire;
use super::super::types::ReplicatedWrite;
use super::columnar;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::ColumnarIngest {
            collection,
            payload,
            schema_bytes,
            surrogates,
            provenance,
            format,
            intent,
            on_conflict_updates,
            returning,
            rls_filters,
        } => decode_sync_engines::columnar_ingest(ColumnarIngestWire {
            collection,
            payload,
            format,
            intent: *intent,
            on_conflict_updates,
            schema_bytes,
            surrogates,
            prov_bytes: provenance,
            returning_bytes: returning,
            rls_filters,
        }),
        ReplicatedWrite::TimeseriesIngest {
            collection,
            payload,
            format,
            surrogates,
            // Carried as the entry's `resolved_now_ms` (see `decode::entry`).
            default_timestamp_ms: _,
            provenance,
            returning,
            rls_filters,
        } => decode_sync_engines::timeseries_ingest(
            collection,
            payload,
            format,
            surrogates,
            provenance,
            returning,
            rls_filters,
        ),
        ReplicatedWrite::FtsIndex {
            collection,
            surrogate,
            text,
            provenance,
        } => decode_sync_engines::fts_index(collection, *surrogate, text, provenance),
        ReplicatedWrite::FtsDelete {
            collection,
            surrogate,
            provenance,
        } => decode_sync_engines::fts_delete(collection, *surrogate, provenance),
        ReplicatedWrite::SpatialInsert {
            collection,
            field,
            surrogate,
            geometry_bytes,
            provenance,
        } => decode_sync_engines::spatial_insert(
            collection,
            field,
            *surrogate,
            geometry_bytes,
            provenance,
        ),
        ReplicatedWrite::SpatialDelete {
            collection,
            field,
            surrogate,
            provenance,
        } => decode_sync_engines::spatial_delete(collection, field, *surrogate, provenance),
        ReplicatedWrite::ColumnarBulkDml {
            collection,
            filters,
            is_update,
            updates,
        } => Ok(columnar::bulk_dml(collection, filters, *is_update, updates)),
        ReplicatedWrite::ColumnarBulkDmlResolved {
            collection,
            is_update,
            rows,
        } => columnar::bulk_dml_resolved(collection, *is_update, rows),
        ReplicatedWrite::ColumnarTruncate {
            collection,
            restart_identity,
        } => Ok(columnar::truncate(collection, *restart_identity)),
        ReplicatedWrite::TimeseriesTruncate {
            collection,
            restart_identity,
        } => Ok(columnar::timeseries_truncate(collection, *restart_identity)),
        _ => Err(crate::Error::Internal {
            detail: "entry_columnar_family::decode_arm called with a non-columnar-family \
                ReplicatedWrite variant (dispatch bug in decode/entry.rs's grouped \
                columnar-family match arm)"
                .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::control::wal_replication::decode;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_types::QualifiedCollection;

    #[test]
    fn a_replicated_timeseries_ingest_carries_the_proposers_instant() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=1".to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let before = crate::engine::kv::current_ms();
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)
            .expect("decide");
        let entry = crate::control::wal_replication::encode::to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &write,
        )
        .expect("encode")
        .expect("a timeseries ingest replicates");
        let after = crate::engine::kv::current_ms();

        let bytes = entry.to_bytes();
        let (_, _, _, resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
            .expect("decode")
            .expect("an entry");
        let instant = resolved_now_ms.expect("the ingest carries an instant");
        assert!((before..=after).contains(&instant));
        // Every replica decodes the same bytes, so every replica stamps alike.
        let (_, _, _, again) = decode::from_replicated_entry(&bytes, None)
            .expect("decode")
            .expect("an entry");
        assert_eq!(again, Some(instant));
    }
}
